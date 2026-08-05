//! Reconstruction runtime: drives per-`(venue, symbol)` `SourcedOrderbook` +
//! `SourcedTradebook` from an adapter's multiplexed `DomainEvent` stream and
//! emits synchronizer-ready events.
//!
//! One adapter socket can carry many symbols; each book seeds, gaps, and
//! re-seeds independently. For REST-seeded venues a book buffers its deltas
//! until its snapshot arrives, discards deltas at or below the snapshot id,
//! applies the rest in order, then streams; trades pass straight through. A
//! continuity gap re-fetches that book's seed and reconciles.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use tokio::sync::{mpsc, watch};

use crate::errors::ExchangeError;
use aetelier_types::orderbooks::{NormalizedDelta, Orderbook};
use aetelier_types::trades::Trade;
use aetelier_types::trading_pair::TradingPair;

use super::budget::SourceMetrics;
use super::feed::{FeedDatatype, SharedFeedSet};
use super::model::{
    DomainEvent, ReconstructionModel, RecoveryAction, SourcedOrderbook, SourcedTradebook,
    TradeApply,
};
use super::rest::RestSnapshot;
use super::symbol::SymbolCodec;
use crate::synchronizers::capture_levels;
use aetelier_types::config::markets::market_config::{DeclaredDatatype, DeclaredSet};

/// A reconstructed event ready to feed a `MarketSynchronizer`.
pub enum ReconstructedEvent {
    /// Full reconstructed book after applying an update.
    Book {
        pair: TradingPair,
        ts_us: u64,
        book: Orderbook,
    },
    /// A public trade print.
    Trade(Trade),
}

/// Why the runtime loop ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeOutcome {
    /// The event stream closed or shutdown fired — normal teardown.
    Finished,
    /// A self-seeded book lost continuity and can only recover by resubscribing.
    /// The caller should reconnect the socket (a fresh subscribe re-seeds).
    ResyncRequired,
}

/// Result of a per-pair seed fetch, tagged by the pair it seeds.
type SeedResult = (TradingPair, Result<NormalizedDelta, ExchangeError>);

/// Cap on a pair's buffered deltas while awaiting a seed. Beyond this the
/// oldest are dropped (they would be discarded at the next seed anyway), so a
/// slow/failing seed cannot grow memory without bound.
const MAX_BUFFERED_DELTAS: usize = 100_000;

/// Cap on consecutive re-seed attempts for one pair before giving up (prevents
/// a re-seed storm against a persistently lagging snapshot).
const MAX_RESEEDS: u32 = 8;

/// Linear backoff base for retrying a FAILED seed fetch (attempt n waits
/// n * base, capped) — venue-friendly against transient REST 429s/timeouts
/// while the reseed budget still bounds the total attempts.
const SEED_RETRY_BASE: Duration = Duration::from_millis(250);
/// Cap on the seed-retry backoff.
const SEED_RETRY_CAP: Duration = Duration::from_secs(2);

const IN_BAND_SEED_DEADLINE: Duration = Duration::from_secs(10);
const SEED_DEADLINE_TICK: Duration = Duration::from_secs(1);

/// Routing mode for a pair's incoming book deltas. This is NOT the book's
/// truth — sync state lives in `OrderBookState` on the book itself.
enum Phase {
    /// Awaiting the seed snapshot; book deltas accumulate here.
    Buffering(VecDeque<NormalizedDelta>),
    /// Deltas route directly to `book.apply` (seeded, or deliberately
    /// best-effort when no seeder exists).
    Passthrough,
}

/// Per-pair reconstruction state.
struct PairBook {
    /// Venue wire symbol passed to the REST seeder (e.g. `"BTCUSDT"`).
    wire: String,
    book: SourcedOrderbook,
    trades: SourcedTradebook,
    phase: Phase,
    buffering_since: Option<Instant>,
    buffer_overflowed: bool,
    /// A seed fetch is in flight for this pair.
    seeding: bool,
    /// Consecutive re-seed attempts since the last clean reconcile.
    reseeds: u32,
    /// Exchange event time (UTC epoch µs) of the last applied update; 0 if unknown.
    last_ts: u64,
    /// Local receipt time (UTC epoch µs) of the last applied update.
    last_local_ts: u64,
    /// Connection ping/pong round-trip (µs) at the last applied update.
    last_rtt: u64,
}

/// Drives reconstruction for one adapter socket across one or more symbols.
pub struct SourceRuntime {
    exchange: String,
    codec: SymbolCodec,
    needs_rest: bool,
    seeds_out_of_band: bool,
    declared: DeclaredSet,
    undeclared_warned: std::collections::BTreeSet<DeclaredDatatype>,
    books: HashMap<TradingPair, PairBook>,
    /// Shared worker counter handle: gaps/resyncs/checksum failures bump here.
    metrics: SourceMetrics,
    /// Worker's shared feed set; when attached, the runtime drives the
    /// per-(instrument, datatype) Live/Resubscribing transitions.
    feeds: Option<SharedFeedSet>,
    /// Pairs already marked Live this runtime, so the hot path takes the
    /// feeds lock only on an actual transition.
    live_orders: HashSet<TradingPair>,
    live_trades: HashSet<TradingPair>,
    /// Worker-owned cross-reconnect trade-sequence carry (see
    /// [`Self::with_trade_seq_carry`]).
    trade_seq_carry: Option<Arc<std::sync::Mutex<HashMap<TradingPair, u64>>>>,
}

impl SourceRuntime {
    /// Build a runtime for `wire_symbols` (decoded to canonical pairs via
    /// `codec`). Symbols that fail to decode are skipped. `metrics` is the
    /// worker's shared counter handle.
    pub fn new(
        exchange: impl Into<String>,
        codec: SymbolCodec,
        wire_symbols: Vec<String>,
        model: ReconstructionModel,
        recovery: RecoveryAction,
        metrics: SourceMetrics,
        declared: DeclaredSet,
    ) -> Self {
        let needs_rest = model.needs_rest();
        let seeds_out_of_band = model.seeds_out_of_band();
        let requested = wire_symbols.len();
        let mut books = HashMap::new();
        for wire in wire_symbols {
            let Some(pair) = codec.decode(&wire) else {
                tracing::warn!(symbol = %wire, "runtime.symbol_undecodable_skipped");
                continue;
            };
            let phase = if seeds_out_of_band {
                Phase::Buffering(VecDeque::new())
            } else {
                Phase::Passthrough
            };
            books.insert(
                pair.clone(),
                PairBook {
                    wire,
                    book: SourcedOrderbook::new(pair.clone(), model.clone(), recovery),
                    trades: SourcedTradebook::new(pair),
                    phase,
                    buffering_since: seeds_out_of_band.then(Instant::now),
                    buffer_overflowed: false,
                    seeding: false,
                    reseeds: 0,
                    last_ts: 0,
                    last_local_ts: 0,
                    last_rtt: 0,
                },
            );
        }
        if books.is_empty() && requested > 0 {
            tracing::error!(requested, "runtime.no_decodable_symbols");
        }
        Self {
            exchange: exchange.into(),
            codec,
            needs_rest,
            seeds_out_of_band,
            declared,
            undeclared_warned: std::collections::BTreeSet::new(),
            books,
            metrics,
            feeds: None,
            live_orders: HashSet::new(),
            live_trades: HashSet::new(),
            trade_seq_carry: None,
        }
    }

    /// Attach the worker's shared `FeedSet`; the runtime then records
    /// per-feed Live/Resubscribing as books seed, gap, and reconcile.
    pub fn with_feeds(mut self, feeds: SharedFeedSet) -> Self {
        self.feeds = Some(feeds);
        self
    }

    /// Attach the worker's cross-reconnect trade-sequence carry. Each pair's
    /// tradebook seeds its continuity pointer from the map (so the first
    /// trade after a reconnect is checked against the pre-disconnect
    /// position — an outage's lost prints get counted by the same exact
    /// arithmetic), and writes its position back as trades apply.
    pub fn with_trade_seq_carry(
        mut self,
        carry: Arc<std::sync::Mutex<HashMap<TradingPair, u64>>>,
    ) -> Self {
        if let Ok(map) = carry.lock() {
            for (pair, b) in self.books.iter_mut() {
                let seed = map.get(pair).copied();
                let fresh = SourcedTradebook::new(pair.clone()).with_last_seq(seed);
                b.trades = fresh;
            }
        }
        self.trade_seq_carry = Some(carry);
        self
    }

    /// Arm the configured order-book depth (TOML `datatypes.orderbook.depth`)
    /// on every reconstructed book. This auto-wires the framework / persistence
    /// path so a standalone `OrderbookDelta` prunes to the subscribed depth
    /// without a manual `with_max_depth`. Checksum venues keep their recipe
    /// depth (see [`SourcedOrderbook::arm_config_depth`]); `None` keeps the
    /// full book.
    pub fn with_max_depth(mut self, max_depth: Option<usize>) -> Self {
        for pair_book in self.books.values_mut() {
            pair_book.book.arm_config_depth(max_depth);
        }
        self
    }

    /// Number of tracked pairs.
    pub fn len(&self) -> usize {
        self.books.len()
    }

    pub fn is_empty(&self) -> bool {
        self.books.is_empty()
    }

    /// Consume `events` (an adapter's `DomainEvent` output), reconstruct each
    /// pair's book, and emit `ReconstructedEvent`s on `out` until the stream
    /// ends or `shutdown` fires. `seeder` is required when the model needs a
    /// REST seed; a continuity gap re-fetches that pair's seed and reconciles.
    pub async fn run(
        mut self,
        mut events: mpsc::Receiver<DomainEvent>,
        seeder: Option<Arc<dyn RestSnapshot>>,
        out: mpsc::Sender<ReconstructedEvent>,
        mut shutdown: watch::Receiver<bool>,
    ) -> RuntimeOutcome {
        let (seed_tx, mut seed_rx) =
            mpsc::channel::<SeedResult>(self.books.len() * 2 + 1);
        let mut in_flight = 0usize;

        // Kick off the initial seed for every pair.
        if self.needs_rest {
            match seeder.as_ref() {
                Some(s) => {
                    let to_seed: Vec<(TradingPair, String)> = self
                        .books
                        .iter()
                        .map(|(p, b)| (p.clone(), b.wire.clone()))
                        .collect();
                    for (pair, wire) in to_seed {
                        spawn_seed(pair.clone(), wire, s, &seed_tx, Duration::ZERO);
                        if let Some(b) = self.books.get_mut(&pair) {
                            b.seeding = true;
                        }
                        in_flight += 1;
                    }
                }
                None => {
                    // Cannot reconstruct without a seed; don't buffer forever.
                    tracing::error!(
                        exchange = %self.exchange,
                        "runtime.needs_rest_without_seeder"
                    );
                    for b in self.books.values_mut() {
                        b.phase = Phase::Passthrough;
                    }
                }
            }
        }

        let mut events_done = false;
        let mut resync = false;
        let awaits_in_band_seed = self.seeds_out_of_band && !self.needs_rest;
        let mut seed_deadline = tokio::time::interval(SEED_DEADLINE_TICK);
        seed_deadline.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if events_done && in_flight == 0 {
                break;
            }
            tokio::select! {
                biased;

                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                }

                maybe = events.recv(), if !events_done => match maybe {
                    Some(ev) => match self
                        .handle_event(ev, &out, &seeder, &seed_tx, &mut in_flight)
                        .await
                    {
                        Ok(true) => {
                            resync = true; // self-seed gap → caller reconnects
                            break;
                        }
                        Ok(false) => {}
                        Err(()) => return RuntimeOutcome::Finished,
                    },
                    None => events_done = true,
                },

                Some((pair, result)) = seed_rx.recv() => {
                    in_flight -= 1;
                    if let Some(b) = self.books.get_mut(&pair) {
                        b.seeding = false;
                    }
                    match result {
                        Ok(snapshot) => {
                            match self
                                .apply_seed(&pair, snapshot, &out, &seeder, &seed_tx, &mut in_flight)
                                .await
                            {
                                Ok(true) => {
                                    resync = true; // reseed budget exhausted → reconnect
                                    break;
                                }
                                Ok(false) => {}
                                Err(()) => return RuntimeOutcome::Finished,
                            }
                        }
                        Err(e) => {
                            // The buffer is RETAINED: recovery is policy-driven
                            // (retry ladder below), never delta-driven — a quiet
                            // stream must not strand an unseeded book.
                            self.metrics.bump_resyncs();
                            let attempts = self
                                .books
                                .get(&pair)
                                .map(|b| b.reseeds)
                                .unwrap_or(MAX_RESEEDS);
                            if attempts < MAX_RESEEDS {
                                let delay =
                                    SEED_RETRY_BASE.saturating_mul(attempts + 1).min(SEED_RETRY_CAP);
                                tracing::warn!(
                                    error = %e,
                                    pair = %pair,
                                    attempt = attempts + 1,
                                    delay_ms = delay.as_millis() as u64,
                                    "runtime.seed_fetch_failed_retrying"
                                );
                                self.request_reseed(&pair, &seeder, &seed_tx, &mut in_flight, delay);
                            } else {
                                // Retry budget exhausted against a failing
                                // seeder: reconnect for a fresh runtime (the
                                // cap resets) instead of stranding the book.
                                tracing::error!(
                                    error = %e,
                                    pair = %pair,
                                    "runtime.seed_fetch_exhausted_reconnecting"
                                );
                                self.metrics.bump_reseed_exhausted();
                                resync = true;
                                break;
                            }
                        }
                    }
                }

                _ = seed_deadline.tick(), if awaits_in_band_seed => {
                    if self.in_band_seed_expired() {
                        resync = true;
                        break;
                    }
                }
            }
        }
        if in_flight > 0 {
            // Shutdown/resync exit with seed fetches still in flight: they
            // are abandoned, never applied. Counted so a "seed requested but
            // no book appeared" window is attributable.
            self.metrics.add_seeds_abandoned(in_flight as u64);
            tracing::warn!(
                exchange = %self.exchange,
                abandoned = in_flight,
                "runtime.seeds_abandoned_on_exit"
            );
        }
        if resync {
            RuntimeOutcome::ResyncRequired
        } else {
            RuntimeOutcome::Finished
        }
    }

    /// Route one decoded event to its pair's book. `Ok(true)` = a self-seeded
    /// book gapped and the socket must be reconnected; `Err(())` = output closed.
    async fn handle_event(
        &mut self,
        ev: DomainEvent,
        out: &mpsc::Sender<ReconstructedEvent>,
        seeder: &Option<Arc<dyn RestSnapshot>>,
        seed_tx: &mpsc::Sender<SeedResult>,
        in_flight: &mut usize,
    ) -> Result<bool, ()> {
        let needs_rest = self.needs_rest;
        match ev {
            DomainEvent::Book(mut delta) => {
                if !self.declared.contains(DeclaredDatatype::Orderbook) {
                    self.metrics.bump_undeclared_dropped();
                    if self.undeclared_warned.insert(DeclaredDatatype::Orderbook) {
                        tracing::warn!(
                            exchange = %self.exchange,
                            datatype = "orderbook",
                            "runtime.undeclared_event_dropped"
                        );
                    }
                    return Ok(false);
                }
                let Some(pair) = self.codec.decode(&delta.symbol) else {
                    return Ok(false);
                };
                // Stamp the canonical pair symbol so the book's symbol check
                // (`OrderbookDelta::process`) matches regardless of the venue
                // wire form — e.g. Gate.io's `BTC_USDT` parses to a non-canonical
                // pair, while `BTCUSDT` (Bitget) doesn't parse at all.
                delta.symbol = pair.to_canonical();
                if delta.is_snapshot
                    && self
                        .books
                        .get(&pair)
                        .is_some_and(|b| matches!(b.phase, Phase::Buffering(_)))
                {
                    return self
                        .apply_seed(&pair, delta, out, seeder, seed_tx, in_flight)
                        .await;
                }
                let can_reseed = seeder.is_some();
                let mut reseed = false;
                let mut resync = false;
                let mut applied = false;
                {
                    let Some(b) = self.books.get_mut(&pair) else {
                        return Ok(false); // untracked symbol on a shared socket
                    };
                    match &mut b.phase {
                        Phase::Buffering(buf) => {
                            if buf.len() >= MAX_BUFFERED_DELTAS {
                                if !b.buffer_overflowed {
                                    b.buffer_overflowed = true;
                                    tracing::warn!(
                                        pair = %pair,
                                        cap = MAX_BUFFERED_DELTAS,
                                        "runtime.seed_buffer_overflow_dropping_oldest"
                                    );
                                }
                                buf.pop_front(); // drop oldest; discarded at seed anyway
                            }
                            buf.push_back(delta);
                        }
                        Phase::Passthrough => match b.book.apply(delta.clone()) {
                            Ok(_) => {
                                if delta.source_orderbook_ts_us > 0 {
                                    b.last_ts = delta.source_orderbook_ts_us;
                                }
                                b.last_local_ts = delta.local_orderbook_ts_us;
                                b.last_rtt = delta.source_orderbook_rtt_us;
                                emit_book(
                                    &self.exchange,
                                    &pair,
                                    &b.book,
                                    book_ts(b),
                                    b.last_ts,
                                    b.last_local_ts,
                                    b.last_rtt,
                                    out,
                                )
                                .await
                                .map_err(|_| ())?;
                                applied = true;
                            }
                            Err(resync_e) if can_reseed && b.reseeds < MAX_RESEEDS => {
                                tracing::warn!(reason = %resync_e.reason, pair = %pair, "runtime.book_gapped_reseeding");
                                self.metrics.bump_gaps();
                                if resync_e.reason.is_checksum() {
                                    self.metrics.bump_checksum_fail();
                                }
                                self.metrics.bump_resyncs();
                                let mut q = VecDeque::new();
                                q.push_back(delta);
                                b.phase = Phase::Buffering(q);
                                b.buffering_since = Some(Instant::now());
                                reseed = true;
                            }
                            Err(resync_e) if !needs_rest => {
                                // Self-seeded book: the only re-seed is a fresh
                                // subscribe, so ask the caller to reconnect.
                                tracing::warn!(reason = %resync_e.reason, pair = %pair, "runtime.book_gapped_resync");
                                self.metrics.bump_gaps();
                                if resync_e.reason.is_checksum() {
                                    self.metrics.bump_checksum_fail();
                                }
                                self.metrics.bump_resyncs();
                                resync = true;
                            }
                            Err(resync_e) => {
                                // REST venue past the reseed cap (or with no
                                // seeder): a known-bad book never keeps
                                // applying deltas — reconnect. The fresh
                                // runtime re-seeds and resets the cap; the
                                // worker's jittered backoff paces retries.
                                tracing::warn!(reason = %resync_e.reason, pair = %pair, "runtime.book_gapped_reseed_exhausted_reconnecting");
                                self.metrics.bump_gaps();
                                if resync_e.reason.is_checksum() {
                                    self.metrics.bump_checksum_fail();
                                }
                                self.metrics.bump_resyncs();
                                resync = true;
                            }
                        },
                    }
                }
                if applied {
                    self.note_live(&pair, FeedDatatype::Orders);
                }
                if reseed {
                    self.note_resubscribing(&pair);
                    self.request_reseed(
                        &pair,
                        seeder,
                        seed_tx,
                        in_flight,
                        Duration::ZERO,
                    );
                }
                Ok(resync)
            }
            DomainEvent::ConnectionGap { dropped } => {
                // The adapter proved messages left this socket undelivered
                // (Coinbase's connection-wide counter — no per-book predicate
                // can see it). Every book on the source is suspect: gap them
                // and clear eagerly (nothing large is held across the
                // reconnect), drop any seed buffers, and hand the caller the
                // resync. Once per connection by construction — the resync
                // tears the socket down — so the worker's jittered reconnect
                // backoff is what paces retries under a chronically lossy
                // link (no storm).
                tracing::warn!(
                    exchange = %self.exchange,
                    dropped,
                    "runtime.connection_gap_resync"
                );
                self.metrics.bump_gaps();
                self.metrics.bump_resyncs();
                // The tracker's count is exact (dense per-book or reconciled
                // connection counter) — feed the coverage ledger.
                self.metrics.add_deltas_missed(dropped);
                for b in self.books.values_mut() {
                    b.book.force_gap();
                    b.phase = Phase::Passthrough;
                    b.buffering_since = None;
                }
                Ok(true)
            }
            DomainEvent::Trade { trade, sequence } => {
                if !self.declared.contains(DeclaredDatatype::Trades) {
                    self.metrics.bump_undeclared_dropped();
                    if self.undeclared_warned.insert(DeclaredDatatype::Trades) {
                        tracing::warn!(
                            exchange = %self.exchange,
                            datatype = "trades",
                            "runtime.undeclared_event_dropped"
                        );
                    }
                    return Ok(false);
                }
                // The runtime's tracked set is authoritative for this socket.
                let Some(b) = self.books.get_mut(&trade.pair) else {
                    return Ok(false); // untracked pair — drop
                };
                let pair = trade.pair.clone();
                // A sequenced trade proves this source's loss accounting is
                // EXACT (dense id armed at the adapter); unarmed venues stay
                // at the Estimated default set at spawn.
                if sequence.is_some() {
                    self.metrics.set_trade_loss_confidence(
                        crate::framework::budget::TradeLossConfidence::Exact,
                    );
                }
                match b.trades.apply(trade.clone(), sequence) {
                    TradeApply::GappedApplied { missed } => {
                        // A trade gap is a permanent, countable loss — no
                        // re-seed path exists for historical prints, so it
                        // is accounted, never escalated to a reconnect.
                        self.metrics.bump_trade_gaps();
                        self.metrics.add_trades_lost(missed);
                        tracing::warn!(
                            pair = %pair,
                            missed,
                            "runtime.trade_gap_prints_lost"
                        );
                    }
                    TradeApply::Applied | TradeApply::Duplicate => {}
                }
                // Persist the continuity position so the NEXT connection's
                // tradebook seeds from it (cross-reconnect exact accounting).
                if let Some(carry) = &self.trade_seq_carry
                    && let Some(last) = b.trades.last_seq()
                    && let Ok(mut map) = carry.lock()
                {
                    map.insert(pair.clone(), last);
                }
                out.send(ReconstructedEvent::Trade(trade))
                    .await
                    .map_err(|_| ())?;
                self.note_live(&pair, FeedDatatype::Trades);
                Ok(false)
            }
        }
    }

    /// Seed one pair's book, then reconcile + replay its buffer. Re-seeds again
    /// if a replayed delta still gaps.
    async fn apply_seed(
        &mut self,
        pair: &TradingPair,
        mut snapshot: NormalizedDelta,
        out: &mpsc::Sender<ReconstructedEvent>,
        seeder: &Option<Arc<dyn RestSnapshot>>,
        seed_tx: &mpsc::Sender<SeedResult>,
        in_flight: &mut usize,
    ) -> Result<bool, ()> {
        // Canonicalize the seed symbol so the book's symbol check passes for
        // venues whose REST/wire form is non-canonical (e.g. Bitso `btc_usdt`,
        // Gate.io `BTC_USDT`). Mirrors the live-diff path (`delta.symbol =
        // pair.to_canonical()`). Without this the seed is rejected with a symbol
        // mismatch and the book never initializes — every diff gap-dropped.
        snapshot.symbol = pair.to_canonical();
        let snap_id = snapshot.update_id;
        let snap_ts = snapshot.source_orderbook_ts_us;
        let snap_local = snapshot.local_orderbook_ts_us;
        let snap_rtt = snapshot.source_orderbook_rtt_us;
        let can_reseed = seeder.is_some();
        let mut reseed = false;
        let mut resync = false;
        let mut applied = false;
        {
            let Some(b) = self.books.get_mut(pair) else {
                return Ok(false);
            };
            if let Err(e) = b.book.apply(snapshot) {
                tracing::warn!(reason = %e.reason, pair = %pair, "runtime.seed_apply_failed");
                if can_reseed && b.reseeds < MAX_RESEEDS {
                    self.metrics.bump_resyncs();
                    reseed = true; // keep the buffer; try a fresh seed
                } else {
                    // Reseed budget exhausted: reconnect for a fresh runtime
                    // (cap resets) instead of running an unseeded book.
                    self.metrics.bump_resyncs();
                    self.metrics.bump_reseed_exhausted();
                    resync = true;
                }
            } else {
                if snap_ts > 0 {
                    b.last_ts = snap_ts;
                }
                b.last_local_ts = snap_local;
                b.last_rtt = snap_rtt;
                emit_book(
                    &self.exchange,
                    pair,
                    &b.book,
                    book_ts(b),
                    b.last_ts,
                    b.last_local_ts,
                    b.last_rtt,
                    out,
                )
                .await?;
                applied = true;

                let buffered = match std::mem::replace(&mut b.phase, Phase::Passthrough) {
                    Phase::Buffering(buf) => buf,
                    Phase::Passthrough => VecDeque::new(),
                };
                b.buffering_since = None;
                b.buffer_overflowed = false;
                let mut it = buffered.into_iter();
                let mut gapped = false;
                while let Some(delta) = it.next() {
                    if delta.update_id <= snap_id {
                        continue; // covered by the snapshot
                    }
                    match b.book.apply(delta.clone()) {
                        Ok(_) => {
                            if delta.source_orderbook_ts_us > 0 {
                                b.last_ts = delta.source_orderbook_ts_us;
                            }
                            b.last_local_ts = delta.local_orderbook_ts_us;
                            b.last_rtt = delta.source_orderbook_rtt_us;
                            emit_book(
                                &self.exchange,
                                pair,
                                &b.book,
                                book_ts(b),
                                b.last_ts,
                                b.last_local_ts,
                                b.last_rtt,
                                out,
                            )
                            .await?;
                        }
                        Err(resync_e) => {
                            gapped = true;
                            self.metrics.bump_gaps();
                            if resync_e.reason.is_checksum() {
                                self.metrics.bump_checksum_fail();
                            }
                            if can_reseed && b.reseeds < MAX_RESEEDS {
                                tracing::warn!(reason = %resync_e.reason, pair = %pair, "runtime.book_gapped_reseeding");
                                self.metrics.bump_resyncs();
                                let mut q: VecDeque<NormalizedDelta> = VecDeque::new();
                                q.push_back(delta);
                                q.extend(it); // keep the un-replayed tail
                                while q.len() > MAX_BUFFERED_DELTAS {
                                    q.pop_front();
                                }
                                b.phase = Phase::Buffering(q);
                                b.buffering_since = Some(Instant::now());
                                reseed = true;
                            } else {
                                // Replay gapped past the reseed budget:
                                // reconnect rather than keep a known-bad book.
                                tracing::warn!(reason = %resync_e.reason, pair = %pair, "runtime.book_gapped_reseed_exhausted_reconnecting");
                                self.metrics.bump_resyncs();
                                self.metrics.bump_reseed_exhausted();
                                resync = true;
                            }
                            break;
                        }
                    }
                }
                if !gapped {
                    b.reseeds = 0; // clean reconcile — recovered
                }
            }
        }
        if applied {
            self.note_live(pair, FeedDatatype::Orders);
        }
        if reseed {
            self.note_resubscribing(pair);
            self.request_reseed(pair, seeder, seed_tx, in_flight, Duration::ZERO);
        }
        Ok(resync)
    }

    /// Record the first data for `(pair, datatype)` on the shared feed set.
    fn note_live(&mut self, pair: &TradingPair, datatype: FeedDatatype) {
        if self.feeds.is_none() {
            return;
        }
        let set = match datatype {
            FeedDatatype::Orders => &mut self.live_orders,
            FeedDatatype::Trades => &mut self.live_trades,
        };
        if set.contains(pair) {
            return;
        }
        set.insert(pair.clone());
        if let Some(feeds) = &self.feeds
            && let Ok(mut fs) = feeds.lock()
        {
            fs.mark_live(pair, datatype);
        }
    }

    /// Record a book gap entering recovery on the shared feed set.
    fn note_resubscribing(&mut self, pair: &TradingPair) {
        if self.feeds.is_none() {
            return;
        }
        self.live_orders.remove(pair);
        if let Some(feeds) = &self.feeds
            && let Ok(mut fs) = feeds.lock()
        {
            fs.mark_resubscribing(pair, FeedDatatype::Orders);
        }
    }

    fn in_band_seed_expired(&mut self) -> bool {
        let mut expired = false;
        for (pair, b) in self.books.iter() {
            if let Some(since) = b.buffering_since
                && matches!(b.phase, Phase::Buffering(_))
                && since.elapsed() > IN_BAND_SEED_DEADLINE
            {
                tracing::warn!(
                    pair = %pair,
                    waited_ms = since.elapsed().as_millis() as u64,
                    "runtime.in_band_seed_deadline_resync"
                );
                expired = true;
            }
        }
        if expired {
            self.metrics.bump_resyncs();
        }
        expired
    }

    /// Start a fresh seed fetch for `pair` (unless one is already in flight).
    /// `delay` paces the fetch (used by the failed-fetch retry ladder).
    fn request_reseed(
        &mut self,
        pair: &TradingPair,
        seeder: &Option<Arc<dyn RestSnapshot>>,
        seed_tx: &mpsc::Sender<SeedResult>,
        in_flight: &mut usize,
        delay: Duration,
    ) {
        let Some(s) = seeder.as_ref() else {
            return;
        };
        if let Some(b) = self.books.get_mut(pair)
            && !b.seeding
            && b.reseeds < MAX_RESEEDS
        {
            b.seeding = true;
            b.reseeds += 1;
            spawn_seed(pair.clone(), b.wire.clone(), s, seed_tx, delay);
            *in_flight += 1;
        }
    }
}

/// Spawn a detached REST seed fetch that reports its result tagged by `pair`.
/// `delay` paces retries after a failed fetch (zero for first attempts).
fn spawn_seed(
    pair: TradingPair,
    wire: String,
    seeder: &Arc<dyn RestSnapshot>,
    seed_tx: &mpsc::Sender<SeedResult>,
    delay: Duration,
) {
    let s = seeder.clone();
    let tx = seed_tx.clone();
    tokio::spawn(async move {
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        // The REST snapshot-seed frame carries no exchange event time, so its
        // local_orderbook_ts_us is defined (per the timestamp model) as the local
        // instant just BEFORE the GET — captured here, right before fetch.
        let pre_get_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        let mut result = s.fetch_snapshot(&wire).await;
        if let Ok(nd) = result.as_mut() {
            nd.local_orderbook_ts_us = pre_get_us;
        }
        let _ = tx.send((pair, result)).await;
    });
}

/// Capture the current book and emit it as a `ReconstructedEvent::Book`.
/// `ts_us` is the exchange event time of the last applied update (the caller
/// passes the pair's `last_ts`, falling back to wall-clock when unknown).
async fn emit_book(
    exchange: &str,
    pair: &TradingPair,
    book: &SourcedOrderbook,
    ts_us: u64,
    source_ts: u64,
    local_us: u64,
    rtt_us: u64,
    out: &mpsc::Sender<ReconstructedEvent>,
) -> Result<(), ()> {
    let (bids, asks) = capture_levels(book.book());
    let mut ob =
        Orderbook::from_levels(0, ts_us, pair.clone(), exchange.to_string(), bids, asks);
    ob.source_orderbook_ts_us = source_ts;
    ob.local_orderbook_ts_us = local_us;
    ob.source_orderbook_rtt_us = rtt_us;
    out.send(ReconstructedEvent::Book {
        pair: pair.clone(),
        ts_us,
        book: ob,
    })
    .await
    .map_err(|_| ())
}

fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// The book timestamp to stamp on emission: the pair's last exchange event
/// time, falling back to wall-clock when the venue gives no event time.
fn book_ts(b: &PairBook) -> u64 {
    if b.last_ts > 0 { b.last_ts } else { now_us() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::wss::WssDecoder;
    use crate::framework::feed::{Feed, FeedDatatype, FeedSet, FeedState};
    use crate::framework::model::{SeqPredicate, SnapshotSource};
    use crate::sources::binance::decoder::BinanceDecoder;
    use crate::sources::binance::events::BinanceWssEvent;
    use crate::sources::binance::responses::orderbooks::BinanceDepthSnapshot;
    use aetelier_types::exchanges::Exchange;
    use aetelier_types::orderbooks::{OrderbookDelta, f64_to_decimal};
    use aetelier_types::trades::TradeSide;

    const WS_FRAMES: &str =
        include_str!("../../datasets/binance/btcusdt_depth_trade.jsonl");
    const REST_SNAPSHOT: &str =
        include_str!("../../datasets/binance/btcusdt_rest_snapshot.json");

    fn binance_model() -> ReconstructionModel {
        ReconstructionModel::SeqDelta {
            predicate: SeqPredicate::RangeInclusive,
            source: SnapshotSource::RestSnapshot,
        }
    }

    /// A `RestSnapshot` failing its first `fails` fetches, then serving
    /// `snapshot` — exercises the failed-fetch retry ladder.
    struct FlakySeeder {
        fails: std::sync::Mutex<u32>,
        snapshot: NormalizedDelta,
    }

    #[async_trait::async_trait]
    impl RestSnapshot for FlakySeeder {
        async fn fetch_snapshot(
            &self,
            _symbol: &str,
        ) -> Result<NormalizedDelta, ExchangeError> {
            let mut left = self.fails.lock().unwrap();
            if *left > 0 {
                *left -= 1;
                return Err(ExchangeError::IoError(std::io::Error::other(
                    "seed endpoint down",
                )));
            }
            Ok(self.snapshot.clone())
        }
    }

    /// A `RestSnapshot` serving each queued snapshot once, then erroring —
    /// lets a test pin a feed in `Resubscribing` by exhausting the seeder.
    struct OnceSeeder(std::sync::Mutex<VecDeque<NormalizedDelta>>);

    #[async_trait::async_trait]
    impl RestSnapshot for OnceSeeder {
        async fn fetch_snapshot(
            &self,
            _symbol: &str,
        ) -> Result<NormalizedDelta, ExchangeError> {
            self.0.lock().unwrap().pop_front().ok_or_else(|| {
                ExchangeError::IoError(std::io::Error::other("seeder exhausted"))
            })
        }
    }

    /// A `RestSnapshot` returning a per-symbol snapshot from a map.
    struct MapSeeder(HashMap<String, NormalizedDelta>);

    #[async_trait::async_trait]
    impl RestSnapshot for MapSeeder {
        async fn fetch_snapshot(
            &self,
            symbol: &str,
        ) -> Result<NormalizedDelta, ExchangeError> {
            self.0.get(symbol).cloned().ok_or_else(|| {
                ExchangeError::IoError(std::io::Error::other("no snapshot"))
            })
        }
    }

    fn nd(
        symbol: &str,
        bids: Vec<(&str, &str)>,
        asks: Vec<(&str, &str)>,
        update_id: u64,
        sequence: u64,
        is_snapshot: bool,
    ) -> NormalizedDelta {
        NormalizedDelta {
            symbol: symbol.into(),
            bids: bids
                .iter()
                .map(|(p, s)| (p.to_string(), s.to_string()))
                .collect(),
            asks: asks
                .iter()
                .map(|(p, s)| (p.to_string(), s.to_string()))
                .collect(),
            update_id,
            sequence,
            source_orderbook_ts_us: 0,
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            checksum: None,
            orders: Vec::new(),
            is_snapshot,
        }
    }

    #[tokio::test]
    async fn runtime_reconstructs_real_capture_to_the_parity_book() {
        let snap: BinanceDepthSnapshot = serde_json::from_str(REST_SNAPSHOT).unwrap();
        let snap_id = snap.last_update_id;
        let seed_delta = snap.to_normalized("BTCUSDT");
        let pair = TradingPair::new("BTC", "USDT");

        let mut expected = OrderbookDelta::new(pair.clone());
        expected.process(&seed_delta).unwrap();

        let (ev_tx, ev_rx) = mpsc::channel(8192);
        let (out_tx, mut out_rx) = mpsc::channel(8192);
        let (_sd_tx, sd_rx) = watch::channel(false);

        let mut trade_events = 0usize;
        for line in WS_FRAMES.lines() {
            match BinanceDecoder::decode(line.trim()).unwrap() {
                Some(BinanceWssEvent::DepthUpdate(u)) => {
                    if u.last_update_id > snap_id {
                        expected.process(&u.to_normalized()).unwrap();
                    }
                    ev_tx
                        .send(DomainEvent::Book(u.to_normalized()))
                        .await
                        .unwrap();
                }
                Some(BinanceWssEvent::TradeData(t)) => {
                    ev_tx
                        .send(DomainEvent::Trade {
                            trade: Trade {
                                source_trade_ts_us: t.trade_time,
                                local_trade_ts_us: 0,
                                source_trade_rtt_us: 0,
                                pair: pair.clone(),
                                side: TradeSide::Buy,
                                amount: f64_to_decimal(t.quantity.parse().unwrap_or(0.0)),
                                price: f64_to_decimal(t.price.parse().unwrap_or(0.0)),
                                exchange: "binance".into(),
                                id: t.trade_id.to_string(),
                                origin: Default::default(),
                            },
                            sequence: None,
                        })
                        .await
                        .unwrap();
                    trade_events += 1;
                }
                _ => {}
            }
        }
        drop(ev_tx);

        let mut seeds = HashMap::new();
        seeds.insert("BTCUSDT".to_string(), seed_delta);
        let seeder: Arc<dyn RestSnapshot> = Arc::new(MapSeeder(seeds));

        let runtime = SourceRuntime::new(
            "binance",
            SymbolCodec::Concat { upper: true },
            vec!["BTCUSDT".to_string()],
            binance_model(),
            RecoveryAction::RestSnapshot,
            SourceMetrics::default(),
            DeclaredSet::all(),
        );
        runtime.run(ev_rx, Some(seeder), out_tx, sd_rx).await;

        let mut last_book: Option<Orderbook> = None;
        let mut emitted_books = 0usize;
        let mut emitted_trades = 0usize;
        while let Some(ev) = out_rx.recv().await {
            match ev {
                ReconstructedEvent::Book { book, .. } => {
                    last_book = Some(book);
                    emitted_books += 1;
                }
                ReconstructedEvent::Trade(_) => emitted_trades += 1,
            }
        }

        assert_eq!(
            emitted_trades, trade_events,
            "every trade must pass through"
        );
        assert!(
            emitted_books > 50,
            "expected a run of book updates, got {emitted_books}"
        );

        let final_book = last_book.expect("at least one book emitted");
        let exp_bids: HashMap<_, _> = expected.top_bids(usize::MAX).into_iter().collect();
        let exp_asks: HashMap<_, _> = expected.top_asks(usize::MAX).into_iter().collect();
        let got_bids: HashMap<_, _> = final_book
            .bids
            .values()
            .map(|l| (l.price, l.volume))
            .collect();
        let got_asks: HashMap<_, _> = final_book
            .asks
            .values()
            .map(|l| (l.price, l.volume))
            .collect();
        assert_eq!(
            got_bids, exp_bids,
            "final bids diverged from the reconciled book"
        );
        assert_eq!(
            got_asks, exp_asks,
            "final asks diverged from the reconciled book"
        );
    }

    #[tokio::test]
    async fn routes_two_symbols_to_independent_books() {
        let (ev_tx, ev_rx) = mpsc::channel(64);
        let (out_tx, mut out_rx) = mpsc::channel(64);
        let (_sd_tx, sd_rx) = watch::channel(false);

        // Per-symbol seeds; ids chosen so each delta abuts its own snapshot.
        let mut seeds = HashMap::new();
        seeds.insert(
            "BTCUSDT".to_string(),
            nd(
                "BTCUSDT",
                vec![("100", "1")],
                vec![("101", "1")],
                10,
                10,
                true,
            ),
        );
        seeds.insert(
            "ETHUSDT".to_string(),
            nd(
                "ETHUSDT",
                vec![("50", "3")],
                vec![("51", "3")],
                20,
                20,
                true,
            ),
        );
        let seeder: Arc<dyn RestSnapshot> = Arc::new(MapSeeder(seeds));

        // Interleaved deltas + trades for both symbols.
        ev_tx
            .send(DomainEvent::Book(nd(
                "BTCUSDT",
                vec![("100", "2")],
                vec![],
                11,
                11,
                false,
            )))
            .await
            .unwrap();
        ev_tx
            .send(DomainEvent::Book(nd(
                "ETHUSDT",
                vec![],
                vec![("51", "5")],
                21,
                21,
                false,
            )))
            .await
            .unwrap();
        ev_tx
            .send(DomainEvent::Trade {
                trade: Trade {
                    source_trade_ts_us: 1,
                    local_trade_ts_us: 0,
                    source_trade_rtt_us: 0,
                    pair: TradingPair::new("ETH", "USDT"),
                    side: TradeSide::Sell,
                    amount: f64_to_decimal(0.5),
                    price: f64_to_decimal(51.0),
                    exchange: "binance".into(),
                    id: "e1".into(),
                    origin: Default::default(),
                },
                sequence: None,
            })
            .await
            .unwrap();
        drop(ev_tx);

        let runtime = SourceRuntime::new(
            "binance",
            SymbolCodec::Concat { upper: true },
            vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
            binance_model(),
            RecoveryAction::RestSnapshot,
            SourceMetrics::default(),
            DeclaredSet::all(),
        );
        assert_eq!(runtime.len(), 2);
        runtime.run(ev_rx, Some(seeder), out_tx, sd_rx).await;

        // Keep the latest book per pair + count trades.
        let mut latest: HashMap<String, Orderbook> = HashMap::new();
        let mut trades = 0usize;
        while let Some(ev) = out_rx.recv().await {
            match ev {
                ReconstructedEvent::Book { pair, book, .. } => {
                    latest.insert(pair.to_canonical(), book);
                }
                ReconstructedEvent::Trade(_) => trades += 1,
            }
        }

        let btc = latest.get("BTC/USDT").expect("btc book");
        let eth = latest.get("ETH/USDT").expect("eth book");
        // BTC: snapshot bid 100x1 then delta set 100->2; ask 101x1 untouched.
        assert_eq!(
            btc.bids.values().next_back().map(|l| (l.price, l.volume)),
            Some(("100".parse().unwrap(), "2".parse().unwrap()))
        );
        assert_eq!(
            btc.asks.values().next().map(|l| (l.price, l.volume)),
            Some(("101".parse().unwrap(), "1".parse().unwrap()))
        );
        // ETH: snapshot ask 51x3 then delta set 51->5; bid 50x3 untouched.
        assert_eq!(
            eth.asks.values().next().map(|l| (l.price, l.volume)),
            Some(("51".parse().unwrap(), "5".parse().unwrap()))
        );
        assert_eq!(
            eth.bids.values().next_back().map(|l| (l.price, l.volume)),
            Some(("50".parse().unwrap(), "3".parse().unwrap()))
        );
        assert_eq!(trades, 1);
    }

    /// Gate.io-class regression: a FullRefresh venue whose wire symbol parses to
    /// a *non-canonical* pair (`BTC_USDT` → `BTC/USDT`) must still reconstruct.
    /// The runtime stamps the canonical symbol before apply, so the book's
    /// symbol check (`OrderbookDelta::process`) matches instead of raising
    /// `SymbolMismatch` (which would gap every frame and emit nothing).
    #[tokio::test]
    async fn full_refresh_underscore_symbol_routes_without_mismatch() {
        let (ev_tx, ev_rx) = mpsc::channel(8);
        let (out_tx, mut out_rx) = mpsc::channel(8);
        let (_sd_tx, sd_rx) = watch::channel(false);

        // Wire symbol "BTC_USDT" parses to a pair (underscore is a separator),
        // so the unstamped delta would mismatch the canonical "BTC/USDT" book.
        ev_tx
            .send(DomainEvent::Book(nd(
                "BTC_USDT",
                vec![("100", "1")],
                vec![("101", "2")],
                1,
                1,
                false,
            )))
            .await
            .unwrap();
        drop(ev_tx);

        let runtime = SourceRuntime::new(
            "gateio",
            SymbolCodec::Underscore { upper: true },
            vec!["BTC_USDT".to_string()],
            ReconstructionModel::FullRefresh,
            RecoveryAction::Resubscribe,
            SourceMetrics::default(),
            DeclaredSet::all(),
        );
        let outcome = runtime.run(ev_rx, None, out_tx, sd_rx).await;
        assert!(
            matches!(outcome, RuntimeOutcome::Finished),
            "should not resync on a symbol-mismatch gap"
        );

        let ev = out_rx.recv().await.expect("a book should be emitted");
        match ev {
            ReconstructedEvent::Book { pair, book, .. } => {
                assert_eq!(pair, TradingPair::new("BTC", "USDT"));
                assert_eq!(
                    book.bids.values().next_back().map(|l| (l.price, l.volume)),
                    Some(("100".parse().unwrap(), "1".parse().unwrap()))
                );
                assert_eq!(
                    book.asks.values().next().map(|l| (l.price, l.volume)),
                    Some(("101".parse().unwrap(), "2".parse().unwrap()))
                );
            }
            ReconstructedEvent::Trade(_) => panic!("expected a Book, got a Trade"),
        }
    }

    /// Timestamp-model regression: a self-seeding `FullRefresh` venue (no REST)
    /// must thread the delta's `source_orderbook_ts_us`, `local_orderbook_ts_us`, and
    /// `source_orderbook_rtt_us` onto the emitted reconstructed `Orderbook`. The
    /// source ts is *preserved* (exchange event time), the local ts is the
    /// receipt instant, and the rtt is the connection round-trip — all carried
    /// straight through reconstruction rather than re-stamped.
    #[tokio::test]
    async fn book_timestamps_thread_through_reconstruction() {
        let (ev_tx, ev_rx) = mpsc::channel(8);
        let (out_tx, mut out_rx) = mpsc::channel(8);
        let (_sd_tx, sd_rx) = watch::channel(false);

        const SRC_TS: u64 = 1_700_000_000_123_000; // exchange event time (µs)
        const LOCAL_TS: u64 = 1_700_000_000_456_000; // receipt instant (µs)
        const RTT_US: u64 = 4_242; // connection round-trip (µs)

        let mut delta = nd(
            "BTC_USDT",
            vec![("100", "1")],
            vec![("101", "2")],
            1,
            1,
            false,
        );
        delta.source_orderbook_ts_us = SRC_TS;
        delta.local_orderbook_ts_us = LOCAL_TS;
        delta.source_orderbook_rtt_us = RTT_US;
        ev_tx.send(DomainEvent::Book(delta)).await.unwrap();
        drop(ev_tx);

        let runtime = SourceRuntime::new(
            "gateio",
            SymbolCodec::Underscore { upper: true },
            vec!["BTC_USDT".to_string()],
            ReconstructionModel::FullRefresh,
            RecoveryAction::Resubscribe,
            SourceMetrics::default(),
            DeclaredSet::all(),
        );
        runtime.run(ev_rx, None, out_tx, sd_rx).await;

        let ev = out_rx.recv().await.expect("a book should be emitted");
        match ev {
            ReconstructedEvent::Book { book, .. } => {
                assert_eq!(
                    book.source_orderbook_ts_us, SRC_TS,
                    "exchange event time must be preserved through reconstruction"
                );
                assert_eq!(
                    book.local_orderbook_ts_us, LOCAL_TS,
                    "local receipt time must thread through reconstruction"
                );
                assert_eq!(
                    book.source_orderbook_rtt_us, RTT_US,
                    "round-trip must thread through reconstruction"
                );
            }
            ReconstructedEvent::Trade(_) => panic!("expected a Book, got a Trade"),
        }
    }

    #[tokio::test]
    async fn self_seed_gap_resyncs_and_counts() {
        // Bybit-style self-seeded RangeInclusive book: a snapshot then a delta
        // that jumps the sequence must end the runtime with ResyncRequired and
        // bump gaps + resyncs on the shared handle.
        let model = ReconstructionModel::SeqDelta {
            predicate: SeqPredicate::RangeInclusive,
            source: SnapshotSource::WssSelfSeed,
        };
        let (ev_tx, ev_rx) = mpsc::channel(16);
        let (out_tx, mut out_rx) = mpsc::channel(16);
        let (_sd_tx, sd_rx) = watch::channel(false);
        let metrics = SourceMetrics::default();
        let runtime = SourceRuntime::new(
            "bybit",
            SymbolCodec::Concat { upper: true },
            vec!["BTCUSDT".to_string()],
            model,
            RecoveryAction::Resubscribe,
            metrics.clone(),
            DeclaredSet::all(),
        );

        ev_tx
            .send(DomainEvent::Book(nd(
                "BTCUSDT",
                vec![("100", "1")],
                vec![("101", "1")],
                100,
                100,
                true,
            )))
            .await
            .unwrap();
        ev_tx
            .send(DomainEvent::Book(nd(
                "BTCUSDT",
                vec![("100", "2")],
                vec![("101", "2")],
                105,
                105,
                false,
            )))
            .await
            .unwrap();
        drop(ev_tx);

        let outcome = runtime.run(ev_rx, None, out_tx, sd_rx).await;
        while out_rx.recv().await.is_some() {}

        assert!(
            matches!(outcome, RuntimeOutcome::ResyncRequired),
            "gap must force a reconnect"
        );
        let m = metrics.snapshot();
        assert_eq!(m.gaps, 1, "one continuity gap");
        assert_eq!(m.resyncs, 1, "one resync triggered");
        assert_eq!(m.checksum_fail, 0, "not a checksum failure");
    }

    #[tokio::test]
    async fn rest_book_without_seeder_escalates_to_resync() {
        // A REST-seeded model with no seeder used to apply deltas to a
        // known-bad book silently; it now ends with ResyncRequired.
        let (ev_tx, ev_rx) = mpsc::channel(16);
        let (out_tx, mut out_rx) = mpsc::channel(16);
        let (_sd_tx, sd_rx) = watch::channel(false);
        let metrics = SourceMetrics::default();
        let runtime = SourceRuntime::new(
            "binance",
            SymbolCodec::Concat { upper: true },
            vec!["BTCUSDT".to_string()],
            binance_model(),
            RecoveryAction::RestSnapshot,
            metrics.clone(),
            DeclaredSet::all(),
        );

        // Non-snapshot delta on a never-seeded book: gap, no reseed possible.
        ev_tx
            .send(DomainEvent::Book(nd(
                "BTCUSDT",
                vec![("100", "1")],
                vec![("101", "1")],
                10,
                10,
                false,
            )))
            .await
            .unwrap();
        drop(ev_tx);

        let outcome = runtime.run(ev_rx, None, out_tx, sd_rx).await;
        while out_rx.recv().await.is_some() {}

        assert!(
            matches!(outcome, RuntimeOutcome::ResyncRequired),
            "reseed-exhausted/no-seeder book must reconnect, not degrade silently"
        );
        let m = metrics.snapshot();
        assert!(m.gaps >= 1, "gap counted");
        assert!(m.resyncs >= 1, "resync counted");
    }

    #[tokio::test]
    async fn connection_gap_event_forces_resync_and_counts() {
        // A ConnectionGap (the adapter proved messages left the socket
        // undelivered — Coinbase's connection-wide counter) must end the
        // runtime with ResyncRequired even though every applied delta was
        // individually accepted, and must count a gap + a resync.
        let (ev_tx, ev_rx) = mpsc::channel(16);
        let (out_tx, mut out_rx) = mpsc::channel(16);
        let (_sd_tx, sd_rx) = watch::channel(false);
        let metrics = SourceMetrics::default();
        let model = ReconstructionModel::SeqDelta {
            predicate: crate::framework::model::SeqPredicate::Monotonic,
            source: crate::framework::model::SnapshotSource::WssSelfSeed,
        };
        let runtime = SourceRuntime::new(
            "coinbase",
            SymbolCodec::Hyphen,
            vec!["BTC-USD".to_string()],
            model,
            RecoveryAction::Resubscribe,
            metrics.clone(),
            DeclaredSet::all(),
        );

        // Self-seed the book cleanly, then the tracker's verdict arrives.
        ev_tx
            .send(DomainEvent::Book(nd(
                "BTC-USD",
                vec![("100", "1")],
                vec![("101", "1")],
                1,
                1,
                true,
            )))
            .await
            .unwrap();
        ev_tx
            .send(DomainEvent::ConnectionGap { dropped: 3 })
            .await
            .unwrap();
        drop(ev_tx);

        let outcome = runtime.run(ev_rx, None, out_tx, sd_rx).await;
        while out_rx.recv().await.is_some() {}

        assert!(
            matches!(outcome, RuntimeOutcome::ResyncRequired),
            "a proven connection gap must reconnect, never stream on"
        );
        let m = metrics.snapshot();
        assert!(m.gaps >= 1, "gap counted");
        assert!(m.resyncs >= 1, "resync counted");
    }

    #[tokio::test]
    async fn trade_seq_carry_counts_cross_reconnect_gaps_exactly() {
        // Connection 1 ends with last trade seq 100 (persisted to the carry).
        // Connection 2's first trade arrives at seq 105 — the 4 prints lost in
        // the outage must be counted by the SAME exact arithmetic instead of
        // silently resetting, and the confidence must read Exact.
        let carry: Arc<std::sync::Mutex<HashMap<TradingPair, u64>>> = Arc::default();
        let pair = TradingPair::new("BTC", "USDT");
        carry.lock().unwrap().insert(pair.clone(), 100);

        let (ev_tx, ev_rx) = mpsc::channel(16);
        let (out_tx, mut out_rx) = mpsc::channel(16);
        let (_sd_tx, sd_rx) = watch::channel(false);
        let metrics = SourceMetrics::default();
        let runtime = SourceRuntime::new(
            "binance",
            SymbolCodec::Concat { upper: true },
            vec!["BTCUSDT".to_string()],
            binance_model(),
            RecoveryAction::RestSnapshot,
            metrics.clone(),
            DeclaredSet::all(),
        )
        .with_trade_seq_carry(carry.clone());

        let mut t = aetelier_types::trades::Trade::random();
        t.pair = pair.clone();
        ev_tx
            .send(DomainEvent::Trade {
                trade: t,
                sequence: Some(105),
            })
            .await
            .unwrap();
        drop(ev_tx);
        let _ = runtime.run(ev_rx, None, out_tx, sd_rx).await;
        while out_rx.recv().await.is_some() {}

        let m = metrics.snapshot();
        assert_eq!(m.trades_lost, 4, "outage prints counted exactly");
        assert_eq!(m.trade_gaps, 1);
        assert_eq!(
            m.trade_loss_confidence,
            crate::framework::budget::TradeLossConfidence::Exact as u64,
            "a sequenced trade proves exact accounting"
        );
        assert_eq!(
            carry.lock().unwrap().get(&pair).copied(),
            Some(105),
            "the carry advances for the NEXT connection"
        );
    }

    #[tokio::test]
    async fn feeds_reach_live_on_first_book_and_trade() {
        let pair = TradingPair::new("BTC", "USDT");
        let feeds = FeedSet::new(vec![
            Feed::new(Exchange::Binance, pair.clone(), FeedDatatype::Orders),
            Feed::new(Exchange::Binance, pair.clone(), FeedDatatype::Trades),
        ])
        .into_shared();
        if let Ok(mut fs) = feeds.lock() {
            fs.mark_all_subscribing();
        }

        let (ev_tx, ev_rx) = mpsc::channel(64);
        let (out_tx, mut out_rx) = mpsc::channel(64);
        let (_sd_tx, sd_rx) = watch::channel(false);

        ev_tx
            .send(DomainEvent::Book(nd(
                "BTCUSDT",
                vec![("100", "1")],
                vec![("101", "1")],
                101,
                101,
                false,
            )))
            .await
            .unwrap();
        ev_tx
            .send(DomainEvent::Trade {
                trade: Trade {
                    source_trade_ts_us: 1,
                    local_trade_ts_us: 0,
                    source_trade_rtt_us: 0,
                    pair: pair.clone(),
                    side: TradeSide::Buy,
                    amount: f64_to_decimal(1.0),
                    price: f64_to_decimal(100.0),
                    exchange: "binance".into(),
                    id: "1".into(),
                    origin: Default::default(),
                },
                sequence: None,
            })
            .await
            .unwrap();
        drop(ev_tx);

        let seeder: Arc<dyn RestSnapshot> =
            Arc::new(OnceSeeder(std::sync::Mutex::new(VecDeque::from([nd(
                "BTC/USDT",
                vec![("100", "2")],
                vec![("101", "2")],
                100,
                100,
                true,
            )]))));
        let runtime = SourceRuntime::new(
            "binance",
            SymbolCodec::Concat { upper: true },
            vec!["BTCUSDT".to_string()],
            binance_model(),
            RecoveryAction::RestSnapshot,
            SourceMetrics::default(),
            DeclaredSet::all(),
        )
        .with_feeds(feeds.clone());
        runtime.run(ev_rx, Some(seeder), out_tx, sd_rx).await;
        while out_rx.try_recv().is_ok() {}

        let snap = feeds.lock().unwrap().snapshot();
        let state = |dt| snap.iter().find(|f| f.datatype == dt).unwrap().state;
        assert_eq!(state(FeedDatatype::Orders), FeedState::Live);
        assert_eq!(state(FeedDatatype::Trades), FeedState::Live);
    }

    #[tokio::test]
    async fn trade_gap_is_accounted_never_escalated() {
        let pair = TradingPair::new("BTC", "USDT");
        let (ev_tx, ev_rx) = mpsc::channel(64);
        let (out_tx, mut out_rx) = mpsc::channel(64);
        let (_sd_tx, sd_rx) = watch::channel(false);
        let metrics = SourceMetrics::default();

        let mk = |seq: u64| DomainEvent::Trade {
            trade: Trade {
                source_trade_ts_us: seq,
                local_trade_ts_us: 0,
                source_trade_rtt_us: 0,
                pair: pair.clone(),
                side: TradeSide::Buy,
                amount: f64_to_decimal(1.0),
                price: f64_to_decimal(100.0),
                exchange: "binance".into(),
                id: seq.to_string(),
                origin: Default::default(),
            },
            sequence: Some(seq),
        };
        for seq in [10u64, 11, 15, 15, 16] {
            ev_tx.send(mk(seq)).await.unwrap();
        }
        drop(ev_tx);

        let runtime = SourceRuntime::new(
            "binance",
            SymbolCodec::Concat { upper: true },
            vec!["BTCUSDT".to_string()],
            ReconstructionModel::FullRefresh,
            RecoveryAction::Resubscribe,
            metrics.clone(),
            DeclaredSet::all(),
        );
        let outcome = runtime.run(ev_rx, None, out_tx, sd_rx).await;
        assert!(matches!(outcome, RuntimeOutcome::Finished));

        let mut forwarded = 0;
        while out_rx.try_recv().is_ok() {
            forwarded += 1;
        }
        assert_eq!(forwarded, 5, "every print forwarded, duplicate included");

        let m = metrics.snapshot();
        assert_eq!(m.trade_gaps, 1);
        assert_eq!(m.trades_lost, 3);
        assert_eq!(m.gaps, 0, "trade losses must not count as book gaps");
        assert_eq!(m.resyncs, 0, "trade losses must never escalate");
    }

    #[tokio::test(start_paused = true)]
    async fn quiet_stream_recovers_via_the_seed_retry_ladder() {
        let (ev_tx, ev_rx) = mpsc::channel::<DomainEvent>(8);
        let (out_tx, mut out_rx) = mpsc::channel(8);
        let (_sd_tx, sd_rx) = watch::channel(false);
        let metrics = SourceMetrics::default();

        let seeder: Arc<dyn RestSnapshot> = Arc::new(FlakySeeder {
            fails: std::sync::Mutex::new(2),
            snapshot: nd(
                "BTC/USDT",
                vec![("100", "2")],
                vec![("101", "2")],
                100,
                100,
                true,
            ),
        });
        let runtime = SourceRuntime::new(
            "binance",
            SymbolCodec::Concat { upper: true },
            vec!["BTCUSDT".to_string()],
            binance_model(),
            RecoveryAction::RestSnapshot,
            metrics.clone(),
            DeclaredSet::all(),
        );
        let handle = tokio::spawn(runtime.run(ev_rx, Some(seeder), out_tx, sd_rx));

        // No deltas ever arrive — the retry ladder alone must seed the book.
        let emitted =
            tokio::time::timeout(std::time::Duration::from_secs(30), out_rx.recv())
                .await
                .expect("retry ladder must seed a quiet stream")
                .expect("book emit");
        assert!(matches!(emitted, ReconstructedEvent::Book { .. }));

        drop(ev_tx);
        let outcome = handle.await.unwrap();
        assert!(matches!(outcome, RuntimeOutcome::Finished));
        assert_eq!(metrics.snapshot().resyncs, 2, "one bump per failed fetch");
    }

    #[tokio::test(start_paused = true)]
    async fn seed_retry_exhaustion_escalates_to_reconnect() {
        let (ev_tx, ev_rx) = mpsc::channel::<DomainEvent>(8);
        let (out_tx, mut out_rx) = mpsc::channel(8);
        let (_sd_tx, sd_rx) = watch::channel(false);

        let seeder: Arc<dyn RestSnapshot> = Arc::new(FlakySeeder {
            fails: std::sync::Mutex::new(u32::MAX),
            snapshot: nd("BTC/USDT", vec![], vec![], 1, 1, true),
        });
        let runtime = SourceRuntime::new(
            "binance",
            SymbolCodec::Concat { upper: true },
            vec!["BTCUSDT".to_string()],
            binance_model(),
            RecoveryAction::RestSnapshot,
            SourceMetrics::default(),
            DeclaredSet::all(),
        );
        let handle = tokio::spawn(runtime.run(ev_rx, Some(seeder), out_tx, sd_rx));
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(60), handle)
            .await
            .expect("exhaustion must escalate, not strand the book")
            .unwrap();
        assert!(matches!(outcome, RuntimeOutcome::ResyncRequired));
        assert!(
            out_rx.try_recv().is_err(),
            "nothing emitted from an unseeded book"
        );
        drop(ev_tx);
    }

    #[tokio::test(start_paused = true)]
    async fn gapped_book_pins_the_feed_in_resubscribing_when_reseed_fails() {
        let pair = TradingPair::new("BTC", "USDT");
        let feeds = FeedSet::new(vec![Feed::new(
            Exchange::Binance,
            pair.clone(),
            FeedDatatype::Orders,
        )])
        .into_shared();
        if let Ok(mut fs) = feeds.lock() {
            fs.mark_all_subscribing();
        }

        let (ev_tx, ev_rx) = mpsc::channel(64);
        let (out_tx, mut out_rx) = mpsc::channel(64);
        let (_sd_tx, sd_rx) = watch::channel(false);

        ev_tx
            .send(DomainEvent::Book(nd(
                "BTCUSDT",
                vec![("100", "1")],
                vec![("101", "1")],
                101,
                101,
                false,
            )))
            .await
            .unwrap();
        ev_tx
            .send(DomainEvent::Book(nd(
                "BTCUSDT",
                vec![("100", "3")],
                vec![("101", "3")],
                151,
                150,
                false,
            )))
            .await
            .unwrap();
        drop(ev_tx);

        let seeder: Arc<dyn RestSnapshot> =
            Arc::new(OnceSeeder(std::sync::Mutex::new(VecDeque::from([nd(
                "BTC/USDT",
                vec![("100", "2")],
                vec![("101", "2")],
                100,
                100,
                true,
            )]))));
        let runtime = SourceRuntime::new(
            "binance",
            SymbolCodec::Concat { upper: true },
            vec!["BTCUSDT".to_string()],
            binance_model(),
            RecoveryAction::RestSnapshot,
            SourceMetrics::default(),
            DeclaredSet::all(),
        )
        .with_feeds(feeds.clone());
        runtime.run(ev_rx, Some(seeder), out_tx, sd_rx).await;
        while out_rx.try_recv().is_ok() {}

        let snap = feeds.lock().unwrap().snapshot();
        assert_eq!(snap[0].state, FeedState::Resubscribing);
    }

    fn htx_model() -> ReconstructionModel {
        ReconstructionModel::SeqDelta {
            predicate: SeqPredicate::ExactPrev,
            source: SnapshotSource::ReqOnSocket,
        }
    }

    fn htx_runtime(metrics: SourceMetrics) -> SourceRuntime {
        SourceRuntime::new(
            "htx",
            SymbolCodec::Concat { upper: true },
            vec!["BTCUSDT".to_string()],
            htx_model(),
            RecoveryAction::Resubscribe,
            metrics,
            DeclaredSet::all(),
        )
    }

    #[tokio::test]
    async fn req_on_socket_seed_lands_mid_stream_and_reconciles() {
        let (ev_tx, ev_rx) = mpsc::channel(16);
        let (out_tx, mut out_rx) = mpsc::channel(16);
        let (_sd_tx, sd_rx) = watch::channel(false);
        let metrics = SourceMetrics::default();
        let runtime = htx_runtime(metrics.clone());

        ev_tx
            .send(DomainEvent::Book(nd(
                "BTCUSDT",
                vec![("100", "1")],
                vec![("101", "1")],
                100,
                99,
                false,
            )))
            .await
            .unwrap();
        ev_tx
            .send(DomainEvent::Book(nd(
                "BTCUSDT",
                vec![("100", "2")],
                vec![("101", "2")],
                106,
                105,
                false,
            )))
            .await
            .unwrap();
        ev_tx
            .send(DomainEvent::Book(nd(
                "BTCUSDT",
                vec![("99", "5")],
                vec![("102", "5")],
                105,
                0,
                true,
            )))
            .await
            .unwrap();
        ev_tx
            .send(DomainEvent::Book(nd(
                "BTCUSDT",
                vec![("99", "6")],
                vec![("102", "6")],
                107,
                106,
                false,
            )))
            .await
            .unwrap();
        drop(ev_tx);

        let outcome = runtime.run(ev_rx, None, out_tx, sd_rx).await;
        let mut books = 0;
        while let Some(ev) = out_rx.recv().await {
            if matches!(ev, ReconstructedEvent::Book { .. }) {
                books += 1;
            }
        }

        assert!(
            matches!(outcome, RuntimeOutcome::Finished),
            "deltas racing the in-band seed must not force a reconnect"
        );
        assert_eq!(books, 3, "seed emit + straddling replay + live delta");
        let m = metrics.snapshot();
        assert_eq!(m.gaps, 0, "the race is not a gap");
        assert_eq!(m.resyncs, 0, "the race is not a resync");
    }

    #[tokio::test]
    async fn req_on_socket_replay_gap_reconnects() {
        let (ev_tx, ev_rx) = mpsc::channel(16);
        let (out_tx, mut out_rx) = mpsc::channel(16);
        let (_sd_tx, sd_rx) = watch::channel(false);
        let metrics = SourceMetrics::default();
        let runtime = htx_runtime(metrics.clone());

        ev_tx
            .send(DomainEvent::Book(nd(
                "BTCUSDT",
                vec![("100", "2")],
                vec![("101", "2")],
                106,
                105,
                false,
            )))
            .await
            .unwrap();
        ev_tx
            .send(DomainEvent::Book(nd(
                "BTCUSDT",
                vec![("100", "3")],
                vec![("101", "3")],
                120,
                119,
                false,
            )))
            .await
            .unwrap();
        ev_tx
            .send(DomainEvent::Book(nd(
                "BTCUSDT",
                vec![("99", "5")],
                vec![("102", "5")],
                105,
                0,
                true,
            )))
            .await
            .unwrap();
        drop(ev_tx);

        let outcome = runtime.run(ev_rx, None, out_tx, sd_rx).await;
        while out_rx.recv().await.is_some() {}

        assert!(
            matches!(outcome, RuntimeOutcome::ResyncRequired),
            "a broken chain in the buffered tail must reconnect (reconnect re-sends the req)"
        );
        let m = metrics.snapshot();
        assert!(m.gaps >= 1);
        assert!(m.resyncs >= 1);
    }

    #[tokio::test(start_paused = true)]
    async fn req_on_socket_seed_deadline_escalates_to_resync() {
        let (ev_tx, ev_rx) = mpsc::channel(16);
        let (out_tx, mut out_rx) = mpsc::channel(16);
        let (_sd_tx, sd_rx) = watch::channel(false);
        let metrics = SourceMetrics::default();
        let runtime = htx_runtime(metrics.clone());

        ev_tx
            .send(DomainEvent::Book(nd(
                "BTCUSDT",
                vec![("100", "1")],
                vec![("101", "1")],
                100,
                99,
                false,
            )))
            .await
            .unwrap();

        let outcome = tokio::time::timeout(
            Duration::from_secs(60),
            runtime.run(ev_rx, None, out_tx, sd_rx),
        )
        .await
        .expect("deadline must end the runtime while the event stream stays open");
        assert!(
            matches!(outcome, RuntimeOutcome::ResyncRequired),
            "a seed that never arrives must reconnect, not stall buffering forever"
        );
        assert!(
            out_rx.try_recv().is_err(),
            "nothing emitted from an unseeded book"
        );
        let m = metrics.snapshot();
        assert!(m.resyncs >= 1);
        drop(ev_tx);
    }

    #[tokio::test]
    async fn undeclared_trades_are_dropped_and_counted_books_pass() {
        let (ev_tx, ev_rx) = mpsc::channel(16);
        let (out_tx, mut out_rx) = mpsc::channel(16);
        let (_sd_tx, sd_rx) = watch::channel(false);
        let metrics = SourceMetrics::default();
        let mut only_books = DeclaredSet::default();
        let mut section =
            aetelier_types::config::markets::market_config::DataTypesSection::default();
        section.orderbook.enabled = true;
        only_books = section.declared_set();
        let model = ReconstructionModel::SeqDelta {
            predicate: SeqPredicate::RangeInclusive,
            source: SnapshotSource::WssSelfSeed,
        };
        let runtime = SourceRuntime::new(
            "bybit",
            SymbolCodec::Concat { upper: true },
            vec!["BTCUSDT".to_string()],
            model,
            RecoveryAction::Resubscribe,
            metrics.clone(),
            only_books,
        );

        ev_tx
            .send(DomainEvent::Book(nd(
                "BTCUSDT",
                vec![("100", "1")],
                vec![("101", "1")],
                100,
                100,
                true,
            )))
            .await
            .unwrap();
        ev_tx
            .send(DomainEvent::Trade {
                trade: Trade {
                    source_trade_ts_us: 1,
                    local_trade_ts_us: 0,
                    source_trade_rtt_us: 0,
                    pair: TradingPair::new("BTC", "USDT"),
                    side: TradeSide::Buy,
                    amount: aetelier_types::orderbooks::f64_to_decimal(1.0),
                    price: aetelier_types::orderbooks::f64_to_decimal(100.0),
                    exchange: "bybit".into(),
                    id: "t1".into(),
                    origin: Default::default(),
                },
                sequence: None,
            })
            .await
            .unwrap();
        drop(ev_tx);

        let outcome = runtime.run(ev_rx, None, out_tx, sd_rx).await;
        let mut books = 0;
        let mut trades = 0;
        while let Some(ev) = out_rx.recv().await {
            match ev {
                ReconstructedEvent::Book { .. } => books += 1,
                ReconstructedEvent::Trade(_) => trades += 1,
            }
        }
        assert!(matches!(outcome, RuntimeOutcome::Finished));
        assert_eq!(books, 1, "declared orderbook passes");
        assert_eq!(trades, 0, "undeclared trade must not emit");
        let m = metrics.snapshot();
        assert_eq!(m.undeclared_dropped, 1, "the drop is counted, not silent");
    }

    #[tokio::test]
    async fn wss_self_seed_venues_stay_passthrough_snapshot_first() {
        for (venue, predicate) in [
            ("bybit", SeqPredicate::RangeInclusive),
            ("bitget", SeqPredicate::ExactPrev),
            ("poloniex", SeqPredicate::ExactPrev),
        ] {
            let model = ReconstructionModel::SeqDelta {
                predicate,
                source: SnapshotSource::WssSelfSeed,
            };
            let (ev_tx, ev_rx) = mpsc::channel(16);
            let (out_tx, mut out_rx) = mpsc::channel(16);
            let (_sd_tx, sd_rx) = watch::channel(false);
            let metrics = SourceMetrics::default();
            let runtime = SourceRuntime::new(
                venue,
                SymbolCodec::Concat { upper: true },
                vec!["BTCUSDT".to_string()],
                model,
                RecoveryAction::Resubscribe,
                metrics.clone(),
                DeclaredSet::all(),
            );

            ev_tx
                .send(DomainEvent::Book(nd(
                    "BTCUSDT",
                    vec![("100", "1")],
                    vec![("101", "1")],
                    100,
                    100,
                    true,
                )))
                .await
                .unwrap();
            ev_tx
                .send(DomainEvent::Book(nd(
                    "BTCUSDT",
                    vec![("100", "2")],
                    vec![("101", "2")],
                    101,
                    100,
                    false,
                )))
                .await
                .unwrap();
            drop(ev_tx);

            let outcome = runtime.run(ev_rx, None, out_tx, sd_rx).await;
            let mut books = 0;
            while let Some(ev) = out_rx.recv().await {
                if matches!(ev, ReconstructedEvent::Book { .. }) {
                    books += 1;
                }
            }
            assert!(
                matches!(outcome, RuntimeOutcome::Finished),
                "{venue}: snapshot-first self-seed must stream unchanged"
            );
            assert_eq!(books, 2, "{venue}: snapshot + delta both emit");
            let m = metrics.snapshot();
            assert_eq!(m.gaps, 0, "{venue}");
            assert_eq!(m.resyncs, 0, "{venue}");
        }
    }
}
