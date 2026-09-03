//! Data-plane sync seam: normalization plus the order-book and trade-book
//! reconstruction types. `Normalizer` turns a venue's decoded events into
//! `DomainEvent`s; `SourcedOrderbook` / `SourcedTradebook` rebuild live books
//! over the `Empty → Synced ⇄ Gapped → Closed` lifecycle, with the
//! source-agnostic apply selected by a `ReconstructionModel`.

use std::collections::HashMap;

use rust_decimal::Decimal;

use aetelier_types::orderbooks::{NormalizedDelta, OrderbookDelta};
use aetelier_types::trades::Trade;
use aetelier_types::trading_pair::TradingPair;

/// The normalized, venue-agnostic market event the sync layer consumes. A
/// normalizer returns a `Vec<DomainEvent>`, so a batched-trade frame yields
/// one event per trade rather than dropping all but the head.
#[derive(Debug, Clone)]
pub enum DomainEvent {
    /// An order-book snapshot or incremental delta (pre-reconstruction).
    Book(NormalizedDelta),
    /// A public trade print, with the venue trade-sequence if any.
    Trade {
        trade: Trade,
        sequence: Option<u64>,
    },
    /// Connection-level continuity loss: `dropped` messages provably left the
    /// socket undelivered (emitted today only by Coinbase's sequence tracker —
    /// its per-book `Monotonic` predicate cannot see a drop, DAT-OB-INV-10).
    /// Every book fed by this connection is suspect; the runtime gaps them
    /// eagerly and requests a resync.
    ConnectionGap {
        dropped: u64,
    },
    FundingRate(aetelier_types::funding::FundingRate),
    OpenInterest(aetelier_types::open_interest::OpenInterest),
    FundingSettlement(aetelier_types::funding::FundingSettlement),
}

impl DomainEvent {
    /// Stamp the transport-side timestamps onto this event: the local receipt
    /// time (UTC epoch µs, the platform standard) and the connection
    /// round-trip (µs). Called once per event by the framework driver before
    /// the event leaves the transport layer.
    pub(crate) fn stamp_local(
        &mut self,
        local_us: u64,
        rtt_us: u64,
        recv_seq: u64,
        conn_epoch_us: u64,
    ) {
        match self {
            DomainEvent::Book(d) => {
                d.local_orderbook_ts_us = local_us;
                d.source_orderbook_rtt_us = rtt_us;
            }
            DomainEvent::Trade { trade, .. } => {
                trade.local_trade_ts_us = local_us;
                trade.source_trade_rtt_us = rtt_us;
            }
            DomainEvent::FundingRate(fr) => {
                fr.local_funding_ts_us = local_us;
                fr.recv_seq = recv_seq;
                fr.conn_epoch_us = conn_epoch_us;
            }
            DomainEvent::OpenInterest(oi) => {
                oi.local_oi_ts_us = local_us;
                oi.recv_seq = recv_seq;
                oi.conn_epoch_us = conn_epoch_us;
            }
            DomainEvent::FundingSettlement(fs) => {
                if fs.local_ts_us == 0 {
                    fs.local_ts_us = local_us;
                }
            }
            DomainEvent::ConnectionGap { .. } => {}
        }
    }
}

/// Maps a venue's decoded WSS event into zero or more `DomainEvent`s. One
/// implementation per venue.
pub trait Normalizer: Send + Sync + 'static {
    /// The decoder event type for this venue (`= <D as WssDecoder>::Event`).
    type Event: Send + 'static;

    /// Normalize one decoded event into zero or more `DomainEvent`s. The
    /// normalizer derives the canonical pair **from the event's venue symbol**
    /// (a socket is multi-symbol), so no pair is passed in; the resulting
    /// `DomainEvent` carries the pair/symbol for downstream routing.
    fn normalize(&self, event: Self::Event) -> Vec<DomainEvent>;
}

/// Normalize an epoch timestamp of unknown unit (seconds / milliseconds /
/// microseconds / nanoseconds) to **UTC epoch microseconds**, by magnitude;
/// `0` stays `0`.
///
/// Venues report the book event time in different units (Gate.io ms, KuCoin
/// match ns, …). Modern epochs are well-separated by digit count, so classifying
/// by magnitude is unambiguous for any plausible market timestamp and removes the
/// per-venue unit-guessing risk. Returns UTC epoch microseconds (the platform standard).
pub(crate) fn epoch_to_us(v: u64) -> u64 {
    match v {
        0 => 0,
        v if v >= 100_000_000_000_000_000 => v / 1_000,
        v if v >= 100_000_000_000_000 => v,
        v if v >= 100_000_000_000 => v * 1_000,
        v => v * 1_000_000,
    }
}

/// OrderBook FSM state: `Empty → Synced ⇄ Gapped → Closed`. A gap is a
/// recoverable state: the book is invalid until a re-seed reconciles it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderBookState {
    /// No seed applied yet.
    Empty,
    /// Current and source-continuity verified.
    Synced,
    /// Continuity broke (`ResyncNeeded` raised); applies nothing until re-seeded.
    Gapped,
    /// Terminal — the bound feed closed or the task drained.
    Closed,
}

/// TradeBook lifecycle: `Empty → Synced → Closed`. A missed venue trade
/// sequence is NOT a state: the print is historical and unrecoverable (no
/// re-seed path exists for trades), so continuity breaks are counted loss
/// events (`gap_events`/`trades_lost`) and the book stays `Synced`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeBookState {
    /// No print appended yet.
    Empty,
    /// Appending; continuity accounted per print.
    Synced,
    /// Terminal — the bound feed closed or the task drained.
    Closed,
}

/// Outcome of appending one trade print.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeApply {
    /// New print; continuity held (or the venue supplies no sequence).
    Applied,
    /// `seq <= last`: duplicate / out-of-order print — not counted.
    Duplicate,
    /// Continuity broke: `missed` prints are permanently lost. The carried
    /// print itself is applied and the book stays `Synced`.
    GappedApplied { missed: u64 },
}

/// The per-venue reconstruction model, keyed per channel (a venue may run
/// FullRefresh on one channel and SeqDelta on another).
#[derive(Debug, Clone)]
pub enum ReconstructionModel {
    /// Every frame is a complete top-N book (Upbit, OKX `books5`, Coinbase).
    FullRefresh,
    /// Incremental deltas seeded by a snapshot, validated by sequence.
    SeqDelta {
        predicate: SeqPredicate,
        source: SnapshotSource,
    },
    /// Incremental deltas validated by a checksum (OKX/Kraken/Bitget).
    ChecksumDelta { fmt: ChecksumFmt },
    /// Per-order (L3) book keyed by order id (Bitso `diff-orders`).
    L3 { source: SnapshotSource },
}

impl ReconstructionModel {
    /// The seeding snapshot source, when the model seeds at all
    /// (`FullRefresh`/`ChecksumDelta` reconstruct without a seed).
    pub fn snapshot_source(&self) -> Option<SnapshotSource> {
        match self {
            ReconstructionModel::SeqDelta { source, .. }
            | ReconstructionModel::L3 { source } => Some(*source),
            ReconstructionModel::FullRefresh
            | ReconstructionModel::ChecksumDelta { .. } => None,
        }
    }

    /// Whether reconstruction requires a REST seed — derived from
    /// [`snapshot_source`](Self::snapshot_source), never declared separately.
    pub fn class_label(&self) -> &'static str {
        match self {
            ReconstructionModel::FullRefresh => "full_refresh",
            ReconstructionModel::SeqDelta { .. } => "seq_delta",
            ReconstructionModel::ChecksumDelta { .. } => "checksum_delta",
            ReconstructionModel::L3 { .. } => "l3",
        }
    }

    pub fn book_level(&self) -> &'static str {
        match self {
            ReconstructionModel::L3 { .. } => "L3",
            _ => "L2",
        }
    }

    pub fn needs_rest(&self) -> bool {
        self.snapshot_source() == Some(SnapshotSource::RestSnapshot)
    }

    pub fn seeds_out_of_band(&self) -> bool {
        matches!(
            self.snapshot_source(),
            Some(SnapshotSource::RestSnapshot) | Some(SnapshotSource::ReqOnSocket)
        )
    }

    pub fn seq_is_connection_scoped(&self) -> bool {
        matches!(
            self,
            ReconstructionModel::SeqDelta {
                predicate: SeqPredicate::Monotonic,
                ..
            }
        )
    }

    pub fn replay_dedup_sound(&self) -> bool {
        self.snapshot_source().is_some() && !self.seq_is_connection_scoped()
    }

    pub fn supports_in_band_reseed(&self) -> bool {
        matches!(self.snapshot_source(), Some(SnapshotSource::ReqOnSocket))
    }

    /// The recovery action continuity breaks resolve through. `ReqOnSocket`
    /// venues (HTX) map to `Resubscribe`: the declared in-band REQ re-seed
    /// has no runtime implementation, and a fresh subscribe re-seeds the
    /// same way.
    pub fn recovery_action(&self) -> RecoveryAction {
        if self.needs_rest() {
            RecoveryAction::RestSnapshot
        } else {
            RecoveryAction::Resubscribe
        }
    }
}

/// Sequence-continuity predicate for `SeqDelta`.
#[derive(Debug, Clone, Copy)]
pub enum SeqPredicate {
    /// Binance: each delta's id range must abut the last applied id —
    /// `first_id <= last + 1 <= final_id` (`sequence` carries `first_id`,
    /// `update_id` the `final_id`). A forward jump (`first_id > last + 1`) is a
    /// dropped-frame gap, not just "advancing", so it raises `ResyncNeeded`.
    RangeInclusive,
    /// HTX/Poloniex: each delta's prev-id pointer must equal the last applied
    /// id. The prev pointer rides on `NormalizedDelta.sequence` (e.g. HTX
    /// `prevSeqNum`), so continuity is `delta.sequence == last`.
    ExactPrev,
    /// Coinbase: the id is a *connection-wide* counter (bumped by every message
    /// on the socket — trades, heartbeats, other products), not a per-book
    /// contiguous id, so only strict monotonic advance can be required:
    /// `delta.update_id > last`. Per-book contiguity is unprovable here; whole-
    /// connection drop detection would need a socket-level sequence tracker.
    Monotonic,
}

/// Where the seeding snapshot comes from. The single source of truth for
/// the seeding taxonomy: REST need and recovery action derive from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotSource {
    /// REST GET over the shared rate-limited `HttpClient` (Binance/KuCoin).
    RestSnapshot,
    /// In-band request on the same socket (HTX `{req:...}`).
    ReqOnSocket,
    /// The first post-subscribe frame is the snapshot (Bybit/Poloniex).
    WssSelfSeed,
}

/// What to do when continuity breaks; carried by `ResyncNeeded.action`.
#[derive(Debug, Clone, Copy)]
pub enum RecoveryAction {
    /// Unsubscribe + resubscribe (Poloniex).
    Resubscribe,
    /// Re-fetch the REST snapshot and replay (Binance/KuCoin/Bitso).
    RestSnapshot,
    /// Re-issue the in-band REQ seed (HTX).
    ReqOnSocket,
}

/// Per-venue checksum recipe for CRC32-validated books (see
/// [`checksum`](super::checksum); each is validated against real frames).
#[derive(Debug, Clone)]
pub enum ChecksumFmt {
    /// OKX: CRC32 of the top-25 levels interleaved bid-then-ask per rank, as a
    /// signed i32.
    OkxTop25,
    /// Kraken: CRC32 (unsigned) of the top-10 asks then bids, each price and qty
    /// with the decimal point and leading zeros removed.
    KrakenTop10,
    /// Bitget's retired CRC32 channel (the live `books` feed moved to seq/pseq,
    /// so Bitget is now `SeqDelta`); kept for completeness, shares OKX's recipe.
    BitgetBidFirstTop25,
}

/// Why a book lost continuity. Structured so the runtime can count checksum
/// failures distinctly (a corrupted book) from sequence/refresh gaps without
/// matching on message strings.
const L3_MAX_ORDERS: usize = 200_000;

#[derive(Debug, Clone)]
pub enum ResyncReason {
    /// CRC32 book checksum mismatched after applying a delta.
    Checksum(String),
    /// Sequence/continuity break or any other reconstruction fault.
    Other(String),
}

impl ResyncReason {
    /// Whether this is a checksum-validation failure.
    pub fn is_checksum(&self) -> bool {
        matches!(self, Self::Checksum(_))
    }
}

impl std::fmt::Display for ResyncReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Checksum(m) | Self::Other(m) => write!(f, "{m}"),
        }
    }
}

impl From<String> for ResyncReason {
    fn from(m: String) -> Self {
        Self::Other(m)
    }
}

impl From<&str> for ResyncReason {
    fn from(m: &str) -> Self {
        Self::Other(m.to_string())
    }
}

/// Signal that a book lost continuity and must be re-seeded.
#[derive(Debug, Clone)]
pub struct ResyncNeeded {
    pub reason: ResyncReason,
    pub action: RecoveryAction,
}

/// Output of applying one input to a book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookOutput {
    /// An incremental delta / trade was applied.
    Applied,
    /// A full snapshot / replacement is now current.
    Snapshot,
}

/// Reconstructs a live order book from a feed's normalized deltas. The
/// source-agnostic `apply` is selected by the `ReconstructionModel`.
/// States: `Empty → Synced ⇄ Gapped → Closed`.
pub struct SourcedOrderbook {
    book: OrderbookDelta,
    model: ReconstructionModel,
    recovery: RecoveryAction,
    state: OrderBookState,
    last: Option<u64>,
    /// L3 order map (order id → position); populated only for the `L3` model.
    l3_orders: HashMap<String, L3Pos>,
    /// L3 aggregate per price level (`(is_ask, price)` → summed size); the
    /// projection of `l3_orders` onto L2, kept incrementally so each order
    /// change is O(1) rather than re-summing the whole book.
    l3_levels: HashMap<(bool, Decimal), Decimal>,
}

/// One resting order's position in the L3 book.
#[derive(Clone, Copy)]
struct L3Pos {
    is_ask: bool,
    price: Decimal,
    size: Decimal,
}

impl SourcedOrderbook {
    pub fn new(
        pair: TradingPair,
        model: ReconstructionModel,
        recovery: RecoveryAction,
    ) -> Self {
        let book = match &model {
            ReconstructionModel::ChecksumDelta {
                fmt: ChecksumFmt::KrakenTop10,
            } => OrderbookDelta::new(pair).with_max_depth(Some(10)),
            _ => OrderbookDelta::new(pair),
        };
        Self {
            book,
            model,
            recovery,
            state: OrderBookState::Empty,
            last: None,
            l3_orders: HashMap::new(),
            l3_levels: HashMap::new(),
        }
    }

    /// Arm the configured order-book depth (TOML `datatypes.orderbook.depth`)
    /// on non-checksum models. Checksum venues must hold the book at the
    /// recipe's mandated depth (set in [`SourcedOrderbook::new`]) — a config
    /// depth that pruned them would change the hashed levels and the venue
    /// checksum would stop matching, so those models are left untouched. For
    /// every other model this prunes the reconstructed book to the configured
    /// depth after each `process()`; `None` keeps the full book.
    pub fn arm_config_depth(&mut self, max_depth: Option<usize>) {
        if matches!(self.model, ReconstructionModel::ChecksumDelta { .. }) {
            return;
        }
        self.book.max_depth = max_depth;
    }

    pub fn book(&self) -> &OrderbookDelta {
        &self.book
    }
    pub fn state(&self) -> OrderBookState {
        self.state
    }
    pub fn close(&mut self) {
        self.state = OrderBookState::Closed;
    }

    /// The source-agnostic apply. Either advances `Synced` or transitions to
    /// `Gapped` and raises `ResyncNeeded` — never silently drops.
    pub fn apply(&mut self, delta: NormalizedDelta) -> Result<BookOutput, ResyncNeeded> {
        let output = match self.model.clone() {
            ReconstructionModel::FullRefresh => self.apply_full_refresh(delta),
            ReconstructionModel::SeqDelta { predicate, .. } => {
                self.apply_seq_delta(delta, predicate)
            }
            ReconstructionModel::ChecksumDelta { fmt } => {
                self.apply_checksummed(delta, fmt)
            }
            ReconstructionModel::L3 { .. } => self.apply_l3(delta),
        }?;
        if let (Some((bid, _)), Some((ask, _))) =
            (self.book.best_bid(), self.book.best_ask())
            && bid >= ask
        {
            return Err(self.gap(format!(
                "book crossed after apply: best_bid {bid} >= best_ask {ask}"
            )));
        }
        Ok(output)
    }

    /// Apply an L3 (per-order) snapshot or delta. Maintains an order-id map and
    /// the per-price aggregate, then writes the changed levels into the shared
    /// L2 engine so the emit path is identical to every other model. A snapshot
    /// resets the order map; each `L3Order` upserts (or removes) one order and
    /// only its affected price level(s) are recomputed.
    fn apply_l3(&mut self, delta: NormalizedDelta) -> Result<BookOutput, ResyncNeeded> {
        let snapshot = delta.is_snapshot;
        if snapshot {
            self.l3_orders.clear();
            self.l3_levels.clear();
        }

        let mut touched: Vec<(bool, Decimal)> = Vec::new();
        let mut touch = |key: (bool, Decimal)| {
            if !touched.contains(&key) {
                touched.push(key);
            }
        };
        for o in &delta.orders {
            if let Some(old) = self.l3_orders.remove(&o.order_id) {
                let key = (old.is_ask, old.price);
                if let Some(total) = self.l3_levels.get_mut(&key) {
                    *total -= old.size;
                    if *total <= Decimal::ZERO {
                        self.l3_levels.remove(&key);
                    }
                }
                touch(key);
            }
            if o.removed {
                continue;
            }

            let (Ok(price), Ok(size)) = (
                Decimal::from_str_exact(&o.price),
                Decimal::from_str_exact(&o.size),
            ) else {
                tracing::warn!(
                    order_id = %o.order_id,
                    price = %o.price,
                    size = %o.size,
                    "l3: unparseable open order skipped after prior position removal"
                );
                continue;
            };
            if size <= Decimal::ZERO {
                continue;
            }
            let key = (o.is_ask, price);
            *self.l3_levels.entry(key).or_insert(Decimal::ZERO) += size;
            self.l3_orders.insert(
                o.order_id.clone(),
                L3Pos {
                    is_ask: o.is_ask,
                    price,
                    size,
                },
            );
            touch(key);
        }
        if self.l3_orders.len() > L3_MAX_ORDERS {
            return Err(self.gap(format!(
                "l3 order map exceeded {L3_MAX_ORDERS} entries; re-seeding"
            )));
        }

        let mut bids = Vec::new();
        let mut asks = Vec::new();
        for key in &touched {
            let total = self.l3_levels.get(key).copied().unwrap_or(Decimal::ZERO);
            let level = (key.1.to_string(), total.to_string());
            if key.0 {
                asks.push(level);
            } else {
                bids.push(level);
            }
        }
        let level_delta = NormalizedDelta {
            symbol: delta.symbol,
            bids,
            asks,
            update_id: delta.update_id,
            sequence: delta.sequence,
            source_orderbook_ts_us: delta.source_orderbook_ts_us,
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            checksum: None,
            orders: Vec::new(),
            is_snapshot: snapshot,
        };
        self.book
            .process(&level_delta)
            .map_err(|e| self.gap(e.to_string()))?;
        self.last = Some(delta.update_id);
        self.state = OrderBookState::Synced;
        Ok(if snapshot {
            BookOutput::Snapshot
        } else {
            BookOutput::Applied
        })
    }

    /// Apply a checksum-validated delta, then recompute the venue checksum over
    /// the book and gap on mismatch (a lost/misapplied delta).
    fn apply_checksummed(
        &mut self,
        delta: NormalizedDelta,
        fmt: ChecksumFmt,
    ) -> Result<BookOutput, ResyncNeeded> {
        let snapshot = delta.is_snapshot;
        let expected = delta.checksum;
        self.book
            .process(&delta)
            .map_err(|e| self.gap(e.to_string()))?;
        self.last = Some(delta.update_id);
        match expected {
            Some(expected) => {
                let computed =
                    crate::framework::checksum::book_checksum(&fmt, &self.book);
                if computed != expected {
                    return Err(self.gap(ResyncReason::Checksum(format!(
                        "checksum mismatch: computed={computed} expected={expected}"
                    ))));
                }
            }

            None if !snapshot => {
                return Err(self.gap(ResyncReason::Checksum(
                    "checksum absent on a checksum-model delta frame".into(),
                )));
            }
            None => {}
        }
        self.state = OrderBookState::Synced;
        Ok(if snapshot {
            BookOutput::Snapshot
        } else {
            BookOutput::Applied
        })
    }

    fn apply_full_refresh(
        &mut self,
        mut delta: NormalizedDelta,
    ) -> Result<BookOutput, ResyncNeeded> {
        delta.is_snapshot = true;
        self.book
            .process(&delta)
            .map_err(|e| self.gap(e.to_string()))?;
        self.last = Some(delta.update_id);
        self.state = OrderBookState::Synced;
        Ok(BookOutput::Snapshot)
    }

    fn apply_seq_delta(
        &mut self,
        delta: NormalizedDelta,
        predicate: SeqPredicate,
    ) -> Result<BookOutput, ResyncNeeded> {
        if delta.is_snapshot {
            self.book
                .process(&delta)
                .map_err(|e| self.gap(e.to_string()))?;
            self.last = Some(delta.update_id);
            self.state = OrderBookState::Synced;
            return Ok(BookOutput::Snapshot);
        }
        match self.last {
            None => return Err(self.gap("delta received before snapshot")),
            Some(last) => {
                let continuous = match predicate {
                    SeqPredicate::RangeInclusive => {
                        delta.sequence <= last + 1 && delta.update_id > last
                    }
                    SeqPredicate::ExactPrev => delta.sequence == last,

                    SeqPredicate::Monotonic => delta.update_id > last,
                };
                if !continuous {
                    return Err(self.gap(format!(
                        "seq gap: last={last} update_id={} sequence={}",
                        delta.update_id, delta.sequence
                    )));
                }
            }
        }
        self.book
            .process(&delta)
            .map_err(|e| self.gap(e.to_string()))?;
        self.last = Some(delta.update_id);
        self.state = OrderBookState::Synced;
        Ok(BookOutput::Applied)
    }

    fn gap(&mut self, reason: impl Into<ResyncReason>) -> ResyncNeeded {
        self.state = OrderBookState::Gapped;
        ResyncNeeded {
            reason: reason.into(),
            action: self.recovery,
        }
    }

    /// Force this book into `Gapped` from OUTSIDE the apply path — used when a
    /// connection-level continuity loss (`DomainEvent::ConnectionGap`) proves
    /// the state is stale without any delta being rejected. Levels and L3
    /// state are released eagerly: the book is untrustworthy the moment the
    /// gap is confirmed, and clearing here bounds memory across the reconnect
    /// that follows (a Coinbase book alone is tens of thousands of levels) —
    /// the fresh snapshot rebuilds into the empty maps rather than beside the
    /// old ones.
    pub fn force_gap(&mut self) {
        self.state = OrderBookState::Gapped;
        self.last = None;
        self.book.bids.clear();
        self.book.asks.clear();
        self.l3_orders.clear();
        self.l3_levels.clear();
    }
}

/// Reconstructs an ordered, de-duplicated public-trade log. Where the venue
/// provides a monotonic trade sequence, continuity breaks are counted as
/// permanent loss events (`gap_events`/`trades_lost`) — never a recoverable
/// state, since no re-seed path exists for historical prints. Every new
/// print is appended — no head-only drop.
pub struct SourcedTradebook {
    pair: TradingPair,
    last_seq: Option<u64>,
    state: TradeBookState,
    count: u64,
    gap_events: u64,
    trades_lost: u64,
}

impl SourcedTradebook {
    pub fn new(pair: TradingPair) -> Self {
        Self {
            pair,
            last_seq: None,
            state: TradeBookState::Empty,
            count: 0,
            gap_events: 0,
            trades_lost: 0,
        }
    }

    /// Seed the continuity pointer from a PRIOR connection's last applied
    /// sequence, so the first trade after a reconnect is checked against the
    /// pre-disconnect position — a cross-reconnect gap is then counted by the
    /// same `seq - last - 1` arithmetic instead of silently resetting. Without
    /// this, `trades_lost` only ever counted within-connection gaps.
    pub fn with_last_seq(mut self, last_seq: Option<u64>) -> Self {
        self.last_seq = last_seq;
        self
    }

    /// The last applied venue sequence (the carry the worker persists across
    /// reconnects — see [`Self::with_last_seq`]).
    pub fn last_seq(&self) -> Option<u64> {
        self.last_seq
    }

    pub fn pair(&self) -> &TradingPair {
        &self.pair
    }
    pub fn state(&self) -> TradeBookState {
        self.state
    }
    pub fn count(&self) -> u64 {
        self.count
    }
    /// Continuity breaks observed (each may cover many lost prints).
    pub fn gap_events(&self) -> u64 {
        self.gap_events
    }
    /// Total prints permanently lost, summed from sequence jumps.
    pub fn trades_lost(&self) -> u64 {
        self.trades_lost
    }
    pub fn close(&mut self) {
        self.state = TradeBookState::Closed;
    }

    /// Append one trade print. `sequence` is the venue trade-sequence if any
    /// (`None` = no continuity accounting). The caller forwards `trade` to
    /// Emit; a `GappedApplied` outcome carries the number of prints
    /// permanently lost so the caller can account for them.
    pub fn apply(&mut self, trade: Trade, sequence: Option<u64>) -> TradeApply {
        let mut outcome = TradeApply::Applied;
        if let (Some(seq), Some(last)) = (sequence, self.last_seq) {
            if seq <= last {
                return TradeApply::Duplicate;
            }
            if seq > last + 1 {
                let missed = seq - last - 1;
                self.gap_events += 1;
                self.trades_lost += missed;
                outcome = TradeApply::GappedApplied { missed };
            }
        }
        if let Some(seq) = sequence {
            self.last_seq = Some(seq);
        }
        self.count += 1;
        self.state = TradeBookState::Synced;
        let _ = trade;
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aetelier_types::orderbooks::L3Order;

    #[test]
    fn seeding_taxonomy_derives_from_snapshot_source() {
        let rest = ReconstructionModel::SeqDelta {
            predicate: SeqPredicate::RangeInclusive,
            source: SnapshotSource::RestSnapshot,
        };
        assert!(rest.needs_rest());
        assert!(matches!(
            rest.recovery_action(),
            RecoveryAction::RestSnapshot
        ));

        let self_seed = ReconstructionModel::SeqDelta {
            predicate: SeqPredicate::Monotonic,
            source: SnapshotSource::WssSelfSeed,
        };
        assert!(!self_seed.needs_rest());
        assert!(matches!(
            self_seed.recovery_action(),
            RecoveryAction::Resubscribe
        ));

        let in_band = ReconstructionModel::SeqDelta {
            predicate: SeqPredicate::ExactPrev,
            source: SnapshotSource::ReqOnSocket,
        };
        assert!(!in_band.needs_rest());
        assert!(matches!(
            in_band.recovery_action(),
            RecoveryAction::Resubscribe
        ));

        let l3 = ReconstructionModel::L3 {
            source: SnapshotSource::RestSnapshot,
        };
        assert!(l3.needs_rest());

        assert_eq!(ReconstructionModel::FullRefresh.snapshot_source(), None);
        assert_eq!(
            ReconstructionModel::ChecksumDelta {
                fmt: ChecksumFmt::OkxTop25
            }
            .snapshot_source(),
            None
        );
    }

    #[test]
    fn out_of_band_seeding_splits_seeder_from_buffering() {
        for source in [SnapshotSource::RestSnapshot, SnapshotSource::ReqOnSocket] {
            let model = ReconstructionModel::SeqDelta {
                predicate: SeqPredicate::ExactPrev,
                source,
            };
            assert!(model.seeds_out_of_band(), "{source:?} races its deltas");
        }
        let self_seed = ReconstructionModel::SeqDelta {
            predicate: SeqPredicate::RangeInclusive,
            source: SnapshotSource::WssSelfSeed,
        };
        assert!(!self_seed.seeds_out_of_band());
        assert!(!ReconstructionModel::FullRefresh.seeds_out_of_band());
        assert!(
            !ReconstructionModel::ChecksumDelta {
                fmt: ChecksumFmt::OkxTop25
            }
            .seeds_out_of_band()
        );
        assert!(
            ReconstructionModel::L3 {
                source: SnapshotSource::RestSnapshot
            }
            .seeds_out_of_band()
        );
        let rest = ReconstructionModel::SeqDelta {
            predicate: SeqPredicate::RangeInclusive,
            source: SnapshotSource::RestSnapshot,
        };
        assert!(rest.needs_rest() && rest.seeds_out_of_band());
        let in_band = ReconstructionModel::SeqDelta {
            predicate: SeqPredicate::ExactPrev,
            source: SnapshotSource::ReqOnSocket,
        };
        assert!(!in_band.needs_rest() && in_band.seeds_out_of_band());
    }

    #[test]
    fn capability_matrix_encodes_the_venue_traps() {
        let coinbase = ReconstructionModel::SeqDelta {
            predicate: SeqPredicate::Monotonic,
            source: SnapshotSource::WssSelfSeed,
        };
        assert!(coinbase.seq_is_connection_scoped());
        assert!(
            !coinbase.replay_dedup_sound(),
            "connection-wide counters must never gate buffered replay by update_id"
        );
        assert!(!coinbase.supports_in_band_reseed());

        let htx = ReconstructionModel::SeqDelta {
            predicate: SeqPredicate::ExactPrev,
            source: SnapshotSource::ReqOnSocket,
        };
        assert!(!htx.seq_is_connection_scoped());
        assert!(htx.replay_dedup_sound());
        assert!(htx.supports_in_band_reseed());

        let binance = ReconstructionModel::SeqDelta {
            predicate: SeqPredicate::RangeInclusive,
            source: SnapshotSource::RestSnapshot,
        };
        assert!(binance.replay_dedup_sound());
        assert!(!binance.supports_in_band_reseed());

        let l3 = ReconstructionModel::L3 {
            source: SnapshotSource::RestSnapshot,
        };
        assert!(l3.replay_dedup_sound());

        for seedless in [
            ReconstructionModel::FullRefresh,
            ReconstructionModel::ChecksumDelta {
                fmt: ChecksumFmt::OkxTop25,
            },
        ] {
            assert!(!seedless.replay_dedup_sound());
            assert!(!seedless.supports_in_band_reseed());
            assert!(!seedless.seq_is_connection_scoped());
        }
    }

    #[test]
    fn crossed_book_after_apply_raises_resync_for_every_class() {
        let model = ReconstructionModel::FullRefresh;
        let mut book = SourcedOrderbook::new(
            TradingPair::new("BTC", "USD"),
            model.clone(),
            model.recovery_action(),
        );
        let crossed = delta("BTCUSD", vec![("101", "1")], vec![("100", "1")], 1, 1, true);
        let out = book.apply(crossed);
        assert!(out.is_err(), "a crossed book must never be emitted");
        assert_eq!(book.state(), OrderBookState::Gapped);
        let sane = delta("BTCUSD", vec![("99", "1")], vec![("100", "1")], 2, 2, true);
        assert!(
            book.apply(sane).is_ok(),
            "a sane snapshot recovers the book"
        );
        assert_eq!(book.state(), OrderBookState::Synced);
    }

    #[test]
    fn l3_stale_top_bid_is_self_healed_not_emitted() {
        let model = ReconstructionModel::L3 {
            source: SnapshotSource::RestSnapshot,
        };
        let mut book = SourcedOrderbook::new(
            TradingPair::new("BTC", "MXN"),
            model.clone(),
            model.recovery_action(),
        );
        let mut seed = delta("btc_mxn", vec![], vec![], 1, 1, true);
        seed.orders = vec![
            L3Order {
                order_id: "b1".into(),
                is_ask: false,
                price: "1105860".into(),
                size: "1".into(),
                removed: false,
            },
            L3Order {
                order_id: "a1".into(),
                is_ask: true,
                price: "1106000".into(),
                size: "1".into(),
                removed: false,
            },
        ];
        assert!(book.apply(seed).is_ok());
        let mut cross = delta("btc_mxn", vec![], vec![], 2, 2, false);
        cross.orders = vec![L3Order {
            order_id: "a2".into(),
            is_ask: true,
            price: "1105290".into(),
            size: "1".into(),
            removed: false,
        }];
        let out = book.apply(cross);
        assert!(
            out.is_err(),
            "an ask arriving under a stale bid must trigger resync, not emit crossed"
        );
        assert_eq!(book.state(), OrderBookState::Gapped);
    }

    #[test]
    fn reconstruction_classes_carry_stable_labels() {
        assert_eq!(
            ReconstructionModel::FullRefresh.class_label(),
            "full_refresh"
        );
        assert_eq!(
            ReconstructionModel::SeqDelta {
                predicate: SeqPredicate::Monotonic,
                source: SnapshotSource::WssSelfSeed,
            }
            .class_label(),
            "seq_delta"
        );
        assert_eq!(
            ReconstructionModel::ChecksumDelta {
                fmt: ChecksumFmt::KrakenTop10
            }
            .class_label(),
            "checksum_delta"
        );
        let l3 = ReconstructionModel::L3 {
            source: SnapshotSource::RestSnapshot,
        };
        assert_eq!(l3.class_label(), "l3");
        assert_eq!(l3.book_level(), "L3");
        assert_eq!(ReconstructionModel::FullRefresh.book_level(), "L2");
    }

    #[test]
    fn checksum_reason_is_structurally_distinguishable() {
        let c = ResyncReason::Checksum("computed=1 expected=2".into());
        assert!(c.is_checksum());
        assert_eq!(c.to_string(), "computed=1 expected=2");
        let o: ResyncReason = "seq gap: last=1".into();
        assert!(!o.is_checksum());
        assert_eq!(o.to_string(), "seq gap: last=1");
    }

    fn delta(
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

    #[test]
    fn config_depth_arms_non_checksum_books_but_spares_checksum_venues() {
        let seq = ReconstructionModel::SeqDelta {
            predicate: SeqPredicate::Monotonic,
            source: SnapshotSource::WssSelfSeed,
        };
        let mut book = SourcedOrderbook::new(
            TradingPair::new("BTC", "USD"),
            seq.clone(),
            seq.recovery_action(),
        );
        assert_eq!(book.book().max_depth, None, "unarmed book keeps full depth");
        book.arm_config_depth(Some(5));
        assert_eq!(
            book.book().max_depth,
            Some(5),
            "config depth prunes a non-checksum book"
        );

        let kraken = ReconstructionModel::ChecksumDelta {
            fmt: ChecksumFmt::KrakenTop10,
        };
        let mut kbook = SourcedOrderbook::new(
            TradingPair::new("BTC", "USD"),
            kraken.clone(),
            kraken.recovery_action(),
        );
        assert_eq!(kbook.book().max_depth, Some(10));
        kbook.arm_config_depth(Some(5));
        assert_eq!(
            kbook.book().max_depth,
            Some(10),
            "checksum venue keeps its recipe depth"
        );

        let okx = ReconstructionModel::ChecksumDelta {
            fmt: ChecksumFmt::OkxTop25,
        };
        let mut obook = SourcedOrderbook::new(
            TradingPair::new("BTC", "USD"),
            okx.clone(),
            okx.recovery_action(),
        );
        assert_eq!(obook.book().max_depth, None);
        obook.arm_config_depth(Some(5));
        assert_eq!(
            obook.book().max_depth,
            None,
            "checksum venue stays a full book"
        );
    }

    #[test]
    fn force_gap_marks_gapped_and_releases_levels_eagerly() {
        let model = ReconstructionModel::SeqDelta {
            predicate: SeqPredicate::Monotonic,
            source: SnapshotSource::WssSelfSeed,
        };
        let mut book = SourcedOrderbook::new(
            TradingPair::new("BTC", "USD"),
            model.clone(),
            model.recovery_action(),
        );
        book.apply(delta(
            "BTC/USD",
            vec![("100", "1"), ("99", "2")],
            vec![("101", "1"), ("102", "2")],
            1,
            1,
            true,
        ))
        .unwrap();
        assert_eq!(book.state(), OrderBookState::Synced);
        assert!(book.book().bid_depth() > 0 && book.book().ask_depth() > 0);

        book.force_gap();
        assert_eq!(book.state(), OrderBookState::Gapped);
        assert_eq!(book.book().bid_depth(), 0, "bids released eagerly");
        assert_eq!(book.book().ask_depth(), 0, "asks released eagerly");

        let err = book.apply(delta(
            "BTC/USD",
            vec![("100", "1")],
            vec![("101", "1")],
            2,
            2,
            false,
        ));
        assert!(err.is_err() || book.state() != OrderBookState::Synced);
    }

    #[test]
    fn epoch_to_us_normalizes_any_unit_to_us() {
        let us = 1_700_000_000_000_000u64;
        assert_eq!(epoch_to_us(us), us, "us passes through");
        assert_eq!(epoch_to_us(1_700_000_000), us, "seconds → us");
        assert_eq!(epoch_to_us(1_700_000_000_000), us, "milliseconds → us");
        assert_eq!(
            epoch_to_us(1_700_000_000_000_000_000),
            us,
            "nanoseconds → us"
        );
        assert_eq!(epoch_to_us(0), 0, "zero stays zero");
    }

    fn checksummed(
        symbol: &str,
        bids: Vec<(&str, &str)>,
        asks: Vec<(&str, &str)>,
        update_id: u64,
        is_snapshot: bool,
        checksum: Option<i64>,
    ) -> NormalizedDelta {
        let mut d = delta(symbol, bids, asks, update_id, 0, is_snapshot);
        d.checksum = checksum;
        d
    }

    #[test]
    fn checksum_delta_frame_without_checksum_fails_closed() {
        let mut b = SourcedOrderbook::new(
            TradingPair::new("BTC", "USDT"),
            ReconstructionModel::ChecksumDelta {
                fmt: ChecksumFmt::OkxTop25,
            },
            RecoveryAction::Resubscribe,
        );

        b.apply(checksummed(
            "BTCUSDT",
            vec![("100", "1")],
            vec![("101", "1")],
            1,
            true,
            None,
        ))
        .unwrap();
        assert_eq!(b.state(), OrderBookState::Synced);

        let err = b
            .apply(checksummed(
                "BTCUSDT",
                vec![("100", "2")],
                vec![],
                2,
                false,
                None,
            ))
            .unwrap_err();
        assert!(
            err.reason.is_checksum(),
            "absence is a checksum-class fault"
        );
        assert_eq!(b.state(), OrderBookState::Gapped);
    }

    #[test]
    fn full_refresh_replaces_each_frame_and_syncs() {
        let mut b = SourcedOrderbook::new(
            TradingPair::new("BTC", "USDT"),
            ReconstructionModel::FullRefresh,
            RecoveryAction::Resubscribe,
        );
        b.apply(delta(
            "BTCUSDT",
            vec![("100", "1")],
            vec![("101", "1")],
            1,
            0,
            false,
        ))
        .unwrap();
        b.apply(delta(
            "BTCUSDT",
            vec![("99", "2")],
            vec![("102", "2")],
            2,
            0,
            false,
        ))
        .unwrap();
        assert_eq!(b.book().bid_depth(), 1);
        assert_eq!(b.book().ask_depth(), 1);
        assert_eq!(b.state(), OrderBookState::Synced);
    }

    #[test]
    fn seq_delta_applies_in_order_then_gaps() {
        let mut b = SourcedOrderbook::new(
            TradingPair::new("BTC", "USDT"),
            ReconstructionModel::SeqDelta {
                predicate: SeqPredicate::RangeInclusive,
                source: SnapshotSource::RestSnapshot,
            },
            RecoveryAction::RestSnapshot,
        );
        b.apply(delta(
            "BTCUSDT",
            vec![("100", "1")],
            vec![("101", "1")],
            10,
            0,
            true,
        ))
        .unwrap();
        assert_eq!(b.state(), OrderBookState::Synced);
        assert_eq!(
            b.apply(delta("BTCUSDT", vec![("100", "2")], vec![], 11, 0, false))
                .unwrap(),
            BookOutput::Applied
        );

        assert!(
            b.apply(delta("BTCUSDT", vec![("100", "3")], vec![], 11, 0, false))
                .is_err()
        );
        assert_eq!(b.state(), OrderBookState::Gapped);
    }

    #[test]
    fn seq_delta_range_inclusive_detects_dropped_frame() {
        let mut b = SourcedOrderbook::new(
            TradingPair::new("BTC", "USDT"),
            ReconstructionModel::SeqDelta {
                predicate: SeqPredicate::RangeInclusive,
                source: SnapshotSource::RestSnapshot,
            },
            RecoveryAction::RestSnapshot,
        );

        b.apply(delta(
            "BTCUSDT",
            vec![("100", "1")],
            vec![("101", "1")],
            100,
            0,
            true,
        ))
        .unwrap();

        b.apply(delta(
            "BTCUSDT",
            vec![("100", "2")],
            vec![],
            105,
            101,
            false,
        ))
        .unwrap();
        assert_eq!(b.state(), OrderBookState::Synced);

        assert!(
            b.apply(delta(
                "BTCUSDT",
                vec![("100", "3")],
                vec![],
                115,
                110,
                false
            ))
            .is_err()
        );
        assert_eq!(b.state(), OrderBookState::Gapped);
    }

    #[test]
    fn seq_delta_exact_prev_follows_sequence_pointer() {
        let mut b = SourcedOrderbook::new(
            TradingPair::new("BTC", "USDT"),
            ReconstructionModel::SeqDelta {
                predicate: SeqPredicate::ExactPrev,
                source: SnapshotSource::ReqOnSocket,
            },
            RecoveryAction::ReqOnSocket,
        );

        b.apply(delta(
            "BTCUSDT",
            vec![("100", "1")],
            vec![("101", "1")],
            100,
            0,
            true,
        ))
        .unwrap();

        b.apply(delta(
            "BTCUSDT",
            vec![("100", "2")],
            vec![],
            101,
            100,
            false,
        ))
        .unwrap();
        assert_eq!(b.state(), OrderBookState::Synced);

        assert!(
            b.apply(delta(
                "BTCUSDT",
                vec![("100", "3")],
                vec![],
                103,
                102,
                false
            ))
            .is_err()
        );
        assert_eq!(b.state(), OrderBookState::Gapped);
    }

    #[test]
    fn seq_delta_monotonic_allows_connection_counter_jumps() {
        let mut b = SourcedOrderbook::new(
            TradingPair::new("BTC", "USD"),
            ReconstructionModel::SeqDelta {
                predicate: SeqPredicate::Monotonic,
                source: SnapshotSource::WssSelfSeed,
            },
            RecoveryAction::Resubscribe,
        );

        b.apply(delta(
            "BTCUSD",
            vec![("100", "1")],
            vec![("101", "1")],
            1,
            1,
            true,
        ))
        .unwrap();

        b.apply(delta("BTCUSD", vec![("100", "2")], vec![], 6, 6, false))
            .unwrap();
        assert_eq!(b.state(), OrderBookState::Synced);

        assert!(
            b.apply(delta("BTCUSD", vec![("100", "3")], vec![], 6, 6, false))
                .is_err()
        );
        assert_eq!(b.state(), OrderBookState::Gapped);
    }

    #[test]
    fn l3_aggregates_multiple_orders_per_price_level() {
        use aetelier_types::orderbooks::L3Order;
        let l3 =
            |id: &str, is_ask: bool, price: &str, size: &str, removed: bool| L3Order {
                order_id: id.into(),
                is_ask,
                price: price.into(),
                size: size.into(),
                removed,
            };
        let l3_delta =
            |update_id: u64, is_snapshot: bool, orders: Vec<L3Order>| NormalizedDelta {
                symbol: "BTC/MXN".into(),
                bids: vec![],
                asks: vec![],
                update_id,
                sequence: 0,
                source_orderbook_ts_us: 0,
                local_orderbook_ts_us: 0,
                source_orderbook_rtt_us: 0,
                checksum: None,
                orders,
                is_snapshot,
            };
        let price: Decimal = "1000000".parse().unwrap();
        let mut b = SourcedOrderbook::new(
            TradingPair::new("BTC", "MXN"),
            ReconstructionModel::L3 {
                source: SnapshotSource::RestSnapshot,
            },
            RecoveryAction::RestSnapshot,
        );

        b.apply(l3_delta(
            1,
            true,
            vec![
                l3("o1", false, "1000000", "1.0", false),
                l3("o2", false, "1000000", "2.0", false),
                l3("o3", true, "1010000", "0.5", false),
            ],
        ))
        .unwrap();
        assert_eq!(b.book().bids.get(&price), Some(&"3.0".parse().unwrap()));
        assert_eq!(
            b.book().asks.get(&"1010000".parse().unwrap()),
            Some(&"0.5".parse().unwrap())
        );

        b.apply(l3_delta(2, false, vec![l3("o1", false, "", "", true)]))
            .unwrap();
        assert_eq!(b.book().bids.get(&price), Some(&"2.0".parse().unwrap()));

        b.apply(l3_delta(
            3,
            false,
            vec![l3("o4", false, "1000000", "0.5", false)],
        ))
        .unwrap();
        assert_eq!(b.book().bids.get(&price), Some(&"2.5".parse().unwrap()));

        b.apply(l3_delta(
            4,
            false,
            vec![l3("o2", false, "", "", true), l3("o4", false, "", "", true)],
        ))
        .unwrap();
        assert!(!b.book().bids.contains_key(&price));
        assert_eq!(b.state(), OrderBookState::Synced);
    }

    #[test]
    fn stamp_local_carries_conn_epoch_us_into_derivative_rows() {
        use aetelier_types::funding::FundingRate;
        use aetelier_types::open_interest::OpenInterest;
        use aetelier_types::trading_pair::TradingPair;

        let mut funding = DomainEvent::FundingRate(FundingRate {
            funding_rate_ts_us: 1,
            local_funding_ts_us: 0,
            recv_seq: 0,
            conn_epoch_us: 0,
            pair: TradingPair::new("BTC", "USDC"),
            funding_rate: "0.0001".parse().unwrap(),
            premium: None,
            interval_hours: 1,
            next_funding_ts_us: 0,
            exchange: "hyperliquid".to_string(),
        });
        funding.stamp_local(42, 7, 3, 1_788_375_005_000_000);
        match &funding {
            DomainEvent::FundingRate(fr) => {
                assert_eq!(fr.conn_epoch_us, 1_788_375_005_000_000);
                assert_eq!(fr.recv_seq, 3);
                assert_eq!(fr.local_funding_ts_us, 42);
            }
            other => panic!("wrong variant: {other:?}"),
        }

        let mut oi = DomainEvent::OpenInterest(OpenInterest {
            open_interest_ts_us: 1,
            local_oi_ts_us: 0,
            recv_seq: 0,
            conn_epoch_us: 0,
            pair: TradingPair::new("BTC", "USDC"),
            open_interest: "10".parse().unwrap(),
            open_interest_value: None,
            mark_px: None,
            exchange: "hyperliquid".to_string(),
        });
        oi.stamp_local(43, 7, 4, 1_788_375_198_000_000);
        match &oi {
            DomainEvent::OpenInterest(o) => {
                assert_eq!(o.conn_epoch_us, 1_788_375_198_000_000);
                assert_eq!(o.recv_seq, 4);
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn stamp_local_sets_only_the_matching_variant_fields() {
        let mut book = DomainEvent::Book(delta(
            "BTCUSDT",
            vec![("100", "1")],
            vec![("101", "1")],
            7,
            0,
            false,
        ));
        book.stamp_local(1_234_567_890, 250, 1, 7);
        match &book {
            DomainEvent::Book(d) => {
                assert_eq!(d.local_orderbook_ts_us, 1_234_567_890);
                assert_eq!(d.source_orderbook_rtt_us, 250);

                assert_eq!(d.source_orderbook_ts_us, 0);
            }
            other => panic!("expected Book variant, got {other:?}"),
        }

        let mut tr = Trade::random();
        tr.source_trade_ts_us = 1_700_000_000_000;
        tr.local_trade_ts_us = 0;
        tr.source_trade_rtt_us = 0;
        let mut trade = DomainEvent::Trade {
            trade: tr,
            sequence: Some(42),
        };
        trade.stamp_local(9_876_543_210, 333, 2, 7);
        match &trade {
            DomainEvent::Trade { trade, sequence } => {
                assert_eq!(trade.local_trade_ts_us, 9_876_543_210);
                assert_eq!(trade.source_trade_rtt_us, 333);

                assert_eq!(trade.source_trade_ts_us, 1_700_000_000_000);
                assert_eq!(*sequence, Some(42));
            }
            other => panic!("expected Trade variant, got {other:?}"),
        }
    }

    #[test]
    fn trade_book_appends_and_gaps_on_sequence() {
        let mut t = SourcedTradebook::new(TradingPair::new("BTC", "USDT"));
        let tr = Trade::random();
        assert_eq!(t.apply(tr.clone(), Some(1)), TradeApply::Applied);
        assert_eq!(t.apply(tr.clone(), Some(2)), TradeApply::Applied);
        assert_eq!(t.count(), 2);
        assert_eq!(t.state(), TradeBookState::Synced);
        assert_eq!(
            t.apply(tr.clone(), Some(5)),
            TradeApply::GappedApplied { missed: 2 }
        );
        assert_eq!(t.state(), TradeBookState::Synced);
        assert_eq!(t.count(), 3);
        assert_eq!(t.gap_events(), 1);
        assert_eq!(t.trades_lost(), 2);
        assert_eq!(t.apply(tr.clone(), Some(5)), TradeApply::Duplicate);
        assert_eq!(t.count(), 3);
        assert_eq!(t.apply(tr.clone(), Some(6)), TradeApply::Applied);
        assert_eq!(t.gap_events(), 1);
        assert_eq!(t.trades_lost(), 2);
    }
}
