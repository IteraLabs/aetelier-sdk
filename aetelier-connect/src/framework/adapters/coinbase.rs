//! Coinbase Advanced Trade public market-data adapter: subscribes to `level2`
//! and `market_trades` channels and normalizes them into the framework's
//! `DomainEvent` stream. The book self-seeds from the first WSS frame
//! (`type:"snapshot"`) with no REST, then validates `update`s by monotonic
//! `sequence_num`.

use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::protocol::Message;

use crate::clients::disconnect::DisconnectReason;

use aetelier_types::orderbooks::NormalizedDelta;
use aetelier_types::trades::{Trade, TradeSide};

use crate::framework::budget::{ConnectionBudget, SourceMetrics};
use crate::framework::driver::{DEFAULT_RAW_BUFFER, drive};
use crate::framework::model::{
    DomainEvent, Normalizer, ReconstructionModel, SeqPredicate, SnapshotSource,
};
use crate::framework::protocol::DeclaredSet;
use crate::framework::protocol::{Heartbeat, ProtocolHooks};
use crate::framework::registry::{ExchangeAdapter, ExchangeProfile, TaskExit};
use crate::framework::symbol::SymbolCodec;
use crate::sources::coinbase::decoder::CoinbaseDecoder;
use crate::sources::coinbase::events::CoinbaseWssEvent;
use crate::sources::coinbase::responses::orderbooks::{
    CoinbaseL2Event, CoinbaseOrderbookResponse,
};
use crate::sources::coinbase::responses::trades::CoinbaseTradeData;

/// Coinbase Advanced Trade public (no-auth) market-data endpoint. Frames arrive
/// bare with a `"channel"` field, matching `CoinbaseDecoder`'s dispatch.
const COINBASE_WSS_URL: &str = "wss://advanced-trade-ws.coinbase.com";

/// Incremental L2 channel name requested in the subscribe frame. The replies
/// arrive on the `"l2_data"` channel (snapshot then updates).
const LEVEL2_CHANNEL: &str = "level2";

/// Public trade channel — `market_trades` both on subscribe and on the wire.
const MARKET_TRADES_CHANNEL: &str = "market_trades";

/// Server heartbeat channel — one frame per second carrying its own
/// `heartbeat_counter` beside the connection-wide `sequence_num`. Subscribed
/// on the level2 socket as the contiguity anchor: between two received
/// heartbeats, `Δsequence_num − Δheartbeat_counter − data frames seen` is the
/// number of dropped level2 messages.
const HEARTBEATS_CHANNEL: &str = "heartbeats";

/// Wire symbol codec — `BTC-USD`.
const COINBASE_CODEC: SymbolCodec = SymbolCodec::Hyphen;

// ─────────────────────────────────────────────────────────────────────────
// ProtocolHooks
// ─────────────────────────────────────────────────────────────────────────

/// Which channel set one Coinbase socket subscribes.
///
/// Production runs TWO sockets per worker: the book socket must carry only
/// `level2` + `heartbeats` so its `sequence_num` arithmetic is attributable —
/// on a mixed socket every trade frame advances the counter and a dropped
/// message cannot be classified. Trades ride a second socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoinbaseChannels {
    /// `level2` + `heartbeats` — the sequence-tracked book socket.
    Level2WithHeartbeats,
    /// `market_trades` only — the trade socket.
    Trades,
}

/// Coinbase Advanced Trade WSS protocol behaviour for ONE socket's channel
/// set. Overrides only `endpoint`/`subscribe_frames`; `prepare` stays the
/// public-open no-op default and `heartbeat` stays the passive `None` default
/// (the subscribed `heartbeats` channel is server-paced).
pub struct CoinbaseHooks {
    channels: CoinbaseChannels,
}

impl CoinbaseHooks {
    /// The book socket: `level2` + `heartbeats`.
    pub fn level2() -> Self {
        Self {
            channels: CoinbaseChannels::Level2WithHeartbeats,
        }
    }

    /// The trade socket: `market_trades` only.
    pub fn trades() -> Self {
        Self {
            channels: CoinbaseChannels::Trades,
        }
    }
}

impl ProtocolHooks for CoinbaseHooks {
    fn endpoint(&self) -> String {
        COINBASE_WSS_URL.to_string()
    }

    /// One frame per channel, each carrying the full `product_ids` array.
    /// `symbols` are venue wire symbols (`"BTC-USD"`, Hyphen codec). Advanced
    /// Trade rejects a single frame that mixes channels, so every channel is
    /// subscribed with its own frame.
    fn subscribe_frames(
        &self,
        symbols: &[String],
        _declared: &DeclaredSet,
    ) -> Vec<Message> {
        let sub = |channel: &str| {
            let frame = serde_json::json!({
                "type": "subscribe",
                "product_ids": symbols,
                "channel": channel,
            });
            Message::Text(frame.to_string().into())
        };
        match self.channels {
            CoinbaseChannels::Level2WithHeartbeats => {
                vec![sub(LEVEL2_CHANNEL), sub(HEARTBEATS_CHANNEL)]
            }
            CoinbaseChannels::Trades => vec![sub(MARKET_TRADES_CHANNEL)],
        }
    }

    /// Passive: no client-initiated keep-alive. The book socket's liveness
    /// rides the subscribed server `heartbeats` channel (1/s); the trade
    /// socket is server-paced by the prints themselves.
    fn heartbeat(&self) -> Heartbeat {
        Heartbeat::None
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Sequence-continuity tracker (the book socket's independent gap oracle)
// ─────────────────────────────────────────────────────────────────────────

/// Debounce floor before a suspected gap is confirmed: give the stream a
/// beat to explain the missing slot (a dropped heartbeat is harmless; an
/// adjacent heartbeat frame reconciles it) before forcing a resync.
const GAP_DEBOUNCE_FLOOR: Duration = Duration::from_millis(10);
/// Uniform jitter added to the floor (2..=20 ms), so a fleet of collectors
/// that all see the same blip does not resubscribe in lockstep.
const GAP_DEBOUNCE_JITTER_MS: std::ops::RangeInclusive<u64> = 2..=20;

/// Connection-level `sequence_num` continuity for the level2 socket.
///
/// Coinbase's counter is connection-wide (DAT-OB-INV-10): the per-book
/// `Monotonic` predicate cannot see a dropped message. On a socket carrying
/// ONLY `level2` + `heartbeats`, every missing counter slot is either a
/// dropped heartbeat (harmless — book state rides only `l2_data`) or a
/// dropped book message (fatal — the book is silently stale). The tracker
/// counts missing slots per frame, subtracts the ones the `heartbeat_counter`
/// proves were heartbeats, and confirms the remainder as a real gap only
/// after the debounce deadline passes unexplained.
///
/// Emission is once per connection: a confirmed gap tears the connection down
/// (resync), so this cannot log-storm or resync-storm by construction — the
/// worker's jittered reconnect backoff paces retries.
struct SeqTracker {
    /// Last `sequence_num` seen on this socket (any frame kind).
    last_seq: Option<u64>,
    /// Last `heartbeat_counter` seen.
    last_hb: Option<u64>,
    /// Missing counter slots not yet attributed to dropped heartbeats.
    unattributed: u64,
    /// Debounce deadline armed when `unattributed` first becomes positive;
    /// evaluated on subsequent frames (a fully-silent socket is the staleness
    /// monitor's problem, not the tracker's).
    deadline: Option<tokio::time::Instant>,
}

impl SeqTracker {
    fn new() -> Self {
        Self {
            last_seq: None,
            last_hb: None,
            unattributed: 0,
            deadline: None,
        }
    }

    /// Observe one sequenced frame. `hb_counter` is `Some` for heartbeat
    /// frames. Returns the number of provably-dropped data messages once a
    /// gap is CONFIRMED (deadline passed, still unexplained) — `None` while
    /// clean, pending, or reconciled.
    fn observe(
        &mut self,
        seq: u64,
        hb_counter: Option<u64>,
        now: tokio::time::Instant,
    ) -> Option<u64> {
        if let Some(prev) = self.last_seq {
            let jump = seq.saturating_sub(prev.saturating_add(1));
            if jump > 0 {
                self.unattributed = self.unattributed.saturating_add(jump);
                if self.deadline.is_none() {
                    let jitter_ms = rand::rng().random_range(GAP_DEBOUNCE_JITTER_MS);
                    self.deadline =
                        Some(now + GAP_DEBOUNCE_FLOOR + Duration::from_millis(jitter_ms));
                    tracing::debug!(
                        expected = prev + 1,
                        got = seq,
                        missing = jump,
                        "coinbase.seq_gap_suspected_debouncing"
                    );
                }
            }
        }
        self.last_seq = Some(seq);

        if let Some(hb) = hb_counter {
            if let Some(prev_hb) = self.last_hb {
                // Missing slots explained as dropped heartbeats are harmless:
                // subtract them; if everything is explained, stand down.
                let hb_jump = hb.saturating_sub(prev_hb.saturating_add(1));
                if hb_jump > 0 {
                    self.unattributed = self.unattributed.saturating_sub(hb_jump);
                    tracing::debug!(
                        dropped_heartbeats = hb_jump,
                        still_unattributed = self.unattributed,
                        "coinbase.seq_gap_attributed_to_heartbeats"
                    );
                }
            }
            self.last_hb = Some(hb);
            if self.unattributed == 0 {
                self.deadline = None;
            }
        }

        match self.deadline {
            Some(d) if now >= d && self.unattributed > 0 => {
                let dropped = self.unattributed;
                self.unattributed = 0;
                self.deadline = None;
                Some(dropped)
            }
            _ => None,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Normalizer — decoded CoinbaseWssEvent → DomainEvent
// ─────────────────────────────────────────────────────────────────────────

/// Maps a decoded [`CoinbaseWssEvent`] to `DomainEvent`s. Derives the canonical
/// pair from the event's venue symbol (the socket is multi-symbol). Holds the
/// worker's shared [`SourceMetrics`] so every dropped event is counted, not
/// just logged.
///
/// With a tracker (the book socket), every sequenced frame feeds the
/// connection-continuity arithmetic and a confirmed gap is emitted as
/// [`DomainEvent::ConnectionGap`] ahead of the frame's own events. The trade
/// socket runs `passthrough` (no tracker): a trade gap is loss-accounting,
/// not book corruption.
#[derive(Default)]
pub struct CoinbaseNormalizer {
    pub metrics: SourceMetrics,
    /// `Some` on the level2 socket only. `Mutex` because `normalize` takes
    /// `&self`; the driver is the sole caller, so it is uncontended.
    tracker: Option<std::sync::Mutex<SeqTracker>>,
}

impl CoinbaseNormalizer {
    /// The book-socket normalizer: connection-continuity tracking armed.
    pub fn sequence_tracked(metrics: SourceMetrics) -> Self {
        Self {
            metrics,
            tracker: Some(std::sync::Mutex::new(SeqTracker::new())),
        }
    }

    /// The trade-socket normalizer: no tracker.
    pub fn passthrough(metrics: SourceMetrics) -> Self {
        Self {
            metrics,
            tracker: None,
        }
    }

    /// Feed one sequenced frame to the tracker; a confirmed gap comes back as
    /// the `ConnectionGap` event to emit ahead of the frame's own events.
    fn track(&self, seq: u64, hb_counter: Option<u64>) -> Option<DomainEvent> {
        let tracker = self.tracker.as_ref()?;
        let confirmed =
            tracker
                .lock()
                .ok()?
                .observe(seq, hb_counter, tokio::time::Instant::now())?;
        tracing::warn!(
            dropped = confirmed,
            "coinbase.connection_gap_confirmed_resyncing"
        );
        Some(DomainEvent::ConnectionGap { dropped: confirmed })
    }
}

impl CoinbaseNormalizer {
    /// Build a `NormalizedDelta` from one `l2_data` event. `update_id` and
    /// `sequence` both carry the connection-wide `sequence_num`; `Monotonic`
    /// continuity is `update_id > last` (the counter is bumped by every message
    /// on the socket, so it is not a per-book contiguous id). `is_snapshot`
    /// selects the seed vs delta path.
    fn to_delta(seq: u64, event_ts: u64, event: &CoinbaseL2Event) -> NormalizedDelta {
        let mut bids = Vec::new();
        let mut asks = Vec::new();
        for update in &event.updates {
            let level = (update.price_level.clone(), update.new_quantity.clone());
            match update.side.as_str() {
                "bid" => bids.push(level),
                "offer" | "ask" => asks.push(level),
                _ => {}
            }
        }
        NormalizedDelta {
            symbol: event.product_id.clone(),
            bids,
            asks,
            update_id: seq,
            sequence: seq,
            source_orderbook_ts_us: event_ts,
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            checksum: None,
            orders: Vec::new(),
            is_snapshot: event.ty == "snapshot",
        }
    }
}

impl Normalizer for CoinbaseNormalizer {
    type Event = CoinbaseWssEvent;

    fn normalize(&self, event: CoinbaseWssEvent) -> Vec<DomainEvent> {
        match event {
            // `l2_data` may carry multiple events (multi-product socket); emit a
            // Book delta per event rather than only the first (no head-drop).
            // A gap the tracker confirms on this frame's slot precedes the
            // frame's own deltas: the loss happened BEFORE this frame, so the
            // books must resync before consuming anything newer.
            CoinbaseWssEvent::OrderbookData(resp) => {
                let mut evs = Vec::new();
                if let Some(gap) = self.track(resp.sequence_num, None) {
                    evs.push(gap);
                }
                evs.extend(normalize_book(resp));
                evs
            }
            // `market_trades` may batch many trades; emit one Trade per trade
            // rather than only the first (no head-drop). Unknown-side trades are
            // dropped by `normalize_trade`. (The trade socket runs no tracker.)
            // The wire batches trades in DESCENDING trade_id order (verified on
            // live BTC-USD captures, cycles #3 and #7) — reverse the batch so
            // the emitted stream is ascending and the armed sequence feeds the
            // SourcedTradebook loss accounting.
            CoinbaseWssEvent::TradeData(trades) => trades
                .into_iter()
                .rev()
                .filter_map(|t| normalize_trade(t, &self.metrics))
                .collect(),
            // The contiguity anchor: the heartbeat's counter delta explains
            // dropped-heartbeat slots; whatever remains unexplained past the
            // debounce is a confirmed data gap.
            CoinbaseWssEvent::Heartbeat {
                sequence_num,
                heartbeat_counter,
            } => self
                .track(sequence_num, Some(heartbeat_counter))
                .into_iter()
                .collect(),
            // Sequenced non-data frames (subscription ack, undecodable
            // payloads): account the slot so it never reads as a gap.
            CoinbaseWssEvent::Control { sequence_num } => {
                self.track(sequence_num, None).into_iter().collect()
            }
        }
    }
}

fn normalize_book(resp: CoinbaseOrderbookResponse) -> Vec<DomainEvent> {
    // Envelope ISO timestamp, parsed once per frame and shared across its events.
    let event_ts = resp.timestamp_us();
    resp.events
        .iter()
        .map(|e| {
            DomainEvent::Book(CoinbaseNormalizer::to_delta(
                resp.sequence_num,
                event_ts,
                e,
            ))
        })
        .collect()
}

/// Map one decoded `market_trades` print to a `DomainEvent::Trade`. Every
/// dropped print bumps `dropped_frames`.
fn normalize_trade(t: CoinbaseTradeData, metrics: &SourceMetrics) -> Option<DomainEvent> {
    let Some(pair) = COINBASE_CODEC.decode(&t.product_id) else {
        tracing::warn!(symbol = %t.product_id, "coinbase.trade.bad_symbol");
        metrics.add_dropped_frames(1);
        return None;
    };
    let (Ok(amount), Ok(price)) = (
        t.size.parse::<rust_decimal::Decimal>(),
        t.price.parse::<rust_decimal::Decimal>(),
    ) else {
        metrics.add_dropped_frames(1);
        return None;
    };
    // Coinbase taker side is upper-case `"BUY"`/`"SELL"` — `from_str_loose`
    // is case-insensitive. An unknown side is dropped (never fabricated as a
    // Buy), so the trade never pollutes the side distribution.
    let Some(side) = TradeSide::from_str_loose(&t.side) else {
        tracing::warn!(side = %t.side, id = %t.trade_id, "coinbase.trade.unknown_side");
        metrics.add_dropped_frames(1);
        return None;
    };
    let trade = Trade {
        source_trade_ts_us: t.timestamp_us(),
        local_trade_ts_us: 0,
        source_trade_rtt_us: 0,
        pair,
        side,
        amount,
        price,
        exchange: "coinbase".to_string(),
        id: t.trade_id.clone(),
        origin: Default::default(),
    };
    // Armed (cycle #7): `trade_id` is a per-product counter, verified
    // strictly monotonic AND dense (+1) across 208 live BTC-USD trades once
    // each frame's DESCENDING batch is reversed (the normalizer does the
    // reverse). Density is what makes `trades_lost = seq - last - 1` a real
    // count rather than a fabrication.
    let sequence = t.trade_id.parse::<u64>().ok();
    Some(DomainEvent::Trade { trade, sequence })
}

// ─────────────────────────────────────────────────────────────────────────
// ExchangeAdapter
// ─────────────────────────────────────────────────────────────────────────

/// Static, data-only Coinbase profile. Const-constructible so it lives in a
/// `static` (no init-time allocation: `Vec::new` is `const`).
static COINBASE_PROFILE: ExchangeProfile = ExchangeProfile {
    id: "coinbase",
    symbol_codec: COINBASE_CODEC,
    budget: ConnectionBudget {
        // Coinbase Advanced Trade public WSS: many products per socket.
        max_connections: None,
        max_streams_per_socket: None,
        subscribe_rate: Vec::new(),
        connect_attempt_rate: None,
        connection_lifetime: None,
    },
    schema_version: 1,
    protocol_revision: "coinbase-adv-v3",
};

/// The registry handle. Unit struct — all state is in the static profile.
pub struct CoinbaseAdapter;

/// The single compiled-in Coinbase instance (referenced by `register_all`).
pub static COINBASE: CoinbaseAdapter = CoinbaseAdapter;

impl ExchangeAdapter for CoinbaseAdapter {
    fn id(&self) -> &'static str {
        "coinbase"
    }

    fn profile(&self) -> &ExchangeProfile {
        &COINBASE_PROFILE
    }

    fn book_model(&self, _channel: &str) -> ReconstructionModel {
        // `l2_data` opens with a `type:"snapshot"` event then streams
        // `type:"update"` events; both ride the connection-wide `sequence_num`,
        // which is bumped by every message on the socket (trades, heartbeats,
        // other products) — NOT a per-book contiguous id. The book seeds itself
        // from the first WSS frame (no REST) and validates by `Monotonic` advance
        // (`update_id > last`); a RangeInclusive upper bound would gap on every
        // frame the counter skips ahead.
        ReconstructionModel::SeqDelta {
            predicate: SeqPredicate::Monotonic,
            source: SnapshotSource::WssSelfSeed,
        }
    }

    fn spawn(
        &self,
        symbols: Vec<String>,
        declared: DeclaredSet,
        tx: mpsc::Sender<DomainEvent>,
        shutdown: watch::Receiver<bool>,
        metrics: SourceMetrics,
    ) -> JoinHandle<TaskExit> {
        // Two sockets: the book socket (level2 + heartbeats, sequence-tracked)
        // and the trade socket (market_trades). Both feed the same DomainEvent
        // channel. The supervisor returns the first exit: on a real disconnect
        // the sibling is aborted so the worker's reconnect loop restarts BOTH
        // (they re-seed together); on graceful shutdown the sibling is awaited
        // so its bounded drain is not cut short.
        use aetelier_types::config::markets::market_config::DeclaredDatatype as DD;
        let want_book = declared.contains(DD::Orderbook);
        let want_trades = declared.contains(DD::Trades);
        let spawn_book = || {
            tokio::spawn(drive::<CoinbaseHooks, CoinbaseDecoder, CoinbaseNormalizer>(
                Arc::new(CoinbaseHooks::level2()),
                symbols.clone(),
                declared.clone(),
                CoinbaseNormalizer::sequence_tracked(metrics.clone()),
                tx.clone(),
                shutdown.clone(),
                DEFAULT_RAW_BUFFER,
                metrics.clone(),
            ))
        };
        let spawn_trades = || {
            tokio::spawn(drive::<CoinbaseHooks, CoinbaseDecoder, CoinbaseNormalizer>(
                Arc::new(CoinbaseHooks::trades()),
                symbols.clone(),
                declared.clone(),
                CoinbaseNormalizer::passthrough(metrics.clone()),
                tx.clone(),
                shutdown.clone(),
                DEFAULT_RAW_BUFFER,
                metrics.clone(),
            ))
        };
        match (want_book, want_trades) {
            (true, false) => return spawn_book(),
            (false, true) => return spawn_trades(),
            (false, false) => {
                return tokio::spawn(async move { TaskExit::Completed });
            }
            (true, true) => {}
        }
        let mut book = spawn_book();
        let mut trades = spawn_trades();
        tokio::spawn(async move {
            let panicked = || {
                TaskExit::Failed(DisconnectReason::TransportError {
                    source: "coinbase socket task panicked".into(),
                })
            };
            let (first, sibling) = tokio::select! {
                a = &mut book => (a, &mut trades),
                b = &mut trades => (b, &mut book),
            };
            if *shutdown.borrow() {
                let second = sibling.await;
                // Graceful path: surface a drain timeout from either socket.
                match (first, second) {
                    (Ok(TaskExit::Completed), Ok(other)) => other,
                    (Ok(f), Ok(_)) => f,
                    _ => panicked(),
                }
            } else {
                sibling.abort();
                first.unwrap_or_else(|_| panicked())
            }
        })
    }

    fn subscribe_frames_preview(
        &self,
        symbols: &[String],
        declared: &crate::framework::protocol::DeclaredSet,
    ) -> Vec<String> {
        use aetelier_types::config::markets::market_config::DeclaredDatatype as DD;
        let mut out = Vec::new();
        let text = |m: Message| match m {
            Message::Text(t) => Some(t.to_string()),
            _ => None,
        };
        if declared.contains(DD::Orderbook) {
            out.extend(
                CoinbaseHooks::level2()
                    .subscribe_frames(symbols, declared)
                    .into_iter()
                    .filter_map(text),
            );
        }
        if declared.contains(DD::Trades) {
            out.extend(
                CoinbaseHooks::trades()
                    .subscribe_frames(symbols, declared)
                    .into_iter()
                    .filter_map(text),
            );
        }
        out
    }

    fn replay_frame(
        &self,
        raw: &str,
    ) -> Result<Vec<DomainEvent>, Box<crate::errors::ExchangeError>> {
        use crate::clients::wss::WssDecoder;
        // Replay is stateless per frame (a fresh normalizer per call), so the
        // sequence tracker never fires here — offline replay certifies the
        // decode surface; the tracker has its own stateful tests.
        let normalizer = CoinbaseNormalizer::passthrough(SourceMetrics::default());
        match CoinbaseDecoder::decode(raw)? {
            Some(event) => Ok(normalizer.normalize(event)),
            None => Ok(Vec::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::wss::WssDecoder;
    use crate::sources::coinbase::responses::orderbooks::CoinbaseL2Update;
    use aetelier_types::orderbooks::decimal_to_f64;
    use aetelier_types::trading_pair::TradingPair;

    fn l2_update(side: &str, price: &str, qty: &str) -> CoinbaseL2Update {
        CoinbaseL2Update {
            side: side.into(),
            event_time: "2023-02-09T20:32:50.714964855Z".into(),
            price_level: price.into(),
            new_quantity: qty.into(),
        }
    }

    fn book_resp(ty: &str, seq: u64, product: &str) -> CoinbaseOrderbookResponse {
        CoinbaseOrderbookResponse {
            channel: "l2_data".into(),
            timestamp: "2023-02-09T20:32:50.714964855Z".into(),
            sequence_num: seq,
            events: vec![CoinbaseL2Event {
                ty: ty.into(),
                product_id: product.into(),
                updates: vec![
                    l2_update("bid", "100.0", "1.0"),
                    l2_update("offer", "101.0", "2.0"),
                ],
            }],
        }
    }

    fn trade(
        id: &str,
        product: &str,
        price: &str,
        size: &str,
        side: &str,
    ) -> CoinbaseTradeData {
        CoinbaseTradeData {
            trade_id: id.into(),
            product_id: product.into(),
            price: price.into(),
            size: size.into(),
            side: side.into(),
            time: "2023-02-09T20:19:35.396Z".into(),
        }
    }

    #[test]
    fn normalizes_snapshot_event_to_book_delta() {
        let evs = CoinbaseNormalizer::default().normalize(
            CoinbaseWssEvent::OrderbookData(book_resp("snapshot", 42, "BTC-USD")),
        );
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            DomainEvent::Book(nd) => {
                assert_eq!(nd.symbol, "BTC-USD");
                assert_eq!(nd.update_id, 42);
                assert_eq!(nd.sequence, 42);
                assert!(nd.is_snapshot);
                assert_eq!(nd.bids, vec![("100.0".to_string(), "1.0".to_string())]);
                assert_eq!(nd.asks, vec![("101.0".to_string(), "2.0".to_string())]);
            }
            other => panic!("expected Book, got {other:?}"),
        }
    }

    #[test]
    fn normalizes_update_event_as_non_snapshot_delta() {
        let evs = CoinbaseNormalizer::default().normalize(
            CoinbaseWssEvent::OrderbookData(book_resp("update", 43, "BTC-USD")),
        );
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            DomainEvent::Book(nd) => {
                assert_eq!(nd.update_id, 43);
                assert!(!nd.is_snapshot);
            }
            other => panic!("expected Book, got {other:?}"),
        }
    }

    #[test]
    fn normalizes_uppercase_sell_trade_to_domain_event() {
        let evs =
            CoinbaseNormalizer::default().normalize(CoinbaseWssEvent::TradeData(vec![
                trade("99", "ETH-USD", "1600.5", "0.25", "SELL"),
            ]));
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            DomainEvent::Trade { trade, sequence } => {
                assert_eq!(trade.exchange, "coinbase");
                assert_eq!(trade.id, "99");
                assert_eq!(trade.side, TradeSide::Sell);
                assert!((decimal_to_f64(trade.price) - 1600.5).abs() < 1e-9);
                assert!((decimal_to_f64(trade.amount) - 0.25).abs() < 1e-9);
                assert_eq!(trade.pair, TradingPair::new("ETH", "USD"));
                assert_eq!(*sequence, Some(99), "trade_id armed as the sequence");
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn multi_trade_frame_reverses_the_descending_batch_no_head_drop() {
        // The wire batches trades in DESCENDING trade_id order; the
        // normalizer must emit all of them (no head-drop) reversed to
        // ascending, each carrying its armed sequence.
        let trades = vec![
            trade("4", "BTC-USD", "103.0", "0.4", "SELL"),
            trade("3", "BTC-USD", "102.0", "0.3", "BUY"),
            trade("2", "BTC-USD", "101.0", "0.2", "SELL"),
            trade("1", "BTC-USD", "100.0", "0.1", "BUY"),
        ];
        let evs =
            CoinbaseNormalizer::default().normalize(CoinbaseWssEvent::TradeData(trades));
        assert_eq!(evs.len(), 4, "all trades must survive (no head-drop)");
        let seqs: Vec<Option<u64>> = evs
            .iter()
            .map(|e| match e {
                DomainEvent::Trade { sequence, .. } => *sequence,
                other => panic!("expected Trade, got {other:?}"),
            })
            .collect();
        assert_eq!(
            seqs,
            [Some(1), Some(2), Some(3), Some(4)],
            "emitted ascending with armed sequences"
        );
    }

    #[test]
    fn decoder_preserves_all_trades_in_a_multi_trade_frame() {
        // End-to-end: raw frame with 3 trades decodes to a TradeData(Vec) of 3.
        let mt = r#"{"channel":"market_trades","timestamp":"2023-02-09T20:19:35.39625135Z","sequence_num":0,"events":[{"type":"update","trades":[
            {"trade_id":"1","product_id":"BTC-USD","price":"100.0","size":"0.1","side":"BUY","time":"2023-02-09T20:19:35.396Z"},
            {"trade_id":"2","product_id":"BTC-USD","price":"101.0","size":"0.2","side":"SELL","time":"2023-02-09T20:19:35.396Z"},
            {"trade_id":"3","product_id":"BTC-USD","price":"102.0","size":"0.3","side":"BUY","time":"2023-02-09T20:19:35.396Z"}
        ]}]}"#;
        let CoinbaseWssEvent::TradeData(trades) =
            CoinbaseDecoder::decode(mt).unwrap().unwrap()
        else {
            panic!("expected TradeData");
        };
        assert_eq!(trades.len(), 3);
        let evs =
            CoinbaseNormalizer::default().normalize(CoinbaseWssEvent::TradeData(trades));
        assert_eq!(evs.len(), 3);
    }

    #[test]
    fn unknown_side_trade_is_dropped_not_labelled_buy() {
        let evs =
            CoinbaseNormalizer::default().normalize(CoinbaseWssEvent::TradeData(vec![
                trade("1", "BTC-USD", "100.0", "0.1", "BUY"),
                trade("2", "BTC-USD", "101.0", "0.2", "GARBAGE"),
            ]));
        assert_eq!(evs.len(), 1, "unknown-side trade must be dropped");
        match &evs[0] {
            DomainEvent::Trade { trade, .. } => assert_eq!(trade.id, "1"),
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn subscribe_frames_split_by_socket_role() {
        // Book socket: level2 + heartbeats (the contiguity anchor), one frame
        // per channel, each carrying the product_ids array.
        let frames = CoinbaseHooks::level2()
            .subscribe_frames(&["BTC-USD".to_string()], &DeclaredSet::all());
        assert_eq!(frames.len(), 2);
        let Message::Text(book) = &frames[0] else {
            panic!("expected a text frame");
        };
        let v: serde_json::Value = serde_json::from_str(book).unwrap();
        assert_eq!(v["type"], "subscribe");
        assert_eq!(v["channel"], "level2");
        assert_eq!(v["product_ids"][0], "BTC-USD");

        let Message::Text(hb) = &frames[1] else {
            panic!("expected a text frame");
        };
        let v: serde_json::Value = serde_json::from_str(hb).unwrap();
        assert_eq!(v["channel"], "heartbeats");

        // Trade socket: market_trades only — a trade frame must never advance
        // the book socket's sequence arithmetic.
        let frames = CoinbaseHooks::trades()
            .subscribe_frames(&["BTC-USD".to_string()], &DeclaredSet::all());
        assert_eq!(frames.len(), 1);
        let Message::Text(trades) = &frames[0] else {
            panic!("expected a text frame");
        };
        let v: serde_json::Value = serde_json::from_str(trades).unwrap();
        assert_eq!(v["channel"], "market_trades");
        assert_eq!(v["product_ids"][0], "BTC-USD");
    }

    #[test]
    fn decoder_routes_l2_and_market_trades() {
        let l2 = r#"{"channel":"l2_data","timestamp":"2023-02-09T20:32:50.714964855Z","sequence_num":0,"events":[{"type":"snapshot","product_id":"BTC-USD","updates":[{"side":"bid","event_time":"2023-02-09T20:32:50.714964855Z","price_level":"100.0","new_quantity":"1.0"}]}]}"#;
        assert!(matches!(
            CoinbaseDecoder::decode(l2).unwrap().unwrap(),
            CoinbaseWssEvent::OrderbookData(_)
        ));
        let mt = r#"{"channel":"market_trades","timestamp":"2023-02-09T20:19:35.39625135Z","sequence_num":0,"events":[{"type":"update","trades":[{"trade_id":"12345","product_id":"BTC-USD","price":"23000.1","size":"0.01","side":"BUY","time":"2023-02-09T20:19:35.396Z"}]}]}"#;
        assert!(matches!(
            CoinbaseDecoder::decode(mt).unwrap().unwrap(),
            CoinbaseWssEvent::TradeData(_)
        ));
        // Subscription ack → no event.
        assert!(
            CoinbaseDecoder::decode(r#"{"type":"subscriptions","channels":[]}"#)
                .unwrap()
                .is_none()
        );
    }

    // ── Sequence tracker (connection contiguity, Way-2 arithmetic) ────────

    #[tokio::test(start_paused = true)]
    async fn tracker_stays_silent_on_a_contiguous_stream() {
        let mut t = SeqTracker::new();
        let now = tokio::time::Instant::now();
        assert_eq!(t.observe(0, None, now), None);
        assert_eq!(t.observe(1, Some(100), now), None);
        for seq in 2..50u64 {
            assert_eq!(t.observe(seq, None, now), None);
        }
        // Even long after, a clean stream never confirms a gap.
        tokio::time::advance(Duration::from_millis(100)).await;
        assert_eq!(t.observe(50, None, tokio::time::Instant::now()), None);
    }

    #[tokio::test(start_paused = true)]
    async fn tracker_confirms_unexplained_jump_after_debounce() {
        let mut t = SeqTracker::new();
        let now = tokio::time::Instant::now();
        assert_eq!(t.observe(0, None, now), None);
        assert_eq!(t.observe(1, Some(100), now), None);
        // Slots 2,3,4 vanish. Detection arms the debounce; nothing fires yet.
        assert_eq!(t.observe(5, None, now), None);
        // Deadline is 10ms + 2..=20ms jitter — 50ms is past any draw.
        tokio::time::advance(Duration::from_millis(50)).await;
        let verdict = t.observe(6, None, tokio::time::Instant::now());
        assert_eq!(verdict, Some(3), "three data messages provably dropped");
        // Confirmation is one-shot: the counter resets.
        assert_eq!(t.observe(7, None, tokio::time::Instant::now()), None);
    }

    #[tokio::test(start_paused = true)]
    async fn tracker_attributes_dropped_heartbeats_and_stands_down() {
        let mut t = SeqTracker::new();
        let now = tokio::time::Instant::now();
        assert_eq!(t.observe(1, Some(100), now), None);
        assert_eq!(t.observe(2, None, now), None);
        // Slot 3 vanishes; the next heartbeat's counter jumped by exactly the
        // same amount — the missing message WAS the heartbeat. Harmless.
        assert_eq!(t.observe(4, Some(102), now), None);
        tokio::time::advance(Duration::from_millis(50)).await;
        assert_eq!(
            t.observe(5, None, tokio::time::Instant::now()),
            None,
            "a dropped heartbeat must never force a resync"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn tracker_reconciled_within_deadline_never_fires() {
        let mut t = SeqTracker::new();
        let now = tokio::time::Instant::now();
        assert_eq!(t.observe(1, Some(100), now), None);
        // Slot 2 vanishes (armed) …
        assert_eq!(t.observe(3, None, now), None);
        // … and 5ms later (inside the 12ms floor) the heartbeat explains it.
        tokio::time::advance(Duration::from_millis(5)).await;
        assert_eq!(t.observe(4, Some(102), tokio::time::Instant::now()), None);
        // Past every possible deadline: still clean.
        tokio::time::advance(Duration::from_millis(60)).await;
        assert_eq!(t.observe(5, None, tokio::time::Instant::now()), None);
    }

    #[tokio::test(start_paused = true)]
    async fn normalize_emits_gap_before_the_frames_own_deltas() {
        let n = CoinbaseNormalizer::sequence_tracked(SourceMetrics::default());
        assert_eq!(
            n.normalize(CoinbaseWssEvent::OrderbookData(book_resp(
                "snapshot", 0, "BTC-USD"
            )))
            .len(),
            1
        );
        // The subscription ack's slot is accounted (Control), so it can never
        // read as a gap.
        assert!(
            n.normalize(CoinbaseWssEvent::Control { sequence_num: 1 })
                .is_empty()
        );
        assert!(
            n.normalize(CoinbaseWssEvent::Heartbeat {
                sequence_num: 2,
                heartbeat_counter: 100
            })
            .is_empty()
        );
        // Slots 3,4 vanish — suspected, debouncing, frame still flows.
        assert_eq!(
            n.normalize(CoinbaseWssEvent::OrderbookData(book_resp(
                "update", 5, "BTC-USD"
            )))
            .len(),
            1
        );
        tokio::time::advance(Duration::from_millis(50)).await;
        let evs = n.normalize(CoinbaseWssEvent::OrderbookData(book_resp(
            "update", 6, "BTC-USD",
        )));
        assert_eq!(evs.len(), 2, "gap + the frame's own delta");
        match &evs[0] {
            DomainEvent::ConnectionGap { dropped } => assert_eq!(*dropped, 2),
            other => panic!("expected ConnectionGap first, got {other:?}"),
        }
        assert!(matches!(evs[1], DomainEvent::Book(_)));
    }

    /// Stateful replay of the REAL captured level2+heartbeats socket (live,
    /// 2026-07-16): a clean capture must produce ZERO ConnectionGap events —
    /// the false-positive guard for the tracker (acks, heartbeats, and l2
    /// frames all account their slots). Dropping one mid-stream frame from
    /// the same capture must produce EXACTLY ONE.
    #[tokio::test(start_paused = true)]
    async fn real_capture_replays_clean_and_detects_an_injected_drop() {
        use crate::clients::wss::WssDecoder;
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/datasets/coinbase/btcusd_l2_heartbeats.jsonl"
        );
        let raw = std::fs::read_to_string(path).expect("fixture present");
        let frames: Vec<&str> = raw.lines().filter(|l| !l.is_empty()).collect();
        assert!(frames.len() > 100, "fixture should carry a real window");

        // Clean pass: no gap, every frame decodes.
        let n = CoinbaseNormalizer::sequence_tracked(SourceMetrics::default());
        let mut gaps = 0u32;
        for f in &frames {
            let Some(ev) = CoinbaseDecoder::decode(f).expect("frame decodes") else {
                continue;
            };
            for de in n.normalize(ev) {
                if matches!(de, DomainEvent::ConnectionGap { .. }) {
                    gaps += 1;
                }
            }
        }
        tokio::time::advance(Duration::from_millis(60)).await;
        assert_eq!(gaps, 0, "a clean live capture must never confirm a gap");

        // Injected drop: skip one mid-stream frame, advance past the
        // debounce, keep replaying — exactly one confirmed gap.
        let n = CoinbaseNormalizer::sequence_tracked(SourceMetrics::default());
        let skip = frames.len() / 2;
        let mut confirmed: Vec<u64> = Vec::new();
        for (i, f) in frames.iter().enumerate() {
            if i == skip {
                continue;
            }
            let Some(ev) = CoinbaseDecoder::decode(f).expect("frame decodes") else {
                continue;
            };
            for de in n.normalize(ev) {
                if let DomainEvent::ConnectionGap { dropped } = de {
                    confirmed.push(dropped);
                }
            }
            if i == skip + 1 {
                tokio::time::advance(Duration::from_millis(50)).await;
            }
        }
        assert_eq!(confirmed, vec![1], "exactly one one-message gap confirmed");
    }

    /// The density guarantee behind the armed trade sequence, frozen against
    /// the REAL capture: replaying every `market_trades` frame of the live
    /// BTC-USD window must yield strictly ascending, DENSE (+1) sequences.
    /// Dense is the load-bearing property — `trades_lost = seq - last - 1`
    /// fabricates losses if ids can legitimately skip.
    #[test]
    fn real_capture_trade_sequences_are_dense_after_batch_reverse() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/datasets/coinbase/btcusd_book_trade.jsonl"
        );
        let raw = std::fs::read_to_string(path).expect("fixture present");
        let adapter = CoinbaseAdapter;
        let mut seqs: Vec<u64> = Vec::new();
        for line in raw.lines().filter(|l| !l.is_empty()) {
            for ev in adapter.replay_frame(line).expect("frame decodes") {
                if let DomainEvent::Trade { sequence, .. } = ev {
                    seqs.push(sequence.expect("coinbase trades are armed"));
                }
            }
        }
        assert!(seqs.len() > 100, "the window carries a real trade sample");
        for w in seqs.windows(2) {
            assert_eq!(
                w[1],
                w[0] + 1,
                "trade_id must be dense (+1) — the premise of loss accounting"
            );
        }
    }

    #[test]
    fn decoder_surfaces_heartbeats_and_sequenced_acks() {
        // Verbatim frames from the live capture (2026-07-16).
        let hb = r#"{"channel":"heartbeats","timestamp":"2026-07-16T20:28:53.166419882Z","sequence_num":33,"events":[{"current_time":"2026-07-16 20:28:53.16294062 +0000 UTC m=+75497.317767664","heartbeat_counter":75497}]}"#;
        match CoinbaseDecoder::decode(hb).unwrap().unwrap() {
            CoinbaseWssEvent::Heartbeat {
                sequence_num,
                heartbeat_counter,
            } => {
                assert_eq!(sequence_num, 33);
                assert_eq!(heartbeat_counter, 75497);
            }
            other => panic!("expected Heartbeat, got {other:?}"),
        }

        let ack = r#"{"channel":"subscriptions","timestamp":"2026-07-16T20:28:52.466793983Z","sequence_num":5,"events":[{"subscriptions":{"level2":["BTC-USD"]}}]}"#;
        match CoinbaseDecoder::decode(ack).unwrap().unwrap() {
            CoinbaseWssEvent::Control { sequence_num } => {
                assert_eq!(sequence_num, 5)
            }
            other => panic!("expected Control, got {other:?}"),
        }

        // An unsequenced unknown frame still decodes to nothing.
        assert!(
            CoinbaseDecoder::decode(r#"{"channel":"mystery"}"#)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn profile_and_book_model_match_the_coinbase_row() {
        let a = CoinbaseAdapter;
        assert_eq!(a.id(), "coinbase");
        assert!(matches!(a.profile().symbol_codec, SymbolCodec::Hyphen));
        assert_eq!(a.profile().id, "coinbase");
        assert_eq!(a.profile().protocol_revision, "coinbase-adv-v3");
        assert!(matches!(
            a.book_model("orders"),
            ReconstructionModel::SeqDelta {
                predicate: SeqPredicate::Monotonic,
                source: SnapshotSource::WssSelfSeed,
            }
        ));
    }
}
