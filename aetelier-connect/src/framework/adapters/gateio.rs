//! Gate.io V4 public spot market-data adapter: subscribes to `spot.order_book`
//! and `spot.trades` streams and normalizes decoded frames into the framework's
//! `DomainEvent` stream.
//!
//! Subscribes each `(symbol × channel)` topic with its own
//! `{"time":…,"channel":…,"event":"subscribe","payload":[…]}` frame, where the
//! envelope's `time` is a live Unix-seconds stamp computed at send time.
//! Keepalive is a `Heartbeat::Text` whose payload is recomputed per tick as
//! `{"time":…,"channel":"spot.ping"}` (the `spot.pong` reply is swallowed by the
//! decoder).
//!
//! `spot.order_book` is Gate.io's **limited-depth full-snapshot** channel: each
//! frame is a complete top-N book (here top-20, every 100ms), not an incremental
//! delta — so it maps onto [`ReconstructionModel::FullRefresh`] (every frame
//! replaces the book; no seed, no continuity gate). Its monotonic `lastUpdateId`
//! jumps by many ticks per frame, so a sequence predicate would gap on every
//! frame; the id is still carried on `update_id`/`sequence` for diagnostics.
//! (The incremental `spot.order_book_update` U/u channel would be `SeqDelta`, but
//! the decoder parses the full-snapshot `result` shape, not that delta shape.)

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::protocol::Message;

use aetelier_types::orderbooks::NormalizedDelta;
use aetelier_types::trades::{Trade, TradeSide};

use crate::framework::budget::{ConnectionBudget, SourceMetrics};
use crate::framework::driver::{DEFAULT_RAW_BUFFER, drive};
use crate::framework::model::{
    DomainEvent, Normalizer, ReconstructionModel, epoch_to_us,
};
use crate::framework::protocol::DeclaredSet;
use crate::framework::protocol::{Heartbeat, ProtocolHooks};
use crate::framework::registry::{ExchangeAdapter, ExchangeProfile, TaskExit};
use crate::framework::symbol::SymbolCodec;
use crate::sources::gateio::decoder::GateioDecoder;
use crate::sources::gateio::events::GateioWssEvent;
use crate::sources::gateio::responses::{GateioOrderbookData, GateioTradeData};

/// Gate.io V4 public (no-auth) spot endpoint.
const GATEIO_WSS_URL: &str = "wss://api.gateio.ws/ws/v4/";

/// App-level `spot.ping` cadence. Gate.io closes a socket idle past its server
/// window; a 15s `{"time":…,"channel":"spot.ping"}` keeps it warm (the
/// `spot.pong` reply is swallowed by the decoder).
const PING_SECS: u64 = 15;

/// Limited-depth full-snapshot channel.
const BOOK_CHANNEL: &str = "spot.order_book";

/// Public-trades channel (single trade object per frame).
const TRADE_CHANNEL: &str = "spot.trades";

/// Book snapshot push interval Gate.io accepts in the subscribe `payload`.
const BOOK_INTERVAL: &str = "100ms";

/// Wire symbol codec — `BTC_USDT`.
const GATEIO_CODEC: SymbolCodec = SymbolCodec::Underscore { upper: true };

/// Current Unix time in whole seconds (the subscribe/heartbeat envelope `time`).
fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ─────────────────────────────────────────────────────────────────────────
// ProtocolHooks
// ─────────────────────────────────────────────────────────────────────────

/// Gate.io WSS protocol behaviour. Overrides the `endpoint`,
/// `subscribe_frames`, and `heartbeat` hooks; all other `ProtocolHooks`
/// defaults apply.
pub struct GateioHooks;

impl ProtocolHooks for GateioHooks {
    fn endpoint(&self) -> String {
        GATEIO_WSS_URL.to_string()
    }

    /// One subscribe frame **per `(symbol × channel)` topic** — Gate.io does not
    /// fan out within a single frame. `symbols` are venue wire symbols
    /// (`"BTC_USDT"`, Underscore codec). The envelope `time` is a live stamp.
    fn subscribe_frames(
        &self,
        symbols: &[String],
        declared: &DeclaredSet,
    ) -> Vec<Message> {
        use aetelier_types::config::markets::market_config::DeclaredDatatype as DD;
        let mut frames = Vec::with_capacity(symbols.len() * 2);
        for s in symbols {
            let t = unix_secs();
            if declared.contains(DD::Orderbook) {
                let book = serde_json::json!({
                    "time": t,
                    "channel": BOOK_CHANNEL,
                    "event": "subscribe",
                    "payload": [s, "20", BOOK_INTERVAL],
                });
                frames.push(Message::Text(book.to_string().into()));
            }
            if declared.contains(DD::Trades) {
                let trades = serde_json::json!({
                    "time": t,
                    "channel": TRADE_CHANNEL,
                    "event": "subscribe",
                    "payload": [s],
                });
                frames.push(Message::Text(trades.to_string().into()));
            }
        }
        frames
    }

    fn heartbeat(&self) -> Heartbeat {
        Heartbeat::Text {
            every: Duration::from_secs(PING_SECS),
            payload: Arc::new(|| {
                serde_json::json!({ "time": unix_secs(), "channel": "spot.ping" })
                    .to_string()
            }),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Normalizer — decoded GateioWssEvent → DomainEvent
// ─────────────────────────────────────────────────────────────────────────

/// Maps a decoded [`GateioWssEvent`] to `DomainEvent`s. Derives the canonical
/// pair from the event's venue symbol (the socket is multi-symbol). Holds the
/// worker's shared [`SourceMetrics`] so every dropped event is counted, not
/// just logged.
#[derive(Default)]
pub struct GateioNormalizer {
    pub metrics: SourceMetrics,
}

impl GateioNormalizer {
    /// Build a `NormalizedDelta` from one Gate.io `spot.order_book` payload —
    /// a complete top-20 book, so `is_snapshot` is `true` (FullRefresh replaces
    /// the book each frame). `update_id`/`sequence` carry the monotonic
    /// `lastUpdateId` for diagnostics only. Negative ids clamp.
    fn to_delta(data: &GateioOrderbookData) -> NormalizedDelta {
        let levels = |side: &[crate::sources::gateio::responses::GateioLevel]| {
            side.iter()
                .map(|l| (l.price_str().to_string(), l.size_str().to_string()))
                .collect::<Vec<_>>()
        };
        let id = data.last_update_id.max(0) as u64;
        NormalizedDelta {
            symbol: data.symbol.clone(),
            bids: levels(&data.bids),
            asks: levels(&data.asks),
            update_id: id,
            sequence: id,
            source_orderbook_ts_us: epoch_to_us(data.ts_ms),
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            checksum: None,
            orders: Vec::new(),
            // Every `spot.order_book` frame is a complete top-20 book.
            is_snapshot: true,
        }
    }
}

impl Normalizer for GateioNormalizer {
    type Event = GateioWssEvent;

    fn normalize(&self, event: GateioWssEvent) -> Vec<DomainEvent> {
        match event {
            GateioWssEvent::OrderbookData(resp) => {
                vec![DomainEvent::Book(Self::to_delta(&resp.result))]
            }
            GateioWssEvent::TradeData(t) => {
                normalize_trade(t, &self.metrics).into_iter().collect()
            }
        }
    }
}

/// Map one decoded `spot.trades` print to a `DomainEvent::Trade`. Every
/// dropped print bumps `dropped_frames`.
fn normalize_trade(t: GateioTradeData, metrics: &SourceMetrics) -> Option<DomainEvent> {
    let Some(pair) = GATEIO_CODEC.decode(&t.currency_pair) else {
        tracing::warn!(symbol = %t.currency_pair, "gateio.trade.bad_symbol");
        metrics.add_dropped_frames(1);
        return None;
    };
    let (Ok(amount), Ok(price)) = (
        t.amount.parse::<rust_decimal::Decimal>(),
        t.price.parse::<rust_decimal::Decimal>(),
    ) else {
        metrics.add_dropped_frames(1);
        return None;
    };
    // Gate publishes the taker side as `"buy"`/`"sell"`.
    let Some(side) = TradeSide::from_str_loose(&t.side) else {
        tracing::warn!(side = %t.side, id = %t.id, "gateio.trade.unknown_side");
        metrics.add_dropped_frames(1);
        return None;
    };
    let trade = Trade {
        source_trade_ts_us: epoch_to_us(t.ts_ms()),
        local_trade_ts_us: 0,
        source_trade_rtt_us: 0,
        pair,
        side,
        amount,
        price,
        exchange: "gateio".to_string(),
        id: t.id.to_string(),
        origin: Default::default(),
    };
    // Gate's public trade stream carries a per-pair monotonic `id` (verified
    // ascending +1 on live BTC_USDT capture, cycle #3); arming it feeds the
    // SourcedTradebook loss accounting. Trades never gap/resync (no re-seed),
    // only count losses.
    Some(DomainEvent::Trade {
        trade,
        sequence: Some(t.id),
    })
}

// ─────────────────────────────────────────────────────────────────────────
// ExchangeAdapter — registry entry
// ─────────────────────────────────────────────────────────────────────────

/// Static, data-only Gate.io profile.
static GATEIO_PROFILE: ExchangeProfile = ExchangeProfile {
    id: "gateio",
    symbol_codec: GATEIO_CODEC,
    budget: ConnectionBudget {
        // Gate.io spot: many channels per socket; per-IP connect/subscribe
        // windows are enforced by the planner.
        max_connections: None,
        max_streams_per_socket: None,
        subscribe_rate: Vec::new(),
        connect_attempt_rate: None,
        connection_lifetime: None,
    },
    schema_version: 1,
    protocol_revision: "gateio-v4",
};

/// The registry handle. Unit struct — all state is in the static profile.
pub struct GateioAdapter;

/// The single compiled-in Gate.io instance (referenced by `register_all`).
pub static GATEIO: GateioAdapter = GateioAdapter;

impl ExchangeAdapter for GateioAdapter {
    fn id(&self) -> &'static str {
        "gateio"
    }

    fn profile(&self) -> &ExchangeProfile {
        &GATEIO_PROFILE
    }

    fn book_model(&self, _channel: &str) -> ReconstructionModel {
        // `spot.order_book` is the limited-depth full-snapshot channel — every
        // frame is a complete top-20 book, so each frame replaces the prior one.
        ReconstructionModel::FullRefresh
    }

    fn spawn(
        &self,
        symbols: Vec<String>,
        declared: DeclaredSet,
        tx: mpsc::Sender<DomainEvent>,
        shutdown: watch::Receiver<bool>,
        metrics: SourceMetrics,
    ) -> JoinHandle<TaskExit> {
        tokio::spawn(drive::<GateioHooks, GateioDecoder, GateioNormalizer>(
            Arc::new(GateioHooks),
            symbols,
            declared,
            GateioNormalizer {
                metrics: metrics.clone(),
            },
            tx,
            shutdown,
            DEFAULT_RAW_BUFFER,
            metrics,
        ))
    }

    fn subscribe_frames_preview(
        &self,
        symbols: &[String],
        declared: &crate::framework::protocol::DeclaredSet,
    ) -> Vec<String> {
        GateioHooks
            .subscribe_frames(symbols, declared)
            .into_iter()
            .filter_map(|m| match m {
                Message::Text(t) => Some(t.to_string()),
                _ => None,
            })
            .collect()
    }

    fn replay_frame(
        &self,
        raw: &str,
    ) -> Result<Vec<DomainEvent>, Box<crate::errors::ExchangeError>> {
        use crate::clients::wss::WssDecoder;
        let normalizer = GateioNormalizer {
            metrics: SourceMetrics::default(),
        };
        match GateioDecoder::decode(raw)? {
            Some(event) => Ok(normalizer.normalize(event)),
            None => Ok(Vec::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::wss::WssDecoder;
    use crate::sources::gateio::responses::{
        GateioLevel, GateioOrderbookResponse, GateioTradeResponse,
    };
    use aetelier_types::orderbooks::decimal_to_f64;
    use aetelier_types::trading_pair::TradingPair;

    fn book_data(id: i64) -> GateioOrderbookData {
        GateioOrderbookData {
            ts_ms: 1_606_295_412_123,
            last_update_id: id,
            symbol: "BTC_USDT".into(),
            bids: vec![GateioLevel("100.0".into(), "1.0".into())],
            asks: vec![GateioLevel("101.0".into(), "2.0".into())],
        }
    }

    #[test]
    fn normalizes_order_book_snapshot_to_book_event() {
        let resp = GateioOrderbookResponse {
            time_ms: Some(1_606_295_412_213),
            channel: "spot.order_book".into(),
            event: "update".into(),
            result: book_data(48_791_820),
        };
        let evs =
            GateioNormalizer::default().normalize(GateioWssEvent::OrderbookData(resp));
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            DomainEvent::Book(nd) => {
                assert_eq!(nd.symbol, "BTC_USDT");
                assert_eq!(nd.update_id, 48_791_820);
                assert_eq!(nd.sequence, 48_791_820);
                // Full-snapshot channel: every frame is a complete book.
                assert!(nd.is_snapshot);
                assert_eq!(nd.bids.len(), 1);
                assert_eq!(nd.asks.len(), 1);
            }
            other => panic!("expected Book, got {other:?}"),
        }
    }

    #[test]
    fn normalizes_trade_to_domain_event() {
        let t = GateioTradeData {
            id: 309_143_071,
            create_time: 1_606_292_218,
            create_time_ms: "1606292218213.4578".into(),
            side: "sell".into(),
            currency_pair: "BTC_USDT".into(),
            amount: "16.47".into(),
            price: "0.4705".into(),
        };
        let evs = GateioNormalizer::default().normalize(GateioWssEvent::TradeData(t));
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            DomainEvent::Trade { trade, sequence } => {
                assert_eq!(trade.exchange, "gateio");
                assert_eq!(trade.id, "309143071");
                assert_eq!(trade.side, TradeSide::Sell);
                assert!((decimal_to_f64(trade.price) - 0.4705).abs() < 1e-9);
                assert!((decimal_to_f64(trade.amount) - 16.47).abs() < 1e-9);
                assert_eq!(trade.pair, TradingPair::new("BTC", "USDT"));
                assert_eq!(trade.source_trade_ts_us, 1_606_292_218_213_000);
                // The monotonic per-pair id arms the sequence (cycle #3).
                assert_eq!(*sequence, Some(309_143_071));
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn subscribe_frames_are_one_per_channel() {
        let frames =
            GateioHooks.subscribe_frames(&["BTC_USDT".to_string()], &DeclaredSet::all());
        assert_eq!(frames.len(), 2);
        let Message::Text(book) = &frames[0] else {
            panic!("expected a text frame");
        };
        let bv: serde_json::Value = serde_json::from_str(book).unwrap();
        assert_eq!(bv["channel"], "spot.order_book");
        assert_eq!(bv["event"], "subscribe");
        assert_eq!(bv["payload"][0], "BTC_USDT");
        assert_eq!(bv["payload"][2], "100ms");
        assert!(bv["time"].is_number());

        let Message::Text(trades) = &frames[1] else {
            panic!("expected a text frame");
        };
        let tv: serde_json::Value = serde_json::from_str(trades).unwrap();
        assert_eq!(tv["channel"], "spot.trades");
        assert_eq!(tv["payload"][0], "BTC_USDT");
    }

    #[test]
    fn heartbeat_payload_is_a_spot_ping() {
        let Heartbeat::Text { every, payload } = GateioHooks.heartbeat() else {
            panic!("expected a text heartbeat");
        };
        assert_eq!(every, Duration::from_secs(PING_SECS));
        let v: serde_json::Value = serde_json::from_str(&payload()).unwrap();
        assert_eq!(v["channel"], "spot.ping");
        assert!(v["time"].is_number());
    }

    #[test]
    fn decoder_dispatches_trade_frame() {
        let raw = r#"{"time":1606292218,"time_ms":1606292218231,
            "channel":"spot.trades","event":"update",
            "result":{"id":309143071,"create_time":1606292218,
            "create_time_ms":"1606292218213.4578","side":"sell",
            "currency_pair":"BTC_USDT","amount":"16.47","price":"0.4705"}}"#;
        let ev = GateioDecoder::decode(raw).unwrap().expect("a trade event");
        let evs = GateioNormalizer::default().normalize(ev);
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            DomainEvent::Trade { trade, .. } => {
                assert_eq!(trade.exchange, "gateio");
                assert_eq!(trade.id, "309143071");
            }
            other => panic!("expected Trade, got {other:?}"),
        }
        // Control frames (subscribe ack / pong) decode to None.
        let ack = r#"{"channel":"spot.trades","event":"subscribe","result":{"status":"success"}}"#;
        assert!(GateioDecoder::decode(ack).unwrap().is_none());
        // The trade envelope round-trips through the typed response too.
        let resp: GateioTradeResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(resp.channel, "spot.trades");
    }

    #[test]
    fn profile_and_book_model_match_the_gateio_row() {
        let a = GateioAdapter;
        assert_eq!(a.id(), "gateio");
        assert_eq!(a.profile().id, "gateio");
        assert_eq!(a.profile().protocol_revision, "gateio-v4");
        assert!(matches!(
            a.profile().symbol_codec,
            SymbolCodec::Underscore { upper: true }
        ));
        // `spot.order_book` is a full-snapshot channel, not an incremental delta.
        assert!(matches!(
            a.book_model("orders"),
            ReconstructionModel::FullRefresh
        ));
    }
}
