//! Poloniex v2 spot public market-data adapter: subscribes to `book_lv2` depth
//! and `trades` streams over a single multi-symbol socket and normalizes them
//! into the framework's `DomainEvent` stream.
//!
//! The `book_lv2` channel is self-seeding: its first frame is an
//! `action:"snapshot"` on the same socket, so there is no REST GET and no
//! in-band request. Each frame carries both `id` (this update) and `lastId`
//! (the previous one), so deltas chain by an exact previous-id pointer. A
//! client-driven JSON ping (`{"event":"ping"}` every 25s) keeps the socket
//! warm; the server answers `{"event":"pong"}`. Codec is uppercase
//! `Underscore` (`BTC_USDT`).

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::protocol::Message;

use aetelier_types::orderbooks::NormalizedDelta;
use aetelier_types::trades::{Trade, TradeSide};

use crate::framework::budget::{ConnectionBudget, SourceMetrics};
use crate::framework::driver::{DEFAULT_RAW_BUFFER, drive};
use crate::framework::model::{
    DomainEvent, Normalizer, ReconstructionModel, SeqPredicate, SnapshotSource,
    epoch_to_us,
};
use crate::framework::protocol::DeclaredSet;
use crate::framework::protocol::{Heartbeat, ProtocolHooks};
use crate::framework::registry::{ExchangeAdapter, ExchangeProfile, TaskExit};
use crate::framework::symbol::SymbolCodec;

use crate::sources::poloniex::decoder::PoloniexDecoder;
use crate::sources::poloniex::events::PoloniexWssEvent;
use crate::sources::poloniex::responses::orderbooks::PoloniexBookData;

/// Public spot v2 endpoint (single socket, multi-symbol, multi-channel).
const POLONIEX_WSS_URL: &str = "wss://ws.poloniex.com/ws/public";

/// Application-level ping cadence. Poloniex disconnects an idle socket; a
/// client `{"event":"ping"}` keeps it warm and the server answers `pong`.
const PING_SECS: u64 = 25;

/// Wire symbol codec — uppercase `BTC_USDT`. Shared by the static profile and
/// the normalizer's pair decode.
const POLONIEX_CODEC: SymbolCodec = SymbolCodec::Underscore { upper: true };

// ─────────────────────────────────────────────────────────────────────────
// ProtocolHooks
// ─────────────────────────────────────────────────────────────────────────

/// Poloniex WSS protocol behaviour. Overrides only
/// `endpoint`/`subscribe_frames`/`heartbeat`; the public-open `prepare`
/// default applies.
pub struct PoloniexHooks;

impl ProtocolHooks for PoloniexHooks {
    fn endpoint(&self) -> String {
        POLONIEX_WSS_URL.to_string()
    }

    /// Two frames: one for `book_lv2`, one for `trades`, each carrying the full
    /// symbol set. Symbols are venue wire symbols (`"BTC_USDT"`).
    fn subscribe_frames(
        &self,
        symbols: &[String],
        declared: &DeclaredSet,
    ) -> Vec<Message> {
        use aetelier_types::config::markets::market_config::DeclaredDatatype as DD;
        let mut frames = Vec::with_capacity(2);
        if declared.contains(DD::Orderbook) {
            let book = serde_json::json!({
                "event": "subscribe",
                "channel": ["book_lv2"],
                "symbols": symbols,
            });
            frames.push(Message::Text(book.to_string().into()));
        }
        if declared.contains(DD::Trades) {
            let trades = serde_json::json!({
                "event": "subscribe",
                "channel": ["trades"],
                "symbols": symbols,
            });
            frames.push(Message::Text(trades.to_string().into()));
        }
        frames
    }

    /// Client-driven application ping: `{"event":"ping"}` every 25s. The server
    /// answers `{"event":"pong"}`, which the decoder maps to `Ok(None)`.
    fn heartbeat(&self) -> Heartbeat {
        Heartbeat::Text {
            every: Duration::from_secs(PING_SECS),
            payload: Arc::new(|| serde_json::json!({ "event": "ping" }).to_string()),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Normalizer — decoded PoloniexWssEvent → DomainEvent
// ─────────────────────────────────────────────────────────────────────────

/// Maps a decoded `PoloniexWssEvent` to `DomainEvent`s. Derives the canonical
/// pair from each entry's venue symbol (the socket is multi-symbol). Holds the
/// worker's shared [`SourceMetrics`] so every dropped event is counted, not
/// just logged.
#[derive(Default)]
pub struct PoloniexNormalizer {
    pub metrics: SourceMetrics,
}

impl PoloniexNormalizer {
    /// Project one `book_lv2` entry to a `NormalizedDelta`:
    /// `update_id = id` (this update), `sequence = lastId` (prev-id pointer);
    /// the shared apply checks `delta.sequence == last`.
    fn to_delta(data: PoloniexBookData, is_snapshot: bool) -> NormalizedDelta {
        let levels = |side: Vec<[String; 2]>| {
            side.into_iter()
                .map(|[p, s]| (p, s))
                .collect::<Vec<(String, String)>>()
        };
        NormalizedDelta {
            symbol: data.symbol,
            bids: levels(data.bids),
            asks: levels(data.asks),
            update_id: data.id,
            sequence: data.last_id,
            source_orderbook_ts_us: epoch_to_us(data.ts),
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            checksum: None,
            orders: Vec::new(),
            is_snapshot,
        }
    }
}

impl Normalizer for PoloniexNormalizer {
    type Event = PoloniexWssEvent;

    fn normalize(&self, event: PoloniexWssEvent) -> Vec<DomainEvent> {
        match event {
            PoloniexWssEvent::Book(frame) => {
                // `action:"snapshot"` is the self-seed; `"update"` is a delta.
                let is_snapshot = frame.action == "snapshot";
                frame
                    .data
                    .into_iter()
                    .map(|d| DomainEvent::Book(Self::to_delta(d, is_snapshot)))
                    .collect()
            }
            PoloniexWssEvent::Trades(frame) => frame
                .data
                .into_iter()
                .filter_map(|t| {
                    let Some(pair) = POLONIEX_CODEC.decode(&t.symbol) else {
                        tracing::warn!(symbol = %t.symbol, "poloniex.trade.bad_symbol");
                        self.metrics.add_dropped_frames(1);
                        return None;
                    };
                    let (Ok(amount), Ok(price)) =
                        (t.quantity.parse::<rust_decimal::Decimal>(), t.price.parse::<rust_decimal::Decimal>())
                    else {
                        self.metrics.add_dropped_frames(1);
                        return None;
                    };
                    let Some(side) = TradeSide::from_str_loose(&t.taker_side) else {
                        tracing::warn!(side = %t.taker_side, id = %t.id, "poloniex.trade.unknown_side");
                        self.metrics.add_dropped_frames(1);
                        return None;
                    };
                    Some(DomainEvent::Trade {
                        trade: Trade {
                            source_trade_ts_us: epoch_to_us(if t.create_time > 0 {
                                t.create_time
                            } else {
                                t.ts
                            }),
                            local_trade_ts_us: 0,
                            source_trade_rtt_us: 0,
                            pair,
                            side,
                            amount,
                            price,
                            exchange: "poloniex".to_string(),
                            id: t.id.clone(),
                            origin: Default::default(),
                        },
                        // Poloniex trade `id` is a per-pair monotonic numeric
                        // string (verified +1 on live BTC_USDT capture, cycle
                        // #4); armed for SourcedTradebook loss accounting.
                        sequence: t.id.parse::<u64>().ok(),
                    })
                })
                .collect(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// ExchangeAdapter
// ─────────────────────────────────────────────────────────────────────────

/// Static, data-only Poloniex profile. Const-constructible so it lives in a
/// `static`.
static POLONIEX_PROFILE: ExchangeProfile = ExchangeProfile {
    id: "poloniex",
    symbol_codec: POLONIEX_CODEC,
    budget: ConnectionBudget {
        max_connections: None,
        max_streams_per_socket: None,
        subscribe_rate: Vec::new(),
        connect_attempt_rate: None,
        connection_lifetime: None,
    },
    schema_version: 1,
    protocol_revision: "poloniex-v2",
};

/// The registry handle. Unit struct — all state is in the static profile.
pub struct PoloniexAdapter;

/// The single compiled-in Poloniex instance (referenced by `register_all`).
pub static POLONIEX: PoloniexAdapter = PoloniexAdapter;

impl ExchangeAdapter for PoloniexAdapter {
    fn id(&self) -> &'static str {
        "poloniex"
    }

    fn profile(&self) -> &ExchangeProfile {
        &POLONIEX_PROFILE
    }

    fn book_model(&self, _channel: &str) -> ReconstructionModel {
        // book_lv2 deltas chain by `lastId` (ExactPrev), and the first frame is
        // an in-band `action:"snapshot"` — self-seeded, no REST.
        ReconstructionModel::SeqDelta {
            predicate: SeqPredicate::ExactPrev,
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
        tokio::spawn(drive::<PoloniexHooks, PoloniexDecoder, PoloniexNormalizer>(
            Arc::new(PoloniexHooks),
            symbols,
            declared,
            PoloniexNormalizer {
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
        PoloniexHooks
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
        let normalizer = PoloniexNormalizer {
            metrics: SourceMetrics::default(),
        };
        match PoloniexDecoder::decode(raw)? {
            Some(event) => Ok(normalizer.normalize(event)),
            None => Ok(Vec::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::wss::WssDecoder;
    use crate::sources::poloniex::responses::{
        PoloniexBookFrame, PoloniexTradeData, PoloniexTradeFrame,
    };
    use aetelier_types::orderbooks::decimal_to_f64;
    use aetelier_types::trading_pair::TradingPair;

    #[test]
    fn book_snapshot_sets_exact_prev_pointer_and_snapshot_flag() {
        let frame = PoloniexBookFrame {
            action: "snapshot".into(),
            data: vec![PoloniexBookData {
                symbol: "BTC_USDT".into(),
                asks: vec![["101.0".into(), "2.0".into()]],
                bids: vec![["100.0".into(), "1.0".into()]],
                id: 100,
                last_id: 0,
                ts: 1_700_000_000_000,
            }],
        };
        let evs = PoloniexNormalizer::default().normalize(PoloniexWssEvent::Book(frame));
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            DomainEvent::Book(nd) => {
                assert_eq!(nd.symbol, "BTC_USDT");
                assert_eq!(nd.update_id, 100); // id → this update
                assert_eq!(nd.sequence, 0); // lastId → prev-id pointer
                assert_eq!(nd.source_orderbook_ts_us, 1_700_000_000_000_000);
                assert!(nd.is_snapshot);
                assert_eq!(nd.bids, vec![("100.0".to_string(), "1.0".to_string())]);
                assert_eq!(nd.asks, vec![("101.0".to_string(), "2.0".to_string())]);
            }
            other => panic!("expected Book, got {other:?}"),
        }
    }

    #[test]
    fn book_update_carries_last_id_as_sequence() {
        let frame = PoloniexBookFrame {
            action: "update".into(),
            data: vec![PoloniexBookData {
                symbol: "BTC_USDT".into(),
                asks: vec![],
                bids: vec![["100.5".into(), "3.0".into()]],
                id: 101,
                last_id: 100,
                ts: 1_700_000_000_001,
            }],
        };
        let evs = PoloniexNormalizer::default().normalize(PoloniexWssEvent::Book(frame));
        match &evs[0] {
            DomainEvent::Book(nd) => {
                assert_eq!(nd.update_id, 101);
                assert_eq!(nd.sequence, 100); // lastId → ExactPrev pointer
                assert!(!nd.is_snapshot);
            }
            other => panic!("expected Book, got {other:?}"),
        }
    }

    #[test]
    fn normalizes_trade_taker_side_and_amount() {
        let frame = PoloniexTradeFrame {
            data: vec![PoloniexTradeData {
                symbol: "BTC_USDT".into(),
                id: "42".into(),
                price: "100.5".into(),
                quantity: "0.3".into(),
                taker_side: "sell".into(),
                ts: 1_700_000_000_000,
                create_time: 1_700_000_000_500,
            }],
        };
        let evs =
            PoloniexNormalizer::default().normalize(PoloniexWssEvent::Trades(frame));
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            DomainEvent::Trade { trade, sequence } => {
                assert_eq!(trade.exchange, "poloniex");
                assert_eq!(trade.id, "42");
                assert!((decimal_to_f64(trade.price) - 100.5).abs() < 1e-9);
                assert!((decimal_to_f64(trade.amount) - 0.3).abs() < 1e-9);
                assert_eq!(trade.side, TradeSide::Sell);
                assert_eq!(trade.pair, TradingPair::new("BTC", "USDT"));
                // createTime (true match time) is preferred over ts (push time).
                assert_eq!(trade.source_trade_ts_us, 1_700_000_000_500_000);
                assert_eq!(*sequence, Some(42));
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn subscribe_emits_book_and_trade_frames() {
        let frames = PoloniexHooks
            .subscribe_frames(&["BTC_USDT".to_string()], &DeclaredSet::all());
        assert_eq!(frames.len(), 2);
        let Message::Text(book) = &frames[0] else {
            panic!("expected a text frame");
        };
        let v: serde_json::Value = serde_json::from_str(book).unwrap();
        assert_eq!(v["event"], "subscribe");
        assert_eq!(v["channel"][0], "book_lv2");
        assert_eq!(v["symbols"][0], "BTC_USDT");

        let Message::Text(trades) = &frames[1] else {
            panic!("expected a text frame");
        };
        let v: serde_json::Value = serde_json::from_str(trades).unwrap();
        assert_eq!(v["channel"][0], "trades");
    }

    #[test]
    fn decoder_routes_book_and_trades_skips_control() {
        let book = r#"{"channel":"book_lv2","data":[{"symbol":"BTC_USDT","asks":[],"bids":[],"id":100,"lastId":0}],"action":"snapshot"}"#;
        assert!(matches!(
            PoloniexDecoder::decode(book).unwrap(),
            Some(PoloniexWssEvent::Book(_))
        ));
        let trd = r#"{"channel":"trades","data":[{"symbol":"BTC_USDT","id":"1","price":"1","quantity":"1","takerSide":"buy","ts":1,"createTime":1}]}"#;
        assert!(matches!(
            PoloniexDecoder::decode(trd).unwrap(),
            Some(PoloniexWssEvent::Trades(_))
        ));
        // Subscribe ack + pong reply + connection heartbeat → no event.
        assert!(
            PoloniexDecoder::decode(r#"{"event":"subscribe","channel":["book_lv2"]}"#)
                .unwrap()
                .is_none()
        );
        assert!(
            PoloniexDecoder::decode(r#"{"event":"pong"}"#)
                .unwrap()
                .is_none()
        );
        assert!(
            PoloniexDecoder::decode(r#"{"channel":"heartbeat"}"#)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn profile_declares_exact_prev_over_wss_self_seed() {
        let a = PoloniexAdapter;
        assert_eq!(a.id(), "poloniex");
        assert_eq!(a.profile().id, "poloniex");
        assert_eq!(a.profile().protocol_revision, "poloniex-v2");
        assert!(matches!(
            a.profile().symbol_codec,
            SymbolCodec::Underscore { upper: true }
        ));
        assert!(matches!(
            a.book_model("orders"),
            ReconstructionModel::SeqDelta {
                predicate: SeqPredicate::ExactPrev,
                source: SnapshotSource::WssSelfSeed,
            }
        ));
    }
}
