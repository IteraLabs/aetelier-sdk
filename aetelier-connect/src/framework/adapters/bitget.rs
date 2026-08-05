//! Bitget V2 spot public market-data adapter: subscribes to `books` + `trade`
//! channels over a single WSS socket and normalizes them into the framework's
//! `DomainEvent` stream.
//!
//! Transport notes:
//! - One socket, many symbols: a single `subscribe` op whose `args` fans out
//!   over `(symbol × {books, trade})` with `instType:"SPOT"`.
//! - Wire symbols are upper concat (`BTCUSDT`).
//! - Keep-alive is a client-initiated literal `"ping"` every 25s; Bitget replies
//!   with the literal `"pong"`, which the decoder swallows as `Ok(None)`.
//! - Book frames carry `action:"snapshot"|"update"` + a `seq`/`pseq` pair; the
//!   model is `SeqDelta { ExactPrev, WssSelfSeed }` (prev-id continuity).

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

use crate::sources::bitget::decoder::BitgetDecoder;
use crate::sources::bitget::events::BitgetWssEvent;
use crate::sources::bitget::responses::BitgetBookData;

/// Bitget V2 public (no-auth) spot endpoint.
const BITGET_WSS_URL: &str = "wss://ws.bitget.com/v2/ws/public";

/// App-level `"ping"` cadence. Bitget closes a socket idle past 30s; a 25s text
/// ping (answered with the literal `"pong"`, swallowed by the decoder) keeps it
/// warm.
const PING_SECS: u64 = 25;

/// Spot instrument type for every subscribe `arg`.
const INST_TYPE: &str = "SPOT";

/// Incremental L2 channel (carries `action` + a `seq`/`pseq` pair), the channel
/// whose reconstruction model is `SeqDelta { ExactPrev, WssSelfSeed }`.
const BOOK_CHANNEL: &str = "books";

/// Wire symbol codec — `BTCUSDT`.
const BITGET_CODEC: SymbolCodec = SymbolCodec::Concat { upper: true };

// ─────────────────────────────────────────────────────────────────────────
// ProtocolHooks — WSS endpoint, subscribe frames, heartbeat
// ─────────────────────────────────────────────────────────────────────────

/// Bitget WSS protocol behaviour. Overrides `endpoint`/`subscribe_frames`/
/// `heartbeat`; `prepare` stays the public-open no-op default.
pub struct BitgetHooks;

impl ProtocolHooks for BitgetHooks {
    fn endpoint(&self) -> String {
        BITGET_WSS_URL.to_string()
    }

    /// One `subscribe` op; `args` fans out over `(symbol × {books, trade})`,
    /// each tagged `instType:"SPOT"`. `symbols` are venue wire symbols
    /// (`"BTCUSDT"`, Concat codec).
    fn subscribe_frames(
        &self,
        symbols: &[String],
        _declared: &DeclaredSet,
    ) -> Vec<Message> {
        let mut args = Vec::with_capacity(symbols.len() * 2);
        for s in symbols {
            args.push(serde_json::json!({
                "instType": INST_TYPE, "channel": BOOK_CHANNEL, "instId": s
            }));
            args.push(serde_json::json!({
                "instType": INST_TYPE, "channel": "trade", "instId": s
            }));
        }
        let frame = serde_json::json!({ "op": "subscribe", "args": args });
        vec![Message::Text(frame.to_string().into())]
    }

    fn heartbeat(&self) -> Heartbeat {
        Heartbeat::Text {
            every: Duration::from_secs(PING_SECS),
            payload: Arc::new(|| "ping".to_string()),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Normalizer — decoded BitgetWssEvent → DomainEvent
// ─────────────────────────────────────────────────────────────────────────

/// Maps a decoded `BitgetWssEvent` to `DomainEvent`s. Holds the worker's
/// shared [`SourceMetrics`] so every dropped event is counted, not just
/// logged.
#[derive(Default)]
pub struct BitgetNormalizer {
    pub metrics: SourceMetrics,
}

impl BitgetNormalizer {
    /// Build a `NormalizedDelta` from one Bitget book payload. `update_id` is the
    /// venue `seq` (else the millisecond `ts`); `sequence` carries `pseq`, the
    /// prev-id continuity pointer; `is_snapshot` is gated by the frame `action`.
    fn to_delta(
        symbol: &str,
        data: &BitgetBookData,
        is_snapshot: bool,
    ) -> NormalizedDelta {
        let levels = |side: &[[String; 2]]| {
            side.iter()
                .map(|l| (l[0].clone(), l[1].clone()))
                .collect::<Vec<_>>()
        };
        let update_id = data
            .seq
            .unwrap_or_else(|| data.ts.parse::<u64>().unwrap_or(0));
        NormalizedDelta {
            symbol: symbol.to_string(),
            bids: levels(&data.bids),
            asks: levels(&data.asks),
            update_id,
            sequence: data.pseq.unwrap_or(0),
            source_orderbook_ts_us: epoch_to_us(data.ts.parse::<u64>().unwrap_or(0)),
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            checksum: None,
            orders: Vec::new(),
            is_snapshot,
        }
    }
}

impl Normalizer for BitgetNormalizer {
    type Event = BitgetWssEvent;

    fn normalize(&self, event: BitgetWssEvent) -> Vec<DomainEvent> {
        match event {
            BitgetWssEvent::Book(frame) => {
                let is_snapshot = frame.action == "snapshot";
                let symbol = frame.arg.inst_id;
                frame
                    .data
                    .iter()
                    .map(|d| DomainEvent::Book(Self::to_delta(&symbol, d, is_snapshot)))
                    .collect()
            }
            BitgetWssEvent::Trade(frame) => {
                let Some(pair) = BITGET_CODEC.decode(&frame.arg.inst_id) else {
                    tracing::warn!(symbol = %frame.arg.inst_id, "bitget.trade.bad_symbol");
                    self.metrics.add_dropped_frames(1);
                    return Vec::new();
                };
                frame
                    .data
                    .into_iter()
                    .filter_map(|d| {
                        let (Ok(amount), Ok(price)) = (
                            d.size.parse::<rust_decimal::Decimal>(),
                            d.price.parse::<rust_decimal::Decimal>(),
                        ) else {
                            self.metrics.add_dropped_frames(1);
                            return None;
                        };
                        let trade_ts = epoch_to_us(d.ts.parse::<u64>().unwrap_or(0));
                        // Drop (never fabricate as Buy) an unknown taker side so
                        // it can't pollute the side distribution.
                        let Some(side) = TradeSide::from_str_loose(&d.side) else {
                            tracing::warn!(
                                side = %d.side,
                                id = %d.trade_id,
                                "bitget.trade.unknown_side"
                            );
                            self.metrics.add_dropped_frames(1);
                            return None;
                        };
                        Some(DomainEvent::Trade {
                            trade: Trade {
                                source_trade_ts_us: trade_ts,
                                local_trade_ts_us: 0,
                                source_trade_rtt_us: 0,
                                pair: pair.clone(),
                                side,
                                amount,
                                price,
                                exchange: "bitget".to_string(),
                                id: d.trade_id,
                                origin: Default::default(),
                            },
                            // NOT armable (cycle #7 density check on live
                            // data): `tradeId` is a ~2^60 snowflake-style id
                            // with huge variable strides, and the payload
                            // carries no other sequence field — arming it
                            // would fabricate `trades_lost`. Best-effort.
                            sequence: None,
                        })
                    })
                    .collect()
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// ExchangeAdapter — registry entry
// ─────────────────────────────────────────────────────────────────────────

/// Static, data-only Bitget profile.
static BITGET_PROFILE: ExchangeProfile = ExchangeProfile {
    id: "bitget",
    symbol_codec: BITGET_CODEC,
    budget: ConnectionBudget {
        // Bitget public: many channels per socket.
        max_connections: None,
        max_streams_per_socket: None,
        subscribe_rate: Vec::new(),
        connect_attempt_rate: None,
        connection_lifetime: None,
    },
    schema_version: 1,
    protocol_revision: "bitget-v2",
};

/// The registry handle. Unit struct — all state is in the static profile.
pub struct BitgetAdapter;

/// The single compiled-in Bitget instance (referenced by `register_all`).
pub static BITGET: BitgetAdapter = BitgetAdapter;

impl ExchangeAdapter for BitgetAdapter {
    fn id(&self) -> &'static str {
        "bitget"
    }

    fn profile(&self) -> &ExchangeProfile {
        &BITGET_PROFILE
    }

    fn book_model(&self, _channel: &str) -> ReconstructionModel {
        // Incremental `books`: prev-id continuity via seq/pseq, self-seeded by
        // the first `action:"snapshot"` frame (the classic CRC32 channel was
        // retired in favour of seq/pseq).
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
        tokio::spawn(drive::<BitgetHooks, BitgetDecoder, BitgetNormalizer>(
            Arc::new(BitgetHooks),
            symbols,
            declared,
            BitgetNormalizer {
                metrics: metrics.clone(),
            },
            tx,
            shutdown,
            DEFAULT_RAW_BUFFER,
            metrics,
        ))
    }

    fn replay_frame(
        &self,
        raw: &str,
    ) -> Result<Vec<DomainEvent>, Box<crate::errors::ExchangeError>> {
        use crate::clients::wss::WssDecoder;
        let normalizer = BitgetNormalizer {
            metrics: SourceMetrics::default(),
        };
        match BitgetDecoder::decode(raw)? {
            Some(event) => Ok(normalizer.normalize(event)),
            None => Ok(Vec::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::wss::WssDecoder;
    use crate::sources::bitget::responses::{
        BitgetArg, BitgetBookFrame, BitgetTradeData, BitgetTradeFrame,
    };
    use aetelier_types::orderbooks::decimal_to_f64;

    #[test]
    fn normalizes_snapshot_book_with_seq_update_id() {
        let frame = BitgetBookFrame {
            action: "snapshot".into(),
            arg: BitgetArg {
                channel: "books".into(),
                inst_id: "BTCUSDT".into(),
            },
            data: vec![BitgetBookData {
                asks: vec![["101.0".into(), "2.0".into()]],
                bids: vec![["100.0".into(), "1.0".into()]],
                ts: "1700000000000".into(),
                seq: Some(123),
                pseq: Some(0),
            }],
        };
        let evs = BitgetNormalizer::default().normalize(BitgetWssEvent::Book(frame));
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            DomainEvent::Book(nd) => {
                assert_eq!(nd.symbol, "BTCUSDT");
                assert_eq!(nd.update_id, 123); // seq wins over ts
                assert_eq!(nd.bids, vec![("100.0".to_string(), "1.0".to_string())]);
                assert!(nd.is_snapshot);
            }
            other => panic!("expected Book, got {other:?}"),
        }
    }

    #[test]
    fn update_book_falls_back_to_ts_and_is_not_snapshot() {
        let frame = BitgetBookFrame {
            action: "update".into(),
            arg: BitgetArg {
                channel: "books".into(),
                inst_id: "BTCUSDT".into(),
            },
            data: vec![BitgetBookData {
                asks: vec![],
                bids: vec![["100.0".into(), "3.0".into()]],
                ts: "1700000000001".into(),
                seq: None,
                pseq: Some(122),
            }],
        };
        let evs = BitgetNormalizer::default().normalize(BitgetWssEvent::Book(frame));
        match &evs[0] {
            DomainEvent::Book(nd) => {
                assert_eq!(nd.update_id, 1_700_000_000_001); // ts fallback
                assert_eq!(nd.sequence, 122); // pseq pointer
                assert!(!nd.is_snapshot);
            }
            other => panic!("expected Book, got {other:?}"),
        }
    }

    #[test]
    fn normalizes_trade_taker_side() {
        let frame = BitgetTradeFrame {
            arg: BitgetArg {
                channel: "trade".into(),
                inst_id: "BTCUSDT".into(),
            },
            data: vec![BitgetTradeData {
                ts: "1700000000000".into(),
                price: "31684.5".into(),
                size: "0.3".into(),
                side: "sell".into(),
                trade_id: "42".into(),
            }],
        };
        let evs = BitgetNormalizer::default().normalize(BitgetWssEvent::Trade(frame));
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            DomainEvent::Trade { trade, sequence } => {
                assert_eq!(trade.exchange, "bitget");
                assert_eq!(trade.id, "42");
                assert_eq!(trade.side, TradeSide::Sell);
                assert!((decimal_to_f64(trade.price) - 31684.5).abs() < 1e-6);
                assert!((decimal_to_f64(trade.amount) - 0.3).abs() < 1e-9);
                assert_eq!(trade.pair.base(), "BTC");
                assert_eq!(trade.pair.quote(), "USDT");
                assert!(sequence.is_none());
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn subscribe_frame_uses_op_args_for_books_and_trade() {
        let frames =
            BitgetHooks.subscribe_frames(&["BTCUSDT".to_string()], &DeclaredSet::all());
        assert_eq!(frames.len(), 1);
        let Message::Text(body) = &frames[0] else {
            panic!("expected a text frame");
        };
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(v["op"], "subscribe");
        let args = v["args"].as_array().unwrap();
        assert_eq!(args.len(), 2);
        assert_eq!(args[0]["instType"], "SPOT");
        assert_eq!(args[0]["channel"], "books");
        assert_eq!(args[0]["instId"], "BTCUSDT");
        assert_eq!(args[1]["channel"], "trade");
    }

    #[test]
    fn decoder_routes_book_trade_and_swallows_control() {
        let book = r#"{"action":"snapshot","arg":{"instType":"SPOT","channel":"books","instId":"BTCUSDT"},"data":[{"asks":[],"bids":[],"checksum":0,"ts":"1700000000000"}]}"#;
        assert!(matches!(
            BitgetDecoder::decode(book).unwrap(),
            Some(BitgetWssEvent::Book(_))
        ));
        let trade = r#"{"action":"snapshot","arg":{"instType":"SPOT","channel":"trade","instId":"BTCUSDT"},"data":[{"ts":"1700000000000","price":"1","size":"1","side":"buy","tradeId":"1"}]}"#;
        assert!(matches!(
            BitgetDecoder::decode(trade).unwrap(),
            Some(BitgetWssEvent::Trade(_))
        ));
        // Subscribe ack (event frame) → no event.
        let ack = r#"{"event":"subscribe","arg":{"instType":"SPOT","channel":"books","instId":"BTCUSDT"}}"#;
        assert!(BitgetDecoder::decode(ack).unwrap().is_none());
        // Literal pong keep-alive reply → no event.
        assert!(BitgetDecoder::decode("pong").unwrap().is_none());
    }

    #[test]
    fn profile_declares_exact_prev_self_seeded_model() {
        let a = BitgetAdapter;
        assert_eq!(a.id(), "bitget");
        assert!(matches!(
            a.profile().symbol_codec,
            SymbolCodec::Concat { upper: true }
        ));
        assert_eq!(a.profile().id, "bitget");
        assert_eq!(a.profile().protocol_revision, "bitget-v2");
        assert!(matches!(
            a.book_model("orders"),
            ReconstructionModel::SeqDelta {
                predicate: SeqPredicate::ExactPrev,
                source: SnapshotSource::WssSelfSeed,
            }
        ));
    }
}
