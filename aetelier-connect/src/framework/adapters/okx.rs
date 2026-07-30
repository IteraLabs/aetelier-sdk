//! OKX V5 spot public market-data adapter: subscribes to the incremental
//! `books` and `trades` channels and normalizes them into the framework's
//! `DomainEvent` stream. Uses a `{op, args:[{channel, instId}]}` subscribe
//! frame, an app-level `"ping"`/`"pong"` text heartbeat, and `ChecksumDelta`
//! book reconstruction over the [`OkxDecoder`] wire types.

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
};
use crate::framework::protocol::{AckOutcome, Heartbeat, ProtocolHooks};
use crate::framework::registry::{ExchangeAdapter, ExchangeProfile, TaskExit};
use crate::framework::symbol::SymbolCodec;
use crate::sources::okx::decoder::OkxDecoder;
use crate::sources::okx::events::OkxWssEvent;
use crate::sources::okx::responses::{OkxOrderbookData, OkxTradeData};

/// OKX V5 public (no-auth) endpoint.
const OKX_WSS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";

/// App-level `"ping"` cadence. OKX closes a socket idle past 30s; a 20s text
/// ping (answered with `"pong"`, swallowed by the decoder) keeps it warm.
const PING_SECS: u64 = 20;

/// Incremental L2 channel (carries `action` + `seqId`/`prevSeqId` + CRC32),
/// reconstructed with the `ChecksumDelta` model.
const BOOK_CHANNEL: &str = "books";

/// Wire symbol codec — `BTC-USDT`.
const OKX_CODEC: SymbolCodec = SymbolCodec::Hyphen;

// ─────────────────────────────────────────────────────────────────────────
// ProtocolHooks
// ─────────────────────────────────────────────────────────────────────────

pub struct OkxHooks;

impl ProtocolHooks for OkxHooks {
    fn endpoint(&self) -> String {
        OKX_WSS_URL.to_string()
    }

    /// One `subscribe` op; `args` fans out over `(symbol × {books, trades})`.
    /// `symbols` are venue wire symbols (`"BTC-USDT"`, Hyphen codec).
    fn subscribe_frames(&self, symbols: &[String]) -> Vec<Message> {
        let mut args = Vec::with_capacity(symbols.len() * 2);
        for s in symbols {
            args.push(serde_json::json!({ "channel": BOOK_CHANNEL, "instId": s }));
            args.push(serde_json::json!({ "channel": "trades", "instId": s }));
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

    /// OKX answers `subscribe` with `{"event":"subscribe","arg":{…}}` and
    /// rejects with `{"event":"error","code":…,"msg":…}`. Data frames carry
    /// `arg`/`data` and no `event`, so the substring guard keeps the hot path
    /// allocation-free.
    fn classify_ack(&self, text: &str) -> AckOutcome {
        if !text.contains("\"event\"") {
            return AckOutcome::NotAck;
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
            return AckOutcome::NotAck;
        };
        match v.get("event").and_then(|e| e.as_str()) {
            Some("subscribe") => AckOutcome::Accepted,
            Some("error") => AckOutcome::Rejected(
                v.get("msg")
                    .and_then(|m| m.as_str())
                    .unwrap_or("stream error")
                    .to_string(),
            ),
            _ => AckOutcome::NotAck,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Normalizer — decoded OkxWssEvent → DomainEvent
// ─────────────────────────────────────────────────────────────────────────

/// Maps a decoded [`OkxWssEvent`] to `DomainEvent`s. Holds the worker's shared
/// [`SourceMetrics`] so every dropped event is counted, not just logged.
#[derive(Default)]
pub struct OkxNormalizer {
    pub metrics: SourceMetrics,
}

impl OkxNormalizer {
    /// Build a `NormalizedDelta` from one OKX book payload. `update_id` is the
    /// current `seqId`; `sequence` is `prevSeqId` (the continuity predecessor,
    /// `-1`→`0` on the first snapshot). Negative ids clamp to `0`.
    fn to_delta(
        symbol: &str,
        data: &OkxOrderbookData,
        is_snapshot: bool,
    ) -> NormalizedDelta {
        let levels = |side: &[crate::sources::okx::responses::OkxLevel]| {
            side.iter()
                .map(|l| (l.price_str().to_string(), l.size_str().to_string()))
                .collect::<Vec<_>>()
        };
        NormalizedDelta {
            symbol: symbol.to_string(),
            bids: levels(&data.bids),
            asks: levels(&data.asks),
            update_id: data.seq_id.max(0) as u64,
            sequence: data.prev_seq_id.unwrap_or(data.seq_id).max(0) as u64,
            source_orderbook_ts_us: crate::framework::model::epoch_to_us(data.ts_ms()),
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            checksum: data.checksum,
            orders: Vec::new(),
            is_snapshot,
        }
    }
}

impl Normalizer for OkxNormalizer {
    type Event = OkxWssEvent;

    fn normalize(&self, event: OkxWssEvent) -> Vec<DomainEvent> {
        match event {
            OkxWssEvent::OrderbookData(resp) => {
                let is_snapshot = resp.is_snapshot();
                let symbol = resp.symbol().to_string();
                resp.data
                    .iter()
                    .map(|d| DomainEvent::Book(Self::to_delta(&symbol, d, is_snapshot)))
                    .collect()
            }
            // A `trades` push may batch multiple prints; emit one Trade per
            // print rather than only the first (no head-drop).
            OkxWssEvent::TradeData(trades) => trades
                .into_iter()
                .filter_map(|t| normalize_trade(t, &self.metrics))
                .collect(),
        }
    }
}

/// Map one decoded `trades` print to a `DomainEvent::Trade`. Every dropped
/// print bumps `dropped_frames`.
fn normalize_trade(t: OkxTradeData, metrics: &SourceMetrics) -> Option<DomainEvent> {
    let Some(pair) = OKX_CODEC.decode(&t.inst_id) else {
        tracing::warn!(symbol = %t.inst_id, "okx.trade.bad_symbol");
        metrics.add_dropped_frames(1);
        return None;
    };
    let (Ok(amount), Ok(price)) = (
        t.sz.parse::<rust_decimal::Decimal>(),
        t.px.parse::<rust_decimal::Decimal>(),
    ) else {
        metrics.add_dropped_frames(1);
        return None;
    };
    // Drop (never fabricate as Buy) an unknown taker side so it can't pollute
    // the side distribution.
    let Some(side) = TradeSide::from_str_loose(&t.side) else {
        tracing::warn!(side = %t.side, id = %t.trade_id, "okx.trade.unknown_side");
        metrics.add_dropped_frames(1);
        return None;
    };
    let trade = Trade {
        source_trade_ts_us: crate::framework::model::epoch_to_us(t.ts_ms()),
        local_trade_ts_us: 0,
        source_trade_rtt_us: 0,
        pair,
        side,
        amount,
        price,
        exchange: "okx".to_string(),
        id: t.trade_id.clone(),
        origin: Default::default(),
    };
    // OKX `trades` carries a per-instrument monotonic numeric `tradeId`
    // (verified ascending +1 on live BTC-USDT capture, cycle #2 trade
    // completion); arming it feeds the SourcedTradebook loss accounting.
    // Trades never gap/resync (no re-seed).
    let sequence = t.trade_id.parse::<u64>().ok();
    Some(DomainEvent::Trade { trade, sequence })
}

// ─────────────────────────────────────────────────────────────────────────
// ExchangeAdapter — registry entry
// ─────────────────────────────────────────────────────────────────────────

static OKX_PROFILE: ExchangeProfile = ExchangeProfile {
    id: "okx",
    symbol_codec: OKX_CODEC,
    budget: ConnectionBudget {
        // OKX public: ~3 connect attempts/s per IP; many channels per socket.
        max_connections: None,
        max_streams_per_socket: None,
        subscribe_rate: Vec::new(),
        connect_attempt_rate: None,
        connection_lifetime: None,
    },
    schema_version: 1,
    protocol_revision: "okx-v5",
};

pub struct OkxAdapter;

/// The single compiled-in OKX instance (referenced by `register_all`).
pub static OKX: OkxAdapter = OkxAdapter;

impl ExchangeAdapter for OkxAdapter {
    fn id(&self) -> &'static str {
        "okx"
    }

    fn profile(&self) -> &ExchangeProfile {
        &OKX_PROFILE
    }

    fn book_model(&self, _channel: &str) -> ReconstructionModel {
        // Incremental `books`: reconstructed by the `seqId`/`prevSeqId`
        // continuity (ExactPrev — each update's prevSeqId equals the prior
        // seqId), self-seeded from the first `action:snapshot`.
        //
        // OKX has DEPRECATED the books-channel CRC32 checksum: it now streams
        // `checksum: 0` on every frame (verified on a live 2026-07-15
        // BTC-USDT capture — all-zero, vs the real non-zero checksums in the
        // earlier okx_books fixture). The prior ChecksumDelta{OkxTop25} model
        // validated against that checksum and would gap-loop against a 0, so
        // reconstruction moves to the sequence continuity the venue still
        // guarantees. The OkxTop25 CRC32 recipe stays in `checksum.rs` (its
        // algorithm test still validates against the known-good older frames).
        ReconstructionModel::SeqDelta {
            predicate: SeqPredicate::ExactPrev,
            source: SnapshotSource::WssSelfSeed,
        }
    }

    fn spawn(
        &self,
        symbols: Vec<String>,
        tx: mpsc::Sender<DomainEvent>,
        shutdown: watch::Receiver<bool>,
        metrics: SourceMetrics,
    ) -> JoinHandle<TaskExit> {
        tokio::spawn(drive::<OkxHooks, OkxDecoder, OkxNormalizer>(
            Arc::new(OkxHooks),
            symbols,
            OkxNormalizer {
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
        let normalizer = OkxNormalizer {
            metrics: SourceMetrics::default(),
        };
        match OkxDecoder::decode(raw)? {
            Some(event) => Ok(normalizer.normalize(event)),
            None => Ok(Vec::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::okx::responses::{OkxLevel, OkxOrderbookResponse};
    use aetelier_types::orderbooks::decimal_to_f64;

    #[test]
    fn subscribe_ack_classification() {
        let h = OkxHooks;
        assert_eq!(
            h.classify_ack(
                r#"{"event":"subscribe","arg":{"channel":"books","instId":"BTC-USDT"}}"#
            ),
            AckOutcome::Accepted
        );
        assert_eq!(
            h.classify_ack(r#"{"event":"error","code":"60012","msg":"Invalid request"}"#),
            AckOutcome::Rejected("Invalid request".into())
        );
        assert_eq!(
            h.classify_ack(
                r#"{"arg":{"channel":"books","instId":"BTC-USDT"},"data":[]}"#
            ),
            AckOutcome::NotAck
        );
    }

    fn level(p: &str, s: &str) -> OkxLevel {
        OkxLevel(p.into(), s.into(), "0".into(), "1".into())
    }

    fn book_data(seq: i64, prev: Option<i64>) -> OkxOrderbookData {
        OkxOrderbookData {
            asks: vec![level("101.0", "2.0")],
            bids: vec![level("100.0", "1.0")],
            ts: "1626537446491".into(),
            seq_id: seq,
            prev_seq_id: prev,
            checksum: Some(123),
        }
    }

    #[test]
    fn normalizes_incremental_book_update() {
        let resp = OkxOrderbookResponse {
            arg: crate::sources::okx::responses::OkxArg {
                channel: "books".into(),
                inst_id: "BTC-USDT".into(),
            },
            action: Some("update".into()),
            data: vec![book_data(1235, Some(1234))],
        };
        let evs = OkxNormalizer::default().normalize(OkxWssEvent::OrderbookData(resp));
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            DomainEvent::Book(nd) => {
                assert_eq!(nd.symbol, "BTC-USDT");
                assert_eq!(nd.update_id, 1235);
                assert_eq!(nd.sequence, 1234);
                assert!(!nd.is_snapshot);
            }
            other => panic!("expected Book, got {other:?}"),
        }
    }

    #[test]
    fn normalizes_trade() {
        let t = OkxTradeData {
            inst_id: "BTC-USDT".into(),
            trade_id: "216970876".into(),
            px: "31684.5".into(),
            sz: "0.00001186".into(),
            side: "sell".into(),
            ts: "1626531038288".into(),
        };
        let evs = OkxNormalizer::default().normalize(OkxWssEvent::TradeData(vec![t]));
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            DomainEvent::Trade { trade, .. } => {
                assert_eq!(trade.exchange, "okx");
                assert_eq!(trade.id, "216970876");
                assert_eq!(trade.side, TradeSide::Sell);
                assert!((decimal_to_f64(trade.price) - 31684.5).abs() < 1e-6);
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    fn trade_data(id: &str, side: &str) -> OkxTradeData {
        OkxTradeData {
            inst_id: "BTC-USDT".into(),
            trade_id: id.into(),
            px: "31684.5".into(),
            sz: "0.00001186".into(),
            side: side.into(),
            ts: "1626531038288".into(),
        }
    }

    #[test]
    fn multi_trade_push_yields_one_domain_event_per_print_no_head_drop() {
        // An OKX `trades` push batches prints in `data` — all must survive.
        let evs = OkxNormalizer::default().normalize(OkxWssEvent::TradeData(vec![
            trade_data("1", "buy"),
            trade_data("2", "sell"),
            trade_data("3", "buy"),
        ]));
        assert_eq!(evs.len(), 3, "all prints must survive (no head-drop)");
        let ids: Vec<&str> = evs
            .iter()
            .map(|e| match e {
                DomainEvent::Trade { trade, .. } => trade.id.as_str(),
                other => panic!("expected Trade, got {other:?}"),
            })
            .collect();
        assert_eq!(ids, ["1", "2", "3"]);
    }

    #[test]
    fn unknown_side_trade_is_dropped_not_labelled_buy() {
        let evs = OkxNormalizer::default().normalize(OkxWssEvent::TradeData(vec![
            trade_data("1", "buy"),
            trade_data("2", "garbage"),
        ]));
        assert_eq!(evs.len(), 1, "unknown-side trade must be dropped");
        match &evs[0] {
            DomainEvent::Trade { trade, .. } => assert_eq!(trade.id, "1"),
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn subscribe_frame_uses_op_args_for_books_and_trades() {
        let frames = OkxHooks.subscribe_frames(&["BTC-USDT".to_string()]);
        assert_eq!(frames.len(), 1);
        let Message::Text(body) = &frames[0] else {
            panic!("expected a text frame");
        };
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(v["op"], "subscribe");
        let args = v["args"].as_array().unwrap();
        assert_eq!(args.len(), 2);
        assert_eq!(args[0]["channel"], "books");
        assert_eq!(args[0]["instId"], "BTC-USDT");
        assert_eq!(args[1]["channel"], "trades");
    }

    #[test]
    fn profile_and_seq_model() {
        let a = OkxAdapter;
        assert_eq!(a.id(), "okx");
        assert!(matches!(a.profile().symbol_codec, SymbolCodec::Hyphen));
        // OKX reconstructs by seqId/prevSeqId continuity (ExactPrev), self-
        // seeded — the books-channel CRC32 checksum was deprecated by OKX.
        assert!(matches!(
            a.book_model("orders"),
            ReconstructionModel::SeqDelta {
                predicate: SeqPredicate::ExactPrev,
                source: SnapshotSource::WssSelfSeed,
            }
        ));
    }
}
