//! Bybit v5 public-spot market-data adapter: subscribes to `orderbook.50` and
//! `publicTrade` streams and normalizes them into the framework's `DomainEvent`
//! stream. The `orderbook.50` channel sends a `type:"snapshot"` first frame then
//! `type:"delta"`s, so reconstruction is seeded in-band with no REST round-trip;
//! continuity rides the per-symbol `u` (update id) via the `RangeInclusive`
//! predicate.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::protocol::Message;

use aetelier_types::trades::{Trade, TradeSide};

use crate::framework::budget::{ConnectionBudget, SourceMetrics};
use crate::framework::driver::{DEFAULT_RAW_BUFFER, drive};
use crate::framework::model::{
    DomainEvent, Normalizer, ReconstructionModel, SeqPredicate, SnapshotSource,
};
use crate::framework::protocol::DeclaredSet;
use crate::framework::protocol::{AckOutcome, Heartbeat, ProtocolHooks};
use crate::framework::registry::{ExchangeAdapter, ExchangeProfile, TaskExit};
use crate::framework::symbol::SymbolCodec;
use crate::sources::bybit::decoder::BybitDecoder;
use crate::sources::bybit::events::BybitWssEvent;
use crate::sources::bybit::responses::BybitTradeData;

/// Bybit v5 public spot stream endpoint (no-auth; LOB + trades only).
const BYBIT_WSS_URL: &str = "wss://stream.bybit.com/v5/public/spot";

/// App-level `"ping"` cadence. Bybit closes a socket idle past its server
/// window; a 20s `{"op":"ping"}` (answered with `{"op":"pong"}`, swallowed by
/// the decoder's `op` dispatch) keeps it warm.
const PING_SECS: u64 = 20;

/// Orderbook depth tier subscribed (`orderbook.50` = top-50 incremental:
/// `type:"snapshot"` then `type:"delta"`).
const BOOK_DEPTH: u32 = 50;

/// Wire symbol codec — `BTCUSDT`.
const BYBIT_CODEC: SymbolCodec = SymbolCodec::Concat { upper: true };

// ─────────────────────────────────────────────────────────────────────────
// ProtocolHooks
// ─────────────────────────────────────────────────────────────────────────

/// Bybit WSS protocol behaviour. Overrides `endpoint`/`subscribe_frames`/
/// `heartbeat`; `prepare` stays the public-open no-op default.
pub struct BybitHooks;

impl ProtocolHooks for BybitHooks {
    fn endpoint(&self) -> String {
        BYBIT_WSS_URL.to_string()
    }

    /// One `subscribe` op; `args` fans out over `(symbol × {orderbook.50,
    /// publicTrade})`. `symbols` are venue wire symbols (`"BTCUSDT"`, Concat
    /// codec).
    fn subscribe_frames(
        &self,
        symbols: &[String],
        declared: &DeclaredSet,
    ) -> Vec<Message> {
        use aetelier_types::config::markets::market_config::DeclaredDatatype as DD;
        let mut args = Vec::with_capacity(symbols.len() * 2);
        for s in symbols {
            if declared.contains(DD::Orderbook) {
                args.push(format!("orderbook.{BOOK_DEPTH}.{s}"));
            }
            if declared.contains(DD::Trades) {
                args.push(format!("publicTrade.{s}"));
            }
        }
        if args.is_empty() {
            return Vec::new();
        }
        let frame = serde_json::json!({ "op": "subscribe", "args": args });
        vec![Message::Text(frame.to_string().into())]
    }

    fn heartbeat(&self) -> Heartbeat {
        Heartbeat::Text {
            every: Duration::from_secs(PING_SECS),
            payload: Arc::new(|| serde_json::json!({ "op": "ping" }).to_string()),
        }
    }

    /// Bybit answers `subscribe` with `{"success":bool,"op":"subscribe",
    /// "ret_msg":…}` — a `success:false` is a rejected subscription. Data
    /// frames carry `topic`/`data` and no `success`, so the substring guard
    /// keeps the hot path allocation-free.
    fn classify_ack(&self, text: &str) -> AckOutcome {
        if !text.contains("\"success\"") {
            return AckOutcome::NotAck;
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
            return AckOutcome::NotAck;
        };
        if v.get("op").and_then(|o| o.as_str()) != Some("subscribe") {
            return AckOutcome::NotAck;
        }
        match v.get("success").and_then(|s| s.as_bool()) {
            Some(true) => AckOutcome::Accepted,
            Some(false) => AckOutcome::Rejected(
                v.get("ret_msg")
                    .and_then(|m| m.as_str())
                    .unwrap_or("subscribe failed")
                    .to_string(),
            ),
            None => AckOutcome::NotAck,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Normalizer: decoded BybitWssEvent → DomainEvent
// ─────────────────────────────────────────────────────────────────────────

/// Maps a decoded [`BybitWssEvent`] to `DomainEvent`s. Derives the canonical
/// pair from the event's venue symbol (the socket is multi-symbol). Holds the
/// worker's shared [`SourceMetrics`] so every dropped event is counted, not
/// just logged.
#[derive(Default)]
pub struct BybitNormalizer {
    pub metrics: SourceMetrics,
}

impl Normalizer for BybitNormalizer {
    type Event = BybitWssEvent;

    fn normalize(&self, event: BybitWssEvent) -> Vec<DomainEvent> {
        match event {
            // LOB snapshot/delta. Bybit's `u` (update_id) increments by exactly
            // one per frame; `seq` is a global cross-sequence, NOT a prev-id
            // pointer. Set `sequence = update_id` so RangeInclusive enforces
            // `u == last + 1` (exact contiguity) and a real jump gaps.
            BybitWssEvent::OrderbookData(resp) => resp
                .to_normalized()
                .map(|mut d| {
                    d.sequence = d.update_id;
                    DomainEvent::Book(d)
                })
                .into_iter()
                .collect(),

            // A frame batches multiple fills; emit one Trade per fill (no
            // head-drop). An unparseable fill is dropped-and-counted by
            // `normalize_trade`.
            BybitWssEvent::TradeData(trades) => trades
                .into_iter()
                .filter_map(|t| normalize_trade(t, &self.metrics))
                .collect(),

            // Liquidations / tickers are out of the public-LOB+Trades scope; the
            // decoder may yield them, the normalizer drops them.
            BybitWssEvent::LiquidationData(_) | BybitWssEvent::TickerData(_) => {
                Vec::new()
            }
        }
    }
}

/// Map one decoded `publicTrade` fill to a `DomainEvent::Trade`. Bybit's `S` is
/// the taker side (`"Buy"`/`"Sell"`), parsed via `TradeSide::from_str_loose`.
/// Every dropped fill bumps `dropped_frames`.
fn normalize_trade(t: BybitTradeData, metrics: &SourceMetrics) -> Option<DomainEvent> {
    let Some(pair) = BYBIT_CODEC.decode(&t.symbol) else {
        tracing::warn!(symbol = %t.symbol, "bybit.trade.bad_symbol");
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
    let Some(side) = TradeSide::from_str_loose(&t.side) else {
        tracing::warn!(side = %t.side, id = %t.trade_id, "bybit.trade.unknown_side");
        metrics.add_dropped_frames(1);
        return None;
    };
    let trade = Trade {
        source_trade_ts_us: crate::framework::model::epoch_to_us(t.trade_ts),
        local_trade_ts_us: 0,
        source_trade_rtt_us: 0,
        pair,
        side,
        amount,
        price,
        exchange: "bybit".to_string(),
        id: t.trade_id.clone(),
        origin: Default::default(),
    };
    // Bybit's `publicTrade` trade id `i` is a per-symbol monotonic numeric
    // string (verified ascending +1 on live BTCUSDT capture, cycle #3 —
    // correcting the earlier assumption that it was an unusable UUID); arming
    // it feeds the SourcedTradebook loss accounting. Trades never gap/resync.
    let sequence = t.trade_id.parse::<u64>().ok();
    Some(DomainEvent::Trade { trade, sequence })
}

// ─────────────────────────────────────────────────────────────────────────
// ExchangeAdapter — registry entry
// ─────────────────────────────────────────────────────────────────────────

/// Static, data-only Bybit profile. Const-constructible so it lives in a
/// `static` (no init-time allocation: `Vec::new` is `const`).
static BYBIT_PROFILE: ExchangeProfile = ExchangeProfile {
    id: "bybit",
    symbol_codec: BYBIT_CODEC,
    budget: ConnectionBudget {
        // Bybit v5 public: many topics per socket; rate windows are enforced by
        // the planner.
        max_connections: None,
        max_streams_per_socket: None,
        subscribe_rate: Vec::new(),
        connect_attempt_rate: None,
        connection_lifetime: None,
    },
    schema_version: 1,
    protocol_revision: "bybit-v5",
};

/// The registry handle. Unit struct — all state is in the static profile.
pub struct BybitAdapter;

/// The single compiled-in Bybit instance (referenced by `register_all`).
pub static BYBIT: BybitAdapter = BybitAdapter;

impl ExchangeAdapter for BybitAdapter {
    fn id(&self) -> &'static str {
        "bybit"
    }

    fn profile(&self) -> &ExchangeProfile {
        &BYBIT_PROFILE
    }

    fn book_model(&self, _channel: &str) -> ReconstructionModel {
        // `orderbook.50`: incremental deltas, RangeInclusive continuity on the
        // `u` update id, seeded in-band by the channel's first `type:"snapshot"`
        // frame (no REST round-trip).
        ReconstructionModel::SeqDelta {
            predicate: SeqPredicate::RangeInclusive,
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
        tokio::spawn(drive::<BybitHooks, BybitDecoder, BybitNormalizer>(
            Arc::new(BybitHooks),
            symbols,
            declared,
            BybitNormalizer {
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
        BybitHooks
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
        let normalizer = BybitNormalizer {
            metrics: SourceMetrics::default(),
        };
        match BybitDecoder::decode(raw)? {
            Some(event) => Ok(normalizer.normalize(event)),
            None => Ok(Vec::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::wss::WssDecoder;
    use crate::sources::bybit::responses::orderbooks::BybitPriceLevel;
    use crate::sources::bybit::responses::{BybitOrderbookData, BybitOrderbookResponse};
    use aetelier_types::orderbooks::decimal_to_f64;
    use aetelier_types::trading_pair::TradingPair;

    fn trade(
        symbol: &str,
        id: &str,
        price: &str,
        amount: &str,
        side: &str,
    ) -> BybitTradeData {
        BybitTradeData {
            trade_ts: 1_672_304_484_978,
            symbol: symbol.into(),
            side: side.into(),
            amount: amount.into(),
            price: price.into(),
            direction: Some("PlusTick".into()),
            trade_id: id.into(),
            block_trade: false,
            rpi_trade: false,
            sequence: 7,
        }
    }

    fn book(symbol: &str, ty: &str, update_id: u64) -> BybitOrderbookResponse {
        BybitOrderbookResponse {
            topic: format!("orderbook.50.{symbol}"),
            ty: ty.into(),
            orderbook_ts_ms: 1_672_304_484_978,
            data: BybitOrderbookData {
                symbol: symbol.into(),
                bids: vec![BybitPriceLevel::new("100.0", "1.0")],
                asks: vec![BybitPriceLevel::new("101.0", "2.0")],
                update_id,
                sequence: 7_961_638_724,
            },
            cts: Some(1_672_304_484_998),
        }
    }

    #[test]
    fn normalizes_trade_to_domain_event() {
        let evs =
            BybitNormalizer::default().normalize(BybitWssEvent::TradeData(vec![trade(
                "BTCUSDT",
                "2290000000000054640",
                "16578.50",
                "0.141596",
                "Buy",
            )]));
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            DomainEvent::Trade { trade, sequence } => {
                assert_eq!(trade.exchange, "bybit");
                assert_eq!(trade.id, "2290000000000054640");
                assert!((decimal_to_f64(trade.price) - 16578.50).abs() < 1e-6);
                assert!((decimal_to_f64(trade.amount) - 0.141596).abs() < 1e-9);
                // Bybit `S` is the taker side directly.
                assert_eq!(trade.side, TradeSide::Buy);
                assert_eq!(trade.pair, TradingPair::new("BTC", "USDT"));
                // The numeric trade id arms the sequence (cycle #3).
                assert_eq!(*sequence, Some(2_290_000_000_000_054_640));
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn multi_fill_frame_yields_one_domain_event_per_fill_no_head_drop() {
        // A `publicTrade` frame carrying N (>=3) fills must yield N Trades.
        let fills = vec![
            trade("BTCUSDT", "1", "100.0", "0.1", "Buy"),
            trade("BTCUSDT", "2", "101.0", "0.2", "Sell"),
            trade("BTCUSDT", "3", "102.0", "0.3", "Buy"),
            trade("BTCUSDT", "4", "103.0", "0.4", "Sell"),
        ];
        let evs = BybitNormalizer::default().normalize(BybitWssEvent::TradeData(fills));
        assert_eq!(evs.len(), 4, "all fills must survive (no head-drop)");
        let ids: Vec<&str> = evs
            .iter()
            .map(|e| match e {
                DomainEvent::Trade { trade, .. } => trade.id.as_str(),
                other => panic!("expected Trade, got {other:?}"),
            })
            .collect();
        assert_eq!(ids, ["1", "2", "3", "4"]);
    }

    #[test]
    fn subscribe_ack_classification() {
        let h = BybitHooks;
        assert_eq!(
            h.classify_ack(r#"{"success":true,"op":"subscribe","conn_id":"x"}"#),
            AckOutcome::Accepted
        );
        assert_eq!(
            h.classify_ack(r#"{"success":false,"op":"subscribe","ret_msg":"bad topic"}"#),
            AckOutcome::Rejected("bad topic".into())
        );
        assert_eq!(
            h.classify_ack(r#"{"topic":"publicTrade.BTCUSDT","data":[]}"#),
            AckOutcome::NotAck
        );
        assert_eq!(
            h.classify_ack(r#"{"success":true,"op":"pong"}"#),
            AckOutcome::NotAck
        );
    }

    #[test]
    fn dropped_fill_bumps_the_shared_counter() {
        let n = BybitNormalizer::default();
        let evs = n.normalize(BybitWssEvent::TradeData(vec![
            trade("BTCUSDT", "1", "100.0", "0.1", "Buy"),
            trade("BTCUSDT", "2", "101.0", "0.2", "GARBAGE"),
        ]));
        assert_eq!(evs.len(), 1);
        assert_eq!(
            n.metrics.snapshot().dropped_frames,
            1,
            "the dropped fill is counted"
        );
    }

    #[test]
    fn unknown_side_trade_is_dropped_not_labelled_buy() {
        let evs = BybitNormalizer::default().normalize(BybitWssEvent::TradeData(vec![
            trade("BTCUSDT", "1", "100.0", "0.1", "Buy"),
            trade("BTCUSDT", "2", "101.0", "0.2", "GARBAGE"),
        ]));
        assert_eq!(
            evs.len(),
            1,
            "unknown-side trade must be dropped, not fabricated as Buy"
        );
        match &evs[0] {
            DomainEvent::Trade { trade, .. } => assert_eq!(trade.id, "1"),
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn decoder_preserves_all_fills_in_a_multi_fill_frame() {
        // End-to-end: a raw publicTrade frame with 3 fills decodes to a
        // TradeData(Vec) of 3.
        let raw = r#"{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1672304486868,"data":[
            {"T":1672304486865,"s":"BTCUSDT","S":"Buy","v":"0.001","p":"16578.5","i":"1","BT":false,"RPI":false,"seq":1},
            {"T":1672304486866,"s":"BTCUSDT","S":"Sell","v":"0.002","p":"16578.0","i":"2","BT":false,"RPI":false,"seq":2},
            {"T":1672304486867,"s":"BTCUSDT","S":"Buy","v":"0.003","p":"16578.5","i":"3","BT":false,"RPI":false,"seq":3}
        ]}"#;
        let BybitWssEvent::TradeData(trades) =
            BybitDecoder::decode(raw).unwrap().unwrap()
        else {
            panic!("expected TradeData");
        };
        assert_eq!(trades.len(), 3);
        let evs = BybitNormalizer::default().normalize(BybitWssEvent::TradeData(trades));
        assert_eq!(evs.len(), 3);
    }

    #[test]
    fn normalizes_snapshot_then_delta_book_events() {
        // First frame is the WSS-self-seed snapshot.
        let snap = BybitNormalizer::default().normalize(BybitWssEvent::OrderbookData(
            book("BTCUSDT", "snapshot", 18_521_288),
        ));
        assert_eq!(snap.len(), 1);
        match &snap[0] {
            DomainEvent::Book(nd) => {
                assert_eq!(nd.symbol, "BTCUSDT");
                assert_eq!(nd.update_id, 18_521_288);
                assert!(nd.is_snapshot);
            }
            other => panic!("expected Book, got {other:?}"),
        }

        // A later `delta` advances the `u` id; RangeInclusive continuity.
        let upd = BybitNormalizer::default().normalize(BybitWssEvent::OrderbookData(
            book("BTCUSDT", "delta", 18_521_289),
        ));
        match &upd[0] {
            DomainEvent::Book(nd) => {
                assert_eq!(nd.update_id, 18_521_289);
                assert!(!nd.is_snapshot);
            }
            other => panic!("expected Book, got {other:?}"),
        }
    }

    #[test]
    fn subscribe_frame_uses_op_args_for_orderbook_and_trades() {
        let frames =
            BybitHooks.subscribe_frames(&["BTCUSDT".to_string()], &DeclaredSet::all());
        assert_eq!(frames.len(), 1);
        let Message::Text(body) = &frames[0] else {
            panic!("expected a text frame");
        };
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(v["op"], "subscribe");
        let args = v["args"].as_array().unwrap();
        assert_eq!(args.len(), 2);
        assert_eq!(args[0], "orderbook.50.BTCUSDT");
        assert_eq!(args[1], "publicTrade.BTCUSDT");
    }

    #[test]
    fn decoder_dispatches_trade_frame_through_normalizer() {
        // End-to-end: the reused decoder routes a `publicTrade` frame to the
        // event the normalizer consumes.
        let raw = r#"{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1672304484978,"data":[{"T":1672304484978,"s":"BTCUSDT","S":"Sell","v":"0.001","p":"16578.50","L":"PlusTick","i":"2290000000000054640","BT":false,"RPI":false,"seq":42}]}"#;
        let ev = BybitDecoder::decode(raw)
            .expect("decode ok")
            .expect("event");
        let evs = BybitNormalizer::default().normalize(ev);
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            DomainEvent::Trade { trade, .. } => {
                assert_eq!(trade.side, TradeSide::Sell);
                assert_eq!(trade.exchange, "bybit");
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn decodes_spot_trade_frame_without_tick_direction() {
        // Regression: real Bybit spot `publicTrade` frames omit `L` (tick
        // direction). The decoder must still yield the trade instead of
        // failing with `missing field 'L'` and dropping every fill.
        let raw = r#"{"topic":"publicTrade.BTCUSDT","ts":1781885870260,"type":"snapshot","data":[{"i":"2290000001162605275","T":1781885870259,"p":"63311.4","v":"0.000001","S":"Buy","seq":109888984948,"s":"BTCUSDT","BT":false,"RPI":false}]}"#;
        let ev = BybitDecoder::decode(raw)
            .expect("decode ok")
            .expect("event");
        let evs = BybitNormalizer::default().normalize(ev);
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            DomainEvent::Trade { trade, .. } => {
                assert_eq!(trade.side, TradeSide::Buy);
                assert_eq!(trade.id, "2290000001162605275");
                assert!((decimal_to_f64(trade.price) - 63311.4).abs() < 1e-6);
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn profile_and_book_model_match_the_bybit_row() {
        let a = BybitAdapter;
        assert_eq!(a.id(), "bybit");
        assert_eq!(a.profile().id, "bybit");
        assert_eq!(a.profile().protocol_revision, "bybit-v5");
        assert!(matches!(
            a.profile().symbol_codec,
            SymbolCodec::Concat { upper: true }
        ));
        assert!(matches!(
            a.book_model("orderbook"),
            ReconstructionModel::SeqDelta {
                predicate: SeqPredicate::RangeInclusive,
                source: SnapshotSource::WssSelfSeed,
            }
        ));
    }
}
