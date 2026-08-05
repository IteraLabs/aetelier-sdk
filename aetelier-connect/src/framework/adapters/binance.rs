//! Binance spot public market-data adapter: subscribes to depth + trade streams
//! and normalizes decoded frames into the framework's `DomainEvent` stream.
//!
//! It composes the
//! [`crate::sources::binance::decoder::BinanceDecoder`] and the
//! `BinanceDepthUpdate`/`BinanceTradeData` wire types behind the framework
//! seams; only the per-venue protocol (subscribe/heartbeat) and normalization
//! (decoded → `DomainEvent`) live here.
//!
//! Transport notes:
//! - One socket, many symbols, `{"method":"SUBSCRIBE","params":[…],"id":1}`.
//! - Stream names are lowercase: `<sym>@depth@100ms` (LOB) + `<sym>@trade`.
//! - Keep-alive is a WS-protocol `Pong` every 15s (no app-level ping frame).
//! - Reconstruction is `SeqDelta { RangeInclusive }` seeded by a REST snapshot
//!   (`needs_rest`).

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
use crate::sources::binance::decoder::BinanceDecoder;
use crate::sources::binance::events::BinanceWssEvent;

/// Combined-stream endpoint (raw streams, no `/stream?streams=` envelope —
/// frames arrive bare, matching `BinanceDecoder`'s `"e"`-field dispatch).
const BINANCE_WSS_URL: &str = "wss://stream.binance.com:9443/ws";

/// WS-protocol `Pong` cadence. Binance disconnects a socket that is silent past
/// its server ping window; a client-initiated `Pong` keeps it warm.
const KEEPALIVE_SECS: u64 = 15;

/// Wire symbol codec — `BTCUSDT`.
const BINANCE_CODEC: SymbolCodec = SymbolCodec::Concat { upper: true };

// ─────────────────────────────────────────────────────────────────────────
// ProtocolHooks
// ─────────────────────────────────────────────────────────────────────────

/// Binance WSS protocol behaviour. Overrides only
/// `endpoint`/`subscribe_frames`/`heartbeat`; `prepare` stays the no-op default.
pub struct BinanceHooks;

impl ProtocolHooks for BinanceHooks {
    fn endpoint(&self) -> String {
        BINANCE_WSS_URL.to_string()
    }

    /// One subscribe frame covering every `(symbol × {depth, trade})` topic.
    /// `symbols` are venue wire symbols (`"BTCUSDT"`); stream names are lower.
    fn subscribe_frames(
        &self,
        symbols: &[String],
        declared: &DeclaredSet,
    ) -> Vec<Message> {
        use aetelier_types::config::markets::market_config::DeclaredDatatype as DD;
        let mut params = Vec::with_capacity(symbols.len() * 2);
        for s in symbols {
            let low = s.to_lowercase();
            if declared.contains(DD::Orderbook) {
                params.push(format!("{low}@depth@100ms"));
            }
            if declared.contains(DD::Trades) {
                params.push(format!("{low}@trade"));
            }
        }
        if params.is_empty() {
            return Vec::new();
        }
        let frame = serde_json::json!({
            "method": "SUBSCRIBE",
            "params": params,
            "id": 1,
        });
        vec![Message::Text(frame.to_string().into())]
    }

    fn heartbeat(&self) -> Heartbeat {
        Heartbeat::WsPong {
            every: Duration::from_secs(KEEPALIVE_SECS),
        }
    }

    /// Binance answers `SUBSCRIBE` with `{"result":null,"id":1}` and rejects
    /// with `{"error":{"code":…,"msg":…},"id":…}`. Data frames carry an `"e"`
    /// event field and neither key, so the substring guard keeps the hot path
    /// allocation-free.
    fn classify_ack(&self, text: &str) -> AckOutcome {
        if !(text.contains("\"result\"") || text.contains("\"error\"")) {
            return AckOutcome::NotAck;
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
            return AckOutcome::NotAck;
        };
        let Some(obj) = v.as_object() else {
            return AckOutcome::NotAck;
        };
        if let Some(err) = obj.get("error") {
            return AckOutcome::Rejected(
                err.get("msg")
                    .and_then(|m| m.as_str())
                    .unwrap_or("subscribe failed")
                    .to_string(),
            );
        }
        if obj.contains_key("result") && obj.contains_key("id") {
            return AckOutcome::Accepted;
        }
        AckOutcome::NotAck
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Normalizer — decoded BinanceWssEvent → DomainEvent
// ─────────────────────────────────────────────────────────────────────────

/// Maps a decoded [`BinanceWssEvent`] to `DomainEvent`s. Derives the canonical
/// pair from the event's venue symbol (the socket is multi-symbol). Holds the
/// worker's shared [`SourceMetrics`] so every dropped event is counted, not
/// just logged.
#[derive(Default)]
pub struct BinanceNormalizer {
    pub metrics: SourceMetrics,
}

impl Normalizer for BinanceNormalizer {
    type Event = BinanceWssEvent;

    fn normalize(&self, event: BinanceWssEvent) -> Vec<DomainEvent> {
        match event {
            // LOB delta: maps via `to_normalized`.
            BinanceWssEvent::DepthUpdate(d) => vec![DomainEvent::Book(d.to_normalized())],

            // REST-seeded snapshot is injected by the Sync seeder (it carries the
            // known symbol there); the live WSS decoder never yields it, so the
            // normalizer has no symbol to attach and emits nothing.
            BinanceWssEvent::DepthSnapshot(_) => Vec::new(),

            BinanceWssEvent::TradeData(t) => {
                let Some(pair) = BINANCE_CODEC.decode(&t.symbol) else {
                    tracing::warn!(symbol = %t.symbol, "binance.trade.bad_symbol");
                    self.metrics.add_dropped_frames(1);
                    return Vec::new();
                };
                let (Ok(amount), Ok(price)) = (
                    t.quantity.parse::<rust_decimal::Decimal>(),
                    t.price.parse::<rust_decimal::Decimal>(),
                ) else {
                    self.metrics.add_dropped_frames(1);
                    return Vec::new();
                };
                let Some(side) = TradeSide::from_str_loose(t.taker_side()) else {
                    tracing::warn!(side = %t.taker_side(), id = %t.trade_id, "binance.trade.unknown_side");
                    self.metrics.add_dropped_frames(1);
                    return Vec::new();
                };
                let trade = Trade {
                    source_trade_ts_us: crate::framework::model::epoch_to_us(
                        t.trade_time,
                    ),
                    local_trade_ts_us: 0,
                    source_trade_rtt_us: 0,
                    pair,
                    side,
                    amount,
                    price,
                    exchange: "binance".to_string(),
                    id: t.trade_id.to_string(),
                    origin: Default::default(),
                };
                // Binance's raw `trade` stream carries a per-symbol monotonic
                // `trade_id`; passing it arms the SourcedTradebook's
                // continuity accounting (gaps counted as permanent losses).
                vec![DomainEvent::Trade {
                    trade,
                    sequence: Some(t.trade_id),
                }]
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// ExchangeAdapter — registry entry
// ─────────────────────────────────────────────────────────────────────────

/// Static, data-only Binance profile. Const-constructible so it lives in a
/// `static` (no init-time allocation: `Vec::new` is `const`).
static BINANCE_PROFILE: ExchangeProfile = ExchangeProfile {
    id: "binance",
    symbol_codec: BINANCE_CODEC,
    budget: ConnectionBudget {
        // Binance spot: ~300 connections / 5 min per IP; up to 1024 streams per
        // socket.
        max_connections: Some(300),
        max_streams_per_socket: Some(1024),
        subscribe_rate: Vec::new(),
        connect_attempt_rate: None,
        connection_lifetime: None,
    },
    schema_version: 1,
    protocol_revision: "binance-spot-v3",
};

/// The registry handle. Unit struct — all state is in the static profile.
pub struct BinanceAdapter;

/// The single compiled-in Binance instance (referenced by `register_all`).
pub static BINANCE: BinanceAdapter = BinanceAdapter;

impl ExchangeAdapter for BinanceAdapter {
    fn id(&self) -> &'static str {
        "binance"
    }

    fn profile(&self) -> &ExchangeProfile {
        &BINANCE_PROFILE
    }

    fn rest_seeder(
        &self,
    ) -> Option<std::sync::Arc<dyn crate::framework::rest::RestSnapshot>> {
        Some(std::sync::Arc::new(BinanceRestSnapshot::new()))
    }

    fn book_model(&self, _channel: &str) -> ReconstructionModel {
        // Binance runs one model across its depth channel: incremental deltas,
        // RangeInclusive continuity, seeded by a REST snapshot.
        ReconstructionModel::SeqDelta {
            predicate: SeqPredicate::RangeInclusive,
            source: SnapshotSource::RestSnapshot,
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
        tokio::spawn(drive::<BinanceHooks, BinanceDecoder, BinanceNormalizer>(
            Arc::new(BinanceHooks),
            symbols,
            declared,
            BinanceNormalizer {
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
        BinanceHooks
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
        let normalizer = BinanceNormalizer {
            metrics: SourceMetrics::default(),
        };
        match BinanceDecoder::decode(raw)? {
            Some(event) => Ok(normalizer.normalize(event)),
            None => Ok(Vec::new()),
        }
    }

    fn replay_seed(
        &self,
        raw: &str,
        wire_symbol: &str,
    ) -> Result<
        Option<aetelier_types::orderbooks::NormalizedDelta>,
        Box<crate::errors::ExchangeError>,
    > {
        let snap: crate::sources::binance::responses::orderbooks::BinanceDepthSnapshot =
            serde_json::from_str(raw)
                .map_err(|e| Box::new(crate::errors::ExchangeError::JsonError(e)))?;
        Ok(Some(snap.to_normalized(wire_symbol)))
    }
}

// ─────────────────────────────────────────────────────────────────────────
// REST snapshot seeder
// ─────────────────────────────────────────────────────────────────────────

/// Public REST base URL and depth limit for the order-book seed snapshot.
const BINANCE_REST_URL: &str = "https://api.binance.com";
const BINANCE_REST_DEPTH: u32 = 5000;

/// Fetches the Binance depth snapshot that seeds a `SourcedOrderbook`.
pub struct BinanceRestSnapshot {
    client: crate::sources::binance::rest::BinanceRestClient,
}

impl BinanceRestSnapshot {
    pub fn new() -> Self {
        Self {
            client: crate::sources::binance::rest::BinanceRestClient::new(
                BINANCE_REST_URL,
            ),
        }
    }
}

impl Default for BinanceRestSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl crate::framework::rest::RestSnapshot for BinanceRestSnapshot {
    async fn fetch_snapshot(
        &self,
        symbol: &str,
    ) -> Result<aetelier_types::orderbooks::NormalizedDelta, crate::errors::ExchangeError>
    {
        let snap = self
            .client
            .fetch_depth(symbol, BINANCE_REST_DEPTH)
            .await
            .map_err(|e| {
                crate::errors::ExchangeError::IoError(std::io::Error::other(
                    e.to_string(),
                ))
            })?;
        Ok(snap.to_normalized(symbol))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::binance::responses::orderbooks::BinanceDepthUpdate;
    use crate::sources::binance::responses::trades::BinanceTradeData;
    use aetelier_types::orderbooks::decimal_to_f64;
    use aetelier_types::trading_pair::TradingPair;

    #[test]
    fn subscribe_ack_classification() {
        let h = BinanceHooks;
        assert_eq!(
            h.classify_ack(r#"{"result":null,"id":1}"#),
            AckOutcome::Accepted
        );
        assert_eq!(
            h.classify_ack(r#"{"error":{"code":2,"msg":"Invalid request"},"id":1}"#),
            AckOutcome::Rejected("Invalid request".into())
        );
        assert_eq!(
            h.classify_ack(r#"{"e":"trade","s":"BTCUSDT","p":"100.0"}"#),
            AckOutcome::NotAck
        );
    }

    fn trade(
        symbol: &str,
        id: u64,
        price: &str,
        qty: &str,
        buyer_maker: bool,
    ) -> BinanceTradeData {
        BinanceTradeData {
            event_type: "trade".into(),
            event_time: 1,
            symbol: symbol.into(),
            trade_id: id,
            price: price.into(),
            quantity: qty.into(),
            trade_time: 1_672_304_484_975,
            is_buyer_maker: buyer_maker,
        }
    }

    #[test]
    fn normalizes_trade_to_domain_event() {
        let evs = BinanceNormalizer::default().normalize(BinanceWssEvent::TradeData(
            trade("BTCUSDT", 42, "100.5", "0.3", true),
        ));
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            DomainEvent::Trade { trade, sequence } => {
                assert_eq!(trade.exchange, "binance");
                assert_eq!(trade.id, "42");
                assert!((decimal_to_f64(trade.price) - 100.5).abs() < 1e-9);
                assert!((decimal_to_f64(trade.amount) - 0.3).abs() < 1e-9);
                // is_buyer_maker = true → the taker SOLD.
                assert_eq!(trade.side, TradeSide::Sell);
                assert_eq!(trade.pair, TradingPair::new("BTC", "USDT"));
                assert_eq!(*sequence, Some(42));
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn normalizes_depth_update_to_book_event() {
        let d = BinanceDepthUpdate {
            event_type: "depthUpdate".into(),
            event_time: 1,
            symbol: "BTCUSDT".into(),
            first_update_id: 10,
            last_update_id: 12,
            bids: vec![["100.0".into(), "1.0".into()]],
            asks: vec![["101.0".into(), "2.0".into()]],
        };
        let evs = BinanceNormalizer::default().normalize(BinanceWssEvent::DepthUpdate(d));
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            DomainEvent::Book(nd) => {
                assert_eq!(nd.symbol, "BTCUSDT");
                assert_eq!(nd.update_id, 12);
                assert!(!nd.is_snapshot);
            }
            other => panic!("expected Book, got {other:?}"),
        }
    }

    #[test]
    fn subscribe_frame_covers_depth_and_trade() {
        let frames =
            BinanceHooks.subscribe_frames(&["BTCUSDT".to_string()], &DeclaredSet::all());
        assert_eq!(frames.len(), 1);
        let Message::Text(body) = &frames[0] else {
            panic!("expected a text frame");
        };
        assert!(body.contains("SUBSCRIBE"));
        assert!(body.contains("btcusdt@depth@100ms"));
        assert!(body.contains("btcusdt@trade"));
    }

    #[test]
    fn profile_and_book_model_match_the_binance_row() {
        let a = BinanceAdapter;
        assert_eq!(a.id(), "binance");
        assert!(matches!(
            a.profile().symbol_codec,
            SymbolCodec::Concat { upper: true }
        ));
        assert_eq!(a.profile().id, "binance");
        assert!(matches!(
            a.book_model("orders"),
            ReconstructionModel::SeqDelta {
                predicate: SeqPredicate::RangeInclusive,
                source: SnapshotSource::RestSnapshot,
            }
        ));
    }
}
