//! Upbit spot public market-data adapter: subscribes to orderbook + trade
//! streams over WebSocket and normalizes them into the framework's DomainEvent
//! stream.
//!
//! - Subscribe is a top-level JSON array (`[{ticket},{type,codes},…]`).
//! - Each `orderbook` frame is a complete top-N book (no seeding, no sequence),
//!   so reconstruction uses [`ReconstructionModel::FullRefresh`].
//! - Symbols are spelled quote-first (`KRW-BTC`) via [`SymbolCodec::QuoteFirst`].
//!
//! Wire types and decoder live in `crate::sources::upbit`. Numbers arrive as
//! JSON numbers (not strings) and are stringified into `NormalizedDelta` here.

use std::sync::{Arc, LazyLock};
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::protocol::Message;

use aetelier_types::orderbooks::{NormalizedDelta, f64_to_decimal};
use aetelier_types::trades::{Trade, TradeSide};

use crate::framework::budget::{ConnectionBudget, RateWindow, SourceMetrics};
use crate::framework::driver::{DEFAULT_RAW_BUFFER, drive};
use crate::framework::model::{
    DomainEvent, Normalizer, ReconstructionModel, epoch_to_us,
};
use crate::framework::protocol::DeclaredSet;
use crate::framework::protocol::{Heartbeat, ProtocolHooks};
use crate::framework::registry::{ExchangeAdapter, ExchangeProfile, TaskExit};
use crate::framework::symbol::SymbolCodec;

use crate::sources::upbit::decoder::UpbitDecoder;
use crate::sources::upbit::events::UpbitWssEvent;

const UPBIT_WSS_URL: &str = "wss://api.upbit.com/websocket/v1";

/// Upbit keep-alive: a literal `"PING"` text answered with `"PONG"` (swallowed
/// by the decoder). Upbit drops connections idle past ~120s.
const PING_SECS: u64 = 30;

/// Wire symbol codec — quote-first `KRW-BTC`.
const UPBIT_CODEC: SymbolCodec = SymbolCodec::QuoteFirst { sep: '-' };

// ─────────────────────────────────────────────────────────────────────────
// ProtocolHooks
// ─────────────────────────────────────────────────────────────────────────

pub struct UpbitHooks;

impl ProtocolHooks for UpbitHooks {
    fn endpoint(&self) -> String {
        UPBIT_WSS_URL.to_string()
    }

    /// A single top-level JSON **array**: a ticket, one `orderbook` type entry
    /// and one `trade` type entry (each carrying every code), then the format.
    fn subscribe_frames(
        &self,
        symbols: &[String],
        declared: &DeclaredSet,
    ) -> Vec<Message> {
        use aetelier_types::config::markets::market_config::DeclaredDatatype as DD;
        let codes: Vec<&String> = symbols.iter().collect();
        let mut parts = vec![serde_json::json!({ "ticket": "aetelier-ingest" })];
        if declared.contains(DD::Orderbook) {
            parts.push(serde_json::json!({ "type": "orderbook", "codes": codes }));
        }
        if declared.contains(DD::Trades) {
            parts.push(serde_json::json!({ "type": "trade", "codes": codes }));
        }
        if parts.len() == 1 {
            return Vec::new();
        }
        parts.push(serde_json::json!({ "format": "DEFAULT" }));
        vec![Message::Text(
            serde_json::Value::Array(parts).to_string().into(),
        )]
    }

    fn heartbeat(&self) -> Heartbeat {
        Heartbeat::Text {
            every: Duration::from_secs(PING_SECS),
            payload: Arc::new(|| "PING".to_string()),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Normalizer
// ─────────────────────────────────────────────────────────────────────────

/// Maps a decoded `UpbitWssEvent` to `DomainEvent`s. Holds the worker's
/// shared [`SourceMetrics`] so every dropped event is counted, not just
/// logged.
#[derive(Default)]
pub struct UpbitNormalizer {
    pub metrics: SourceMetrics,
}

impl Normalizer for UpbitNormalizer {
    type Event = UpbitWssEvent;

    fn normalize(&self, event: UpbitWssEvent) -> Vec<DomainEvent> {
        match event {
            UpbitWssEvent::Orderbook(ob) => {
                let bids = ob
                    .orderbook_units
                    .iter()
                    .map(|u| (u.bid_price.to_string(), u.bid_size.to_string()))
                    .collect();
                let asks = ob
                    .orderbook_units
                    .iter()
                    .map(|u| (u.ask_price.to_string(), u.ask_size.to_string()))
                    .collect();
                vec![DomainEvent::Book(NormalizedDelta {
                    symbol: ob.code,
                    bids,
                    asks,
                    update_id: ob.timestamp,
                    sequence: 0,
                    source_orderbook_ts_us: epoch_to_us(ob.timestamp),
                    local_orderbook_ts_us: 0,
                    source_orderbook_rtt_us: 0,
                    checksum: None,
                    orders: Vec::new(),
                    is_snapshot: true, // every frame is a full book
                })]
            }
            UpbitWssEvent::Trade(t) => {
                let Some(pair) = UPBIT_CODEC.decode(&t.code) else {
                    tracing::warn!(symbol = %t.code, "upbit.trade.bad_symbol");
                    self.metrics.add_dropped_frames(1);
                    return Vec::new();
                };
                let side = if t.ask_bid.eq_ignore_ascii_case("BID") {
                    TradeSide::Buy
                } else if t.ask_bid.eq_ignore_ascii_case("ASK") {
                    TradeSide::Sell
                } else {
                    tracing::warn!(ask_bid = %t.ask_bid, "upbit.trade.unknown_side");
                    self.metrics.add_dropped_frames(1);
                    return Vec::new();
                };
                let trade = Trade {
                    source_trade_ts_us: epoch_to_us(t.trade_timestamp),
                    local_trade_ts_us: 0,
                    source_trade_rtt_us: 0,
                    pair,
                    side,
                    amount: f64_to_decimal(t.trade_volume),
                    price: f64_to_decimal(t.trade_price),
                    exchange: "upbit".to_string(),
                    id: t.sequential_id.to_string(),
                    origin: Default::default(),
                };
                // Upbit `sequential_id` is per-market monotonic (verified +1
                // on live KRW-BTC capture, cycle #4); armed for loss accounting.
                vec![DomainEvent::Trade {
                    trade,
                    sequence: Some(t.sequential_id),
                }]
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// ExchangeAdapter
// ─────────────────────────────────────────────────────────────────────────

// `LazyLock` (not `static`) because the documented multi-window subscribe rate
// needs a non-empty `Vec`, which is not const-constructible.
static UPBIT_PROFILE: LazyLock<ExchangeProfile> = LazyLock::new(|| ExchangeProfile {
    id: "upbit",
    symbol_codec: UPBIT_CODEC,
    budget: ConnectionBudget {
        max_connections: None,
        max_streams_per_socket: None,
        // Documented Upbit subscribe rate: 5 requests/s and 100 requests/min.
        subscribe_rate: vec![
            RateWindow {
                max: 5,
                per: Duration::from_secs(1),
            },
            RateWindow {
                max: 100,
                per: Duration::from_secs(60),
            },
        ],
        connect_attempt_rate: None,
        connection_lifetime: None,
    },
    schema_version: 1,
    protocol_revision: "upbit-v1",
});

pub struct UpbitAdapter;

/// The single compiled-in Upbit instance (referenced by `register_all`).
pub static UPBIT: UpbitAdapter = UpbitAdapter;

impl ExchangeAdapter for UpbitAdapter {
    fn id(&self) -> &'static str {
        "upbit"
    }

    fn profile(&self) -> &ExchangeProfile {
        &UPBIT_PROFILE
    }

    fn book_model(&self, _channel: &str) -> ReconstructionModel {
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
        tokio::spawn(drive::<UpbitHooks, UpbitDecoder, UpbitNormalizer>(
            Arc::new(UpbitHooks),
            symbols,
            declared,
            UpbitNormalizer {
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
        UpbitHooks
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
        let normalizer = UpbitNormalizer {
            metrics: SourceMetrics::default(),
        };
        match UpbitDecoder::decode(raw)? {
            Some(event) => Ok(normalizer.normalize(event)),
            None => Ok(Vec::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::wss::WssDecoder;
    use crate::sources::upbit::responses::{UpbitBookUnit, UpbitOrderbook, UpbitTrade};

    #[test]
    fn normalizes_full_orderbook_frame() {
        let ob = UpbitOrderbook {
            code: "KRW-BTC".into(),
            timestamp: 1_700_000_000_000,
            orderbook_units: vec![
                UpbitBookUnit {
                    ask_price: 50_001_000.0,
                    bid_price: 50_000_000.0,
                    ask_size: 0.5,
                    bid_size: 1.0,
                },
                UpbitBookUnit {
                    ask_price: 50_002_000.0,
                    bid_price: 49_999_000.0,
                    ask_size: 0.25,
                    bid_size: 2.0,
                },
            ],
        };
        let evs = UpbitNormalizer::default().normalize(UpbitWssEvent::Orderbook(ob));
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            DomainEvent::Book(nd) => {
                assert_eq!(nd.symbol, "KRW-BTC");
                assert!(nd.is_snapshot);
                assert_eq!(nd.bids.len(), 2);
                assert_eq!(nd.asks.len(), 2);
                assert_eq!(nd.bids[0].0, "50000000");
            }
            other => panic!("expected Book, got {other:?}"),
        }
    }

    #[test]
    fn normalizes_trade_bid_is_buy() {
        let t = UpbitTrade {
            code: "KRW-BTC".into(),
            trade_price: 50_000_000.0,
            trade_volume: 0.01,
            ask_bid: "BID".into(),
            trade_timestamp: 1_700_000_000_000,
            sequential_id: 99,
        };
        let evs = UpbitNormalizer::default().normalize(UpbitWssEvent::Trade(t));
        match &evs[0] {
            DomainEvent::Trade { trade, .. } => {
                assert_eq!(trade.exchange, "upbit");
                assert_eq!(trade.side, TradeSide::Buy);
                assert_eq!(trade.id, "99");
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn subscribe_frame_is_a_json_array() {
        let frames =
            UpbitHooks.subscribe_frames(&["KRW-BTC".to_string()], &DeclaredSet::all());
        assert_eq!(frames.len(), 1);
        let Message::Text(body) = &frames[0] else {
            panic!("expected a text frame");
        };
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        let arr = v.as_array().expect("subscribe frame must be a JSON array");
        assert_eq!(arr[0]["ticket"], "aetelier-ingest");
        assert_eq!(arr[1]["type"], "orderbook");
        assert_eq!(arr[2]["type"], "trade");
    }

    #[test]
    fn decoder_dispatches_on_type() {
        let ob =
            r#"{"type":"orderbook","code":"KRW-BTC","timestamp":1,"orderbook_units":[]}"#;
        assert!(matches!(
            UpbitDecoder::decode(ob).unwrap(),
            Some(UpbitWssEvent::Orderbook(_))
        ));
        // Status frame → no event.
        assert!(
            UpbitDecoder::decode(r#"{"status":"UP"}"#)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn profile_uses_quote_first_codec_and_full_refresh() {
        let a = UpbitAdapter;
        assert_eq!(a.id(), "upbit");
        assert!(matches!(
            a.profile().symbol_codec,
            SymbolCodec::QuoteFirst { sep: '-' }
        ));
        assert!(matches!(
            a.book_model("orders"),
            ReconstructionModel::FullRefresh
        ));
    }
}
