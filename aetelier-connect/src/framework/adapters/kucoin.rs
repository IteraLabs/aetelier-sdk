//! KuCoin spot public market-data adapter: bootstraps via a bullet-token POST,
//! subscribes to level2 (book) + match (trade) topics, and normalizes them into
//! the framework's DomainEvent stream.
//!
//! KuCoin will not accept a connection to a static URL. A client must first
//! `POST /api/v1/bullet-public`, which returns a one-time `token`, the actual
//! `endpoint`, and the server's `pingInterval`. The live socket URL is
//! `{endpoint}?token={token}&connectId={id}`, and the keep-alive cadence is
//! whatever the server said — both injected via [`Prepared::endpoint_override`]
//! / [`Prepared::heartbeat_override`]. Reconstruction is REST-seeded `SeqDelta`.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::protocol::Message;
use uuid::Uuid;

use aetelier_types::orderbooks::NormalizedDelta;
use aetelier_types::trades::{Trade, TradeSide};

use crate::errors::ExchangeError;
use crate::framework::budget::{ConnectionBudget, SourceMetrics};
use crate::framework::driver::{DEFAULT_RAW_BUFFER, drive};
use crate::framework::model::{
    DomainEvent, Normalizer, ReconstructionModel, SeqPredicate, SnapshotSource,
};
use crate::framework::protocol::DeclaredSet;
use crate::framework::protocol::{Heartbeat, Prepared, ProtocolHooks};
use crate::framework::registry::{ExchangeAdapter, ExchangeProfile, TaskExit};
use crate::framework::symbol::SymbolCodec;
use crate::sources::kucoin::decoder::KucoinDecoder;
use crate::sources::kucoin::events::KucoinWssEvent;
use crate::sources::kucoin::responses::orderbooks::KucoinChange;
use crate::sources::kucoin::rest::parse_snapshot;

/// Public bullet-token endpoint (no auth for public market data).
const BULLET_URL: &str = "https://api.kucoin.com/api/v1/bullet-public";

/// Fallback ping cadence if the bullet response omits `pingInterval`.
const DEFAULT_PING_MS: u64 = 18_000;

/// Wire symbol codec — `BTC-USDT`.
const KUCOIN_CODEC: SymbolCodec = SymbolCodec::Hyphen;

fn invalid(msg: &str) -> ExchangeError {
    ExchangeError::IoError(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        msg.to_string(),
    ))
}

/// Build the connection [`Prepared`] from a bullet-token response body. Pure,
/// so it can be tested independently of the live POST.
fn parse_bullet(body: &str, connect_id: &str) -> Result<Prepared, ExchangeError> {
    let v: serde_json::Value = serde_json::from_str(body)?;
    let data = v
        .get("data")
        .ok_or_else(|| invalid("bullet: missing data"))?;
    let token = data
        .get("token")
        .and_then(|t| t.as_str())
        .ok_or_else(|| invalid("bullet: missing token"))?;
    let server = data
        .get("instanceServers")
        .and_then(|s| s.as_array())
        .and_then(|a| a.first())
        .ok_or_else(|| invalid("bullet: no instanceServers"))?;
    let endpoint = server
        .get("endpoint")
        .and_then(|e| e.as_str())
        .ok_or_else(|| invalid("bullet: missing endpoint"))?;
    let ping_ms = server
        .get("pingInterval")
        .and_then(|p| p.as_u64())
        .unwrap_or(DEFAULT_PING_MS);

    let url = format!("{endpoint}?token={token}&connectId={connect_id}");
    Ok(Prepared {
        endpoint_override: Some(url),
        heartbeat_override: Some(Heartbeat::Text {
            every: Duration::from_millis(ping_ms),
            payload: Arc::new(|| {
                serde_json::json!({ "id": "aetelier-hb", "type": "ping" }).to_string()
            }),
        }),
        extra_frames: Vec::new(),
        extra_frames_delay: None,
    })
}

// ─────────────────────────────────────────────────────────────────────────
// ProtocolHooks
// ─────────────────────────────────────────────────────────────────────────

pub struct KucoinHooks {
    /// Unique per-connection id required in the bullet WSS URL query.
    connect_id: String,
}

impl KucoinHooks {
    /// Construct hooks with a per-connection id (`prepare` does the
    /// bullet-token POST to form the real endpoint). Public so capture
    /// tooling can drive the same protocol the runtime uses.
    pub fn with_connect_id(connect_id: String) -> Self {
        Self { connect_id }
    }
}

#[async_trait::async_trait]
impl ProtocolHooks for KucoinHooks {
    /// `prepare` overrides this with the bullet endpoint. Never connected to
    /// directly (KuCoin rejects a token-less URL).
    fn endpoint(&self) -> String {
        "wss://ws-api-spot.kucoin.com/".to_string()
    }

    /// One `subscribe` frame for level2 (book) and one for match (trades),
    /// each spanning every symbol (comma-joined topic).
    fn subscribe_frames(
        &self,
        symbols: &[String],
        declared: &DeclaredSet,
    ) -> Vec<Message> {
        use aetelier_types::config::markets::market_config::DeclaredDatatype as DD;
        let csv = symbols.join(",");
        let mut frames = Vec::with_capacity(2);
        if declared.contains(DD::Orderbook) {
            let l2 = serde_json::json!({
                "id": "sub-l2", "type": "subscribe",
                "topic": format!("/market/level2:{csv}"),
                "privateChannel": false, "response": true,
            });
            frames.push(Message::Text(l2.to_string().into()));
        }
        if declared.contains(DD::Trades) {
            let m = serde_json::json!({
                "id": "sub-match", "type": "subscribe",
                "topic": format!("/market/match:{csv}"),
                "privateChannel": false, "response": true,
            });
            frames.push(Message::Text(m.to_string().into()));
        }
        frames
    }

    /// Bootstrap: fetch the bullet token, then derive the dynamic endpoint +
    /// server ping cadence via `parse_bullet`.
    async fn prepare(&self) -> Result<Prepared, ExchangeError> {
        let body = reqwest::Client::new()
            .post(BULLET_URL)
            .send()
            .await
            .map_err(|e| invalid(&format!("bullet POST: {e}")))?
            .text()
            .await
            .map_err(|e| invalid(&format!("bullet body: {e}")))?;
        parse_bullet(&body, &self.connect_id)
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Normalizer
// ─────────────────────────────────────────────────────────────────────────

/// Maps a decoded `KucoinWssEvent` to `DomainEvent`s. Holds the worker's
/// shared [`SourceMetrics`] so every dropped event is counted, not just
/// logged.
#[derive(Default)]
pub struct KucoinNormalizer {
    pub metrics: SourceMetrics,
}

impl Normalizer for KucoinNormalizer {
    type Event = KucoinWssEvent;

    fn normalize(&self, event: KucoinWssEvent) -> Vec<DomainEvent> {
        match event {
            KucoinWssEvent::Level2(d) => {
                let map = |c: &KucoinChange| (c.0.clone(), c.1.clone());
                vec![DomainEvent::Book(NormalizedDelta {
                    symbol: d.symbol,
                    bids: d.changes.bids.iter().map(map).collect(),
                    asks: d.changes.asks.iter().map(map).collect(),
                    update_id: d.sequence_end,
                    sequence: d.sequence_start,
                    source_orderbook_ts_us: crate::framework::model::epoch_to_us(d.time),
                    local_orderbook_ts_us: 0,
                    source_orderbook_rtt_us: 0,
                    checksum: None,
                    orders: Vec::new(),
                    is_snapshot: false,
                })]
            }
            KucoinWssEvent::Match(m) => {
                let Some(pair) = KUCOIN_CODEC.decode(&m.symbol) else {
                    tracing::warn!(symbol = %m.symbol, "kucoin.trade.bad_symbol");
                    self.metrics.add_dropped_frames(1);
                    return Vec::new();
                };
                let (Ok(amount), Ok(price)) = (
                    m.size.parse::<rust_decimal::Decimal>(),
                    m.price.parse::<rust_decimal::Decimal>(),
                ) else {
                    self.metrics.add_dropped_frames(1);
                    return Vec::new();
                };
                // Drop (never fabricate as Buy) an unknown taker side so it can't
                // pollute the side distribution.
                let Some(side) = TradeSide::from_str_loose(&m.side) else {
                    tracing::warn!(side = %m.side, id = %m.trade_id, "kucoin.trade.unknown_side");
                    self.metrics.add_dropped_frames(1);
                    return Vec::new();
                };
                // KuCoin match `time` is nanoseconds; epoch_to_us normalizes by
                // magnitude (ns -> ms) so the unit is never mis-guessed.
                let trade_ts = crate::framework::model::epoch_to_us(
                    m.time.parse::<u64>().unwrap_or(0),
                );
                vec![DomainEvent::Trade {
                    trade: Trade {
                        source_trade_ts_us: trade_ts,
                        local_trade_ts_us: 0,
                        source_trade_rtt_us: 0,
                        pair,
                        side,
                        amount,
                        price,
                        exchange: "kucoin".to_string(),
                        id: m.trade_id,
                        origin: Default::default(),
                    },
                    // NOT armable (cycle #7 density check on live data): the
                    // match `tradeId` EQUALS `sequence` and both stride in
                    // ~1e7 jumps between consecutive prints (sequence-scale,
                    // not a trade counter) — arming would fabricate millions
                    // of `trades_lost` per print. Best-effort.
                    sequence: None,
                }]
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// ExchangeAdapter
// ─────────────────────────────────────────────────────────────────────────

static KUCOIN_PROFILE: ExchangeProfile = ExchangeProfile {
    id: "kucoin",
    symbol_codec: KUCOIN_CODEC,
    budget: ConnectionBudget {
        max_connections: None,
        max_streams_per_socket: None,
        subscribe_rate: Vec::new(),
        connect_attempt_rate: None,
        // The bullet token has a finite lifetime — must reconnect (and re-fetch)
        // before it expires.
        connection_lifetime: Some(Duration::from_secs(24 * 60 * 60)),
    },
    schema_version: 1,
    protocol_revision: "kucoin-v1",
};

pub struct KucoinAdapter;

/// The single compiled-in KuCoin instance (referenced by `register_all`).
pub static KUCOIN: KucoinAdapter = KucoinAdapter;

impl ExchangeAdapter for KucoinAdapter {
    fn id(&self) -> &'static str {
        "kucoin"
    }

    fn profile(&self) -> &ExchangeProfile {
        &KUCOIN_PROFILE
    }

    fn rest_seeder(
        &self,
    ) -> Option<std::sync::Arc<dyn crate::framework::rest::RestSnapshot>> {
        Some(std::sync::Arc::new(KucoinRestSnapshot::new()))
    }

    fn book_model(&self, _channel: &str) -> ReconstructionModel {
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
        let hooks = Arc::new(KucoinHooks {
            connect_id: Uuid::new_v4().to_string(),
        });
        tokio::spawn(drive::<KucoinHooks, KucoinDecoder, KucoinNormalizer>(
            hooks,
            symbols,
            declared,
            KucoinNormalizer {
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
        KucoinHooks::with_connect_id("preview".to_string())
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
        let normalizer = KucoinNormalizer {
            metrics: SourceMetrics::default(),
        };
        match KucoinDecoder::decode(raw)? {
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
        parse_snapshot(raw, wire_symbol).map(Some).map_err(Box::new)
    }
}

// ─────────────────────────────────────────────────────────────────────────
// REST seeder
// ─────────────────────────────────────────────────────────────────────────

pub use crate::sources::kucoin::rest::KucoinRestSnapshot;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framework::protocol::Heartbeat;
    use crate::sources::kucoin::responses::{
        KucoinChange, KucoinChanges, KucoinL2Data, KucoinMatchData,
    };

    const BULLET_SAMPLE: &str = r#"{
        "code":"200000",
        "data":{
            "token":"abc123",
            "instanceServers":[
                {"endpoint":"wss://ws-spot.kucoin.com/","encrypt":true,"pingInterval":18000,"pingTimeout":10000}
            ]
        }
    }"#;

    #[test]
    fn parse_bullet_builds_dynamic_endpoint_and_ping() {
        let prepared = parse_bullet(BULLET_SAMPLE, "conn-42").unwrap();
        let url = prepared.endpoint_override.expect("endpoint override");
        assert_eq!(
            url,
            "wss://ws-spot.kucoin.com/?token=abc123&connectId=conn-42"
        );
        match prepared.heartbeat_override.expect("heartbeat override") {
            Heartbeat::Text { every, payload } => {
                assert_eq!(every, Duration::from_millis(18000));
                assert!(payload().contains("\"ping\""));
            }
            _ => panic!("expected a Text ping heartbeat"),
        }
    }

    #[test]
    fn parse_bullet_rejects_malformed() {
        assert!(parse_bullet(r#"{"code":"200000"}"#, "x").is_err());
    }

    #[test]
    fn normalizes_level2_range() {
        let d = KucoinL2Data {
            sequence_start: 100,
            sequence_end: 105,
            symbol: "BTC-USDT".into(),
            changes: KucoinChanges {
                asks: vec![KucoinChange("101.0".into(), "0.0".into(), "105".into())],
                bids: vec![KucoinChange("100.0".into(), "2.0".into(), "104".into())],
            },
            time: 1_784_133_306_841,
        };
        let evs = KucoinNormalizer::default().normalize(KucoinWssEvent::Level2(d));
        match &evs[0] {
            DomainEvent::Book(nd) => {
                assert_eq!(nd.symbol, "BTC-USDT");
                assert_eq!(nd.update_id, 105);
                assert_eq!(nd.sequence, 100);
                assert_eq!(nd.source_orderbook_ts_us, 1_784_133_306_841_000);
            }
            other => panic!("expected Book, got {other:?}"),
        }
    }

    #[test]
    fn level2_venue_timestamp_survives_a_captured_frame() {
        let raw = include_str!("../../../datasets/kucoin/btcusdt_book_trade.jsonl");
        let frame = raw
            .lines()
            .find(|l| l.contains("trade.l2update"))
            .expect("capture contains a level2 frame");
        let v: serde_json::Value = serde_json::from_str(frame).expect("frame parses");
        let d: KucoinL2Data =
            serde_json::from_value(v["data"].clone()).expect("level2 data decodes");

        assert!(
            d.time > 0,
            "KuCoin level2 frames carry `time`; got {}",
            d.time
        );

        let evs = KucoinNormalizer::default().normalize(KucoinWssEvent::Level2(d));
        match &evs[0] {
            DomainEvent::Book(nd) => assert!(
                nd.source_orderbook_ts_us > 1_600_000_000_000_000,
                "venue time must reach the delta in us, got {}",
                nd.source_orderbook_ts_us
            ),
            other => panic!("expected Book, got {other:?}"),
        }
    }

    #[test]
    fn normalizes_match_nanoseconds_to_us() {
        let m = KucoinMatchData {
            symbol: "BTC-USDT".into(),
            side: "buy".into(),
            price: "50000.0".into(),
            size: "0.01".into(),
            time: "1700000000000000000".into(),
            trade_id: "tid-1".into(),
        };
        let evs = KucoinNormalizer::default().normalize(KucoinWssEvent::Match(m));
        match &evs[0] {
            DomainEvent::Trade { trade, .. } => {
                assert_eq!(trade.exchange, "kucoin");
                assert_eq!(trade.side, TradeSide::Buy);
                assert_eq!(trade.source_trade_ts_us, 1_700_000_000_000_000); // ns → us
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn profile_id_and_codec() {
        let a = KucoinAdapter;
        assert_eq!(a.id(), "kucoin");
        assert!(matches!(a.profile().symbol_codec, SymbolCodec::Hyphen));
    }
}
