use std::sync::{Arc, LazyLock};
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::protocol::Message;

use aetelier_types::orderbooks::NormalizedDelta;
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

use crate::sources::hyperliquid::decoder::HyperliquidDecoder;
use crate::sources::hyperliquid::events::HyperliquidWssEvent;

const HYPERLIQUID_WSS_URL: &str = "wss://api.hyperliquid.xyz/ws";

const PING_SECS: u64 = 30;

const HYPERLIQUID_CODEC: SymbolCodec = SymbolCodec::BareCoin { quote: "USDC" };

pub struct HyperliquidHooks;

impl ProtocolHooks for HyperliquidHooks {
    fn endpoint(&self) -> String {
        HYPERLIQUID_WSS_URL.to_string()
    }

    fn subscribe_frames(
        &self,
        symbols: &[String],
        declared: &DeclaredSet,
    ) -> Vec<Message> {
        use aetelier_types::config::markets::market_config::DeclaredDatatype as DD;
        let mut frames = Vec::new();
        for symbol in symbols {
            if declared.contains(DD::Orderbook) {
                frames.push(subscribe_frame("l2Book", symbol));
            }
            if declared.contains(DD::Trades) {
                frames.push(subscribe_frame("trades", symbol));
            }
        }
        frames
    }

    fn heartbeat(&self) -> Heartbeat {
        Heartbeat::Text {
            every: Duration::from_secs(PING_SECS),
            payload: Arc::new(|| r#"{"method":"ping"}"#.to_string()),
        }
    }
}

fn subscribe_frame(channel: &str, symbol: &str) -> Message {
    Message::Text(
        serde_json::json!({
            "method": "subscribe",
            "subscription": { "type": channel, "coin": symbol }
        })
        .to_string()
        .into(),
    )
}

#[derive(Default)]
pub struct HyperliquidNormalizer {
    pub metrics: SourceMetrics,
}

impl Normalizer for HyperliquidNormalizer {
    type Event = HyperliquidWssEvent;

    fn normalize(&self, event: HyperliquidWssEvent) -> Vec<DomainEvent> {
        match event {
            HyperliquidWssEvent::Book(book) => {
                let [bids, asks] = book.levels.as_slice() else {
                    tracing::warn!(
                        symbol = %book.coin,
                        sides = book.levels.len(),
                        "hyperliquid.book.bad_levels_shape"
                    );
                    self.metrics.add_dropped_frames(1);
                    return Vec::new();
                };
                let bids = bids.iter().map(|l| (l.px.clone(), l.sz.clone())).collect();
                let asks = asks.iter().map(|l| (l.px.clone(), l.sz.clone())).collect();
                vec![DomainEvent::Book(NormalizedDelta {
                    symbol: book.coin,
                    bids,
                    asks,
                    update_id: book.time,
                    sequence: 0,
                    source_orderbook_ts_us: epoch_to_us(book.time),
                    local_orderbook_ts_us: 0,
                    source_orderbook_rtt_us: 0,
                    checksum: None,
                    orders: Vec::new(),
                    is_snapshot: true,
                })]
            }
            HyperliquidWssEvent::Trades(trades) => trades
                .into_iter()
                .filter_map(|t| normalize_trade(t, &self.metrics))
                .collect(),
        }
    }
}

fn normalize_trade(
    t: crate::sources::hyperliquid::responses::HyperliquidTrade,
    metrics: &SourceMetrics,
) -> Option<DomainEvent> {
    let Some(pair) = HYPERLIQUID_CODEC.decode(&t.coin) else {
        tracing::warn!(symbol = %t.coin, "hyperliquid.trade.bad_symbol");
        metrics.add_dropped_frames(1);
        return None;
    };
    let side = if t.side.eq_ignore_ascii_case("B") {
        TradeSide::Buy
    } else if t.side.eq_ignore_ascii_case("A") {
        TradeSide::Sell
    } else {
        tracing::warn!(side = %t.side, tid = t.tid, "hyperliquid.trade.unknown_side");
        metrics.add_dropped_frames(1);
        return None;
    };
    let (Ok(amount), Ok(price)) = (
        t.sz.parse::<rust_decimal::Decimal>(),
        t.px.parse::<rust_decimal::Decimal>(),
    ) else {
        tracing::warn!(tid = t.tid, "hyperliquid.trade.bad_decimal");
        metrics.add_dropped_frames(1);
        return None;
    };
    let trade = Trade {
        source_trade_ts_us: epoch_to_us(t.time),
        local_trade_ts_us: 0,
        source_trade_rtt_us: 0,
        pair,
        side,
        amount,
        price,
        exchange: "hyperliquid".to_string(),
        id: t.tid.to_string(),
        origin: Default::default(),
    };
    Some(DomainEvent::Trade {
        trade,
        sequence: None,
    })
}

static HYPERLIQUID_PROFILE: LazyLock<ExchangeProfile> =
    LazyLock::new(|| ExchangeProfile {
        id: "hyperliquid",
        symbol_codec: HYPERLIQUID_CODEC,
        budget: ConnectionBudget {
            max_connections: Some(10),
            max_streams_per_socket: Some(1000),
            subscribe_rate: vec![RateWindow {
                max: 2000,
                per: Duration::from_secs(60),
            }],
            connect_attempt_rate: Some(RateWindow {
                max: 30,
                per: Duration::from_secs(60),
            }),
            connection_lifetime: None,
        },
        schema_version: 1,
        protocol_revision: "hyperliquid-v1",
    });

pub struct HyperliquidAdapter;

pub static HYPERLIQUID: HyperliquidAdapter = HyperliquidAdapter;

impl ExchangeAdapter for HyperliquidAdapter {
    fn id(&self) -> &'static str {
        "hyperliquid"
    }

    fn profile(&self) -> &ExchangeProfile {
        &HYPERLIQUID_PROFILE
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
        tokio::spawn(drive::<
            HyperliquidHooks,
            HyperliquidDecoder,
            HyperliquidNormalizer,
        >(
            Arc::new(HyperliquidHooks),
            symbols,
            declared,
            HyperliquidNormalizer {
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
        HyperliquidHooks
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
        let normalizer = HyperliquidNormalizer {
            metrics: SourceMetrics::default(),
        };
        match HyperliquidDecoder::decode(raw)? {
            Some(event) => Ok(normalizer.normalize(event)),
            None => Ok(Vec::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::wss::WssDecoder;
    use crate::sources::hyperliquid::responses::{HyperliquidBook, HyperliquidLevel};

    fn level(px: &str, sz: &str) -> HyperliquidLevel {
        HyperliquidLevel {
            px: px.to_string(),
            sz: sz.to_string(),
            n: 1,
        }
    }

    #[test]
    fn normalizes_full_book_snapshot_preserving_wire_decimal_strings() {
        let book = HyperliquidBook {
            coin: "BTC".into(),
            time: 1_700_000_000_000,
            levels: vec![
                vec![level("50000.0", "1.5"), level("49999.5", "2.0")],
                vec![level("50000.5", "0.75")],
            ],
        };
        let evs =
            HyperliquidNormalizer::default().normalize(HyperliquidWssEvent::Book(book));
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            DomainEvent::Book(nd) => {
                assert_eq!(nd.symbol, "BTC");
                assert!(nd.is_snapshot);
                assert_eq!(nd.bids.len(), 2);
                assert_eq!(nd.asks.len(), 1);
                assert_eq!(nd.bids[0], ("50000.0".to_string(), "1.5".to_string()));
                assert_eq!(nd.source_orderbook_ts_us, 1_700_000_000_000_000);
            }
            other => panic!("expected Book, got {other:?}"),
        }
    }

    #[test]
    fn drops_book_without_two_sides() {
        let book = HyperliquidBook {
            coin: "BTC".into(),
            time: 1,
            levels: vec![vec![level("1", "1")]],
        };
        let evs =
            HyperliquidNormalizer::default().normalize(HyperliquidWssEvent::Book(book));
        assert!(evs.is_empty());
    }

    #[test]
    fn normalizes_trades_b_is_buy_a_is_sell_sequence_unarmed() {
        let raw = r#"{"channel":"trades","data":[
            {"coin":"BTC","side":"B","px":"50000.1","sz":"0.01","time":1700000000000,"tid":123,"users":["0xa","0xb"],"hash":"0xc"},
            {"coin":"BTC","side":"A","px":"50000.0","sz":"0.02","time":1700000000001,"tid":124,"users":["0xd","0xe"],"hash":"0xf"}
        ]}"#;
        let event = HyperliquidDecoder::decode(raw).unwrap().unwrap();
        let evs = HyperliquidNormalizer::default().normalize(event);
        assert_eq!(evs.len(), 2);
        match &evs[0] {
            DomainEvent::Trade { trade, sequence } => {
                assert_eq!(trade.side, TradeSide::Buy);
                assert_eq!(trade.exchange, "hyperliquid");
                assert_eq!(trade.id, "123");
                assert_eq!(trade.pair.to_canonical(), "BTC/USDC");
                assert_eq!(trade.source_trade_ts_us, 1_700_000_000_000_000);
                assert_eq!(sequence, &None);
            }
            other => panic!("expected Trade, got {other:?}"),
        }
        match &evs[1] {
            DomainEvent::Trade { trade, .. } => {
                assert_eq!(trade.side, TradeSide::Sell);
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn drops_trade_with_unparseable_decimal() {
        let raw = r#"{"channel":"trades","data":[
            {"coin":"BTC","side":"B","px":"not-a-number","sz":"0.01","time":1,"tid":1}
        ]}"#;
        let event = HyperliquidDecoder::decode(raw).unwrap().unwrap();
        let evs = HyperliquidNormalizer::default().normalize(event);
        assert!(evs.is_empty());
    }

    #[test]
    fn subscribe_frames_filter_on_declared_datatypes() {
        use aetelier_types::config::markets::market_config::DeclaredDatatype as DD;
        let symbols = vec!["BTC".to_string()];
        let both = HyperliquidHooks.subscribe_frames(&symbols, &DeclaredSet::all());
        assert_eq!(both.len(), 2);
        let ob_only = HyperliquidHooks
            .subscribe_frames(&symbols, &DeclaredSet::only(DD::Orderbook));
        assert_eq!(ob_only.len(), 1);
        let Message::Text(body) = &ob_only[0] else {
            panic!("expected a text frame");
        };
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(v["method"], "subscribe");
        assert_eq!(v["subscription"]["type"], "l2Book");
        assert_eq!(v["subscription"]["coin"], "BTC");
        let trades_only =
            HyperliquidHooks.subscribe_frames(&symbols, &DeclaredSet::only(DD::Trades));
        assert_eq!(trades_only.len(), 1);
        let none = HyperliquidHooks.subscribe_frames(
            &symbols,
            &DeclaredSet::only(DD::Orderbook).without(DD::Orderbook),
        );
        assert!(none.is_empty());
    }

    #[test]
    fn decoder_dispatches_on_channel_and_swallows_control_frames() {
        let book =
            r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[],[]]}}"#;
        assert!(matches!(
            HyperliquidDecoder::decode(book).unwrap(),
            Some(HyperliquidWssEvent::Book(_))
        ));
        assert!(
            HyperliquidDecoder::decode(r#"{"channel":"subscriptionResponse","data":{}}"#)
                .unwrap()
                .is_none()
        );
        assert!(
            HyperliquidDecoder::decode(r#"{"channel":"pong"}"#)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn profile_uses_bare_coin_codec_full_refresh_and_ten_connection_cap() {
        let a = HyperliquidAdapter;
        assert_eq!(a.id(), "hyperliquid");
        assert!(matches!(
            a.profile().symbol_codec,
            SymbolCodec::BareCoin { quote: "USDC" }
        ));
        assert!(matches!(
            a.book_model("orderbook"),
            ReconstructionModel::FullRefresh
        ));
        assert_eq!(a.profile().budget.max_connections, Some(10));
    }
}
