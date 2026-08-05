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
            if declared.contains(DD::FundingRates) || declared.contains(DD::OpenInterest)
            {
                frames.push(subscribe_frame("activeAssetCtx", symbol));
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

pub struct HyperliquidNormalizer {
    pub metrics: SourceMetrics,
    pub declared: DeclaredSet,
}

impl Default for HyperliquidNormalizer {
    fn default() -> Self {
        Self {
            metrics: SourceMetrics::default(),
            declared: DeclaredSet::all(),
        }
    }
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
            HyperliquidWssEvent::AssetCtx(msg) => self.normalize_asset_ctx(msg),
        }
    }
}

impl HyperliquidNormalizer {
    fn normalize_asset_ctx(
        &self,
        msg: crate::sources::hyperliquid::responses::HyperliquidAssetCtxMsg,
    ) -> Vec<DomainEvent> {
        use aetelier_types::config::markets::market_config::DeclaredDatatype as DD;
        let Some(pair) = HYPERLIQUID_CODEC.decode(&msg.coin) else {
            tracing::warn!(symbol = %msg.coin, "hyperliquid.asset_ctx.bad_symbol");
            self.metrics.add_dropped_frames(1);
            return Vec::new();
        };
        let mut events = Vec::new();
        let premium = msg
            .ctx
            .premium
            .as_deref()
            .and_then(|p| p.parse::<rust_decimal::Decimal>().ok());
        let mark_px = msg
            .ctx
            .mark_px
            .as_deref()
            .and_then(|p| p.parse::<rust_decimal::Decimal>().ok());
        if self.declared.contains(DD::FundingRates)
            && let Some(raw) = msg.ctx.funding.as_deref()
        {
            match raw.parse::<rust_decimal::Decimal>() {
                Ok(rate) => {
                    events.push(DomainEvent::FundingRate(
                        aetelier_types::funding::FundingRate {
                            funding_rate_ts_us: 0,
                            local_funding_ts_us: 0,
                            recv_seq: 0,
                            conn_epoch: 0,
                            pair: pair.clone(),
                            funding_rate: rate,
                            premium,
                            interval_hours: 1,
                            next_funding_ts_us: 0,
                            exchange: "hyperliquid".to_string(),
                        },
                    ));
                }
                Err(_) => {
                    tracing::warn!(
                        symbol = %msg.coin,
                        "hyperliquid.asset_ctx.bad_funding_decimal"
                    );
                    self.metrics.add_dropped_frames(1);
                }
            }
        }
        if self.declared.contains(DD::OpenInterest)
            && let Some(raw) = msg.ctx.open_interest.as_deref()
        {
            match raw.parse::<rust_decimal::Decimal>() {
                Ok(oi) => {
                    events.push(DomainEvent::OpenInterest(
                        aetelier_types::open_interest::OpenInterest {
                            open_interest_ts_us: 0,
                            local_oi_ts_us: 0,
                            recv_seq: 0,
                            conn_epoch: 0,
                            pair,
                            open_interest: oi,
                            open_interest_value: None,
                            mark_px,
                            exchange: "hyperliquid".to_string(),
                        },
                    ));
                }
                Err(_) => {
                    tracing::warn!(
                        symbol = %msg.coin,
                        "hyperliquid.asset_ctx.bad_oi_decimal"
                    );
                    self.metrics.add_dropped_frames(1);
                }
            }
        }
        events
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

const HYPERLIQUID_INFO_URL: &str = "https://api.hyperliquid.xyz/info";

const SETTLEMENT_FETCH_DELAY_SECS: u64 = 5;

const SETTLEMENT_BACKFILL_MS: u64 = 24 * 3600 * 1000;

const SETTLEMENT_OVERLAP_MS: u64 = 2 * 3600 * 1000;

fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

async fn settlement_poller(
    symbols: Vec<String>,
    tx: mpsc::Sender<DomainEvent>,
    mut shutdown: watch::Receiver<bool>,
) {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "hyperliquid.settlement.client_build_failed");
            return;
        }
    };
    let mut start_ms = (now_us() / 1_000).saturating_sub(SETTLEMENT_BACKFILL_MS);
    loop {
        for symbol in &symbols {
            let mut attempts = 0;
            while attempts < 2 {
                attempts += 1;
                match fetch_settlements(&client, symbol, start_ms, &tx).await {
                    Ok(()) => break,
                    Err(e) => {
                        tracing::warn!(
                            symbol = %symbol,
                            attempt = attempts,
                            error = %e,
                            "hyperliquid.settlement.fetch_failed"
                        );
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    }
                }
            }
        }
        start_ms = (now_us() / 1_000).saturating_sub(SETTLEMENT_OVERLAP_MS);
        let now_ms = now_us() / 1_000;
        let next_hour_ms = (now_ms / 3_600_000 + 1) * 3_600_000;
        let wait_ms =
            next_hour_ms.saturating_sub(now_ms) + SETTLEMENT_FETCH_DELAY_SECS * 1_000;
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(wait_ms)) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}

async fn fetch_settlements(
    client: &reqwest::Client,
    symbol: &str,
    start_ms: u64,
    tx: &mpsc::Sender<DomainEvent>,
) -> Result<(), String> {
    use crate::sources::hyperliquid::responses::HyperliquidFundingHistoryRow;
    let body = serde_json::json!({
        "type": "fundingHistory",
        "coin": symbol,
        "startTime": start_ms,
    });
    let sent_at = std::time::Instant::now();
    let resp = client
        .post(HYPERLIQUID_INFO_URL)
        .json(&body)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("fundingHistory status {status}"));
    }
    let rows: Vec<HyperliquidFundingHistoryRow> =
        resp.json().await.map_err(|e| e.to_string())?;
    let rtt_us = sent_at.elapsed().as_micros() as u64;
    let local_ts_us = now_us();
    for row in rows {
        let Some(pair) = HYPERLIQUID_CODEC.decode(&row.coin) else {
            continue;
        };
        let Ok(rate) = row.funding_rate.parse::<rust_decimal::Decimal>() else {
            tracing::warn!(
                symbol = %row.coin,
                time = row.time,
                "hyperliquid.settlement.bad_decimal"
            );
            continue;
        };
        let premium = row
            .premium
            .as_deref()
            .and_then(|p| p.parse::<rust_decimal::Decimal>().ok());
        let fs = aetelier_types::funding::FundingSettlement {
            funding_time_us: epoch_to_us(row.time),
            local_ts_us,
            rtt_us,
            pair,
            funding_rate: rate,
            premium,
            exchange: "hyperliquid".to_string(),
        };
        if tx.send(DomainEvent::FundingSettlement(fs)).await.is_err() {
            return Ok(());
        }
    }
    Ok(())
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

    fn supported_datatypes(
        &self,
    ) -> &'static [aetelier_types::config::markets::market_config::DeclaredDatatype] {
        use aetelier_types::config::markets::market_config::DeclaredDatatype as DD;
        &[
            DD::Orderbook,
            DD::Trades,
            DD::FundingRates,
            DD::OpenInterest,
        ]
    }

    fn spawn(
        &self,
        symbols: Vec<String>,
        declared: DeclaredSet,
        tx: mpsc::Sender<DomainEvent>,
        shutdown: watch::Receiver<bool>,
        metrics: SourceMetrics,
    ) -> JoinHandle<TaskExit> {
        use aetelier_types::config::markets::market_config::DeclaredDatatype as DD;
        let poll_settlements = declared.contains(DD::FundingRates);
        let normalizer = HyperliquidNormalizer {
            metrics: metrics.clone(),
            declared: declared.clone(),
        };
        let poller_symbols = symbols.clone();
        let poller_tx = tx.clone();
        let poller_shutdown = shutdown.clone();
        tokio::spawn(async move {
            let poller = poll_settlements.then(|| {
                tokio::spawn(settlement_poller(
                    poller_symbols,
                    poller_tx,
                    poller_shutdown,
                ))
            });
            let exit =
                drive::<HyperliquidHooks, HyperliquidDecoder, HyperliquidNormalizer>(
                    Arc::new(HyperliquidHooks),
                    symbols,
                    declared,
                    normalizer,
                    tx,
                    shutdown,
                    DEFAULT_RAW_BUFFER,
                    metrics,
                )
                .await;
            if let Some(p) = poller {
                p.abort();
            }
            exit
        })
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
        let normalizer = HyperliquidNormalizer::default();
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
        assert_eq!(both.len(), 3);
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

    const CTX_FRAME: &str = r#"{"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{"funding":"0.0000125","openInterest":"12345.678","prevDayPx":"50000.0","dayNtlVlm":"123456789.0","markPx":"50100.5","midPx":"50100.0","oraclePx":"50099.0","premium":"0.00001","impactPxs":["50099.5","50100.5"],"dayBaseVlm":"2500.0"}}}"#;

    #[test]
    fn normalizes_asset_ctx_into_funding_and_oi_samples() {
        let event = HyperliquidDecoder::decode(CTX_FRAME).unwrap().unwrap();
        let evs = HyperliquidNormalizer::default().normalize(event);
        assert_eq!(evs.len(), 2);
        match &evs[0] {
            DomainEvent::FundingRate(fr) => {
                assert_eq!(fr.pair.to_canonical(), "BTC/USDC");
                assert_eq!(fr.funding_rate, "0.0000125".parse().unwrap());
                assert_eq!(fr.premium, Some("0.00001".parse().unwrap()));
                assert_eq!(fr.interval_hours, 1);
                assert_eq!(fr.funding_rate_ts_us, 0);
                assert_eq!(fr.exchange, "hyperliquid");
            }
            other => panic!("expected FundingRate, got {other:?}"),
        }
        match &evs[1] {
            DomainEvent::OpenInterest(oi) => {
                assert_eq!(oi.open_interest, "12345.678".parse().unwrap());
                assert_eq!(oi.mark_px, Some("50100.5".parse().unwrap()));
                assert_eq!(oi.open_interest_value, None);
                assert_eq!(oi.open_interest_ts_us, 0);
            }
            other => panic!("expected OpenInterest, got {other:?}"),
        }
    }

    #[test]
    fn asset_ctx_respects_the_declared_set() {
        use aetelier_types::config::markets::market_config::DeclaredDatatype as DD;
        let event = HyperliquidDecoder::decode(CTX_FRAME).unwrap().unwrap();
        let normalizer = HyperliquidNormalizer {
            metrics: SourceMetrics::default(),
            declared: DeclaredSet::only(DD::FundingRates),
        };
        let evs = normalizer.normalize(event);
        assert_eq!(evs.len(), 1);
        assert!(matches!(evs[0], DomainEvent::FundingRate(_)));

        let event = HyperliquidDecoder::decode(CTX_FRAME).unwrap().unwrap();
        let normalizer = HyperliquidNormalizer {
            metrics: SourceMetrics::default(),
            declared: DeclaredSet::only(DD::Orderbook),
        };
        assert!(normalizer.normalize(event).is_empty());
    }

    #[test]
    fn subscribe_adds_asset_ctx_only_for_derivative_datatypes() {
        use aetelier_types::config::markets::market_config::DeclaredDatatype as DD;
        let symbols = vec!["BTC".to_string()];
        let all = HyperliquidHooks.subscribe_frames(&symbols, &DeclaredSet::all());
        assert_eq!(all.len(), 3);
        let Message::Text(body) = &all[2] else {
            panic!("expected a text frame");
        };
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(v["subscription"]["type"], "activeAssetCtx");

        let funding_only = HyperliquidHooks
            .subscribe_frames(&symbols, &DeclaredSet::only(DD::FundingRates));
        assert_eq!(funding_only.len(), 1);

        let book_trades = HyperliquidHooks.subscribe_frames(
            &symbols,
            &DeclaredSet::only(DD::Orderbook)
                .without(DD::Orderbook)
                .without(DD::Trades),
        );
        assert!(book_trades.is_empty());
        let spot_style = HyperliquidHooks.subscribe_frames(
            &symbols,
            &DeclaredSet::all()
                .without(DD::FundingRates)
                .without(DD::OpenInterest)
                .without(DD::Liquidations),
        );
        assert_eq!(spot_style.len(), 2);
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
