use std::sync::{Arc, LazyLock};
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::protocol::Message;

use aetelier_types::exchanges::VenueEnvironment;
use aetelier_types::orderbooks::NormalizedDelta;
use aetelier_types::trades::{Trade, TradeSide};

use crate::framework::budget::{ConnectionBudget, RateWindow, SourceMetrics};
use crate::framework::driver::{DEFAULT_RAW_BUFFER, drive};
use crate::framework::model::{
    DomainEvent, Normalizer, ReconstructionModel, epoch_to_us,
};
use crate::framework::protocol::DeclaredSet;
use crate::framework::protocol::{AckOutcome, Heartbeat, ProtocolHooks};
use crate::framework::registry::{ExchangeAdapter, ExchangeProfile, TaskExit};
use crate::framework::symbol::SymbolCodec;

use crate::sources::hyperliquid::decoder::HyperliquidDecoder;
use crate::sources::hyperliquid::events::HyperliquidWssEvent;

const HYPERLIQUID_WSS_URL: &str = "wss://api.hyperliquid.xyz/ws";

const HYPERLIQUID_TESTNET_WSS_URL: &str = "wss://api.hyperliquid-testnet.xyz/ws";

const PING_SECS: u64 = 30;

const ACK_DEADLINE_SECS: u64 = 10;

const STALE_AFTER_SECS: u64 = 45;

const L2_BOOK_LEVELS: usize = 20;

const HYPERLIQUID_CODEC: SymbolCodec = SymbolCodec::BareCoin { quote: "USDC" };

#[derive(Debug, Clone, Copy, Default)]
pub struct HyperliquidHooks {
    pub environment: VenueEnvironment,
}

impl HyperliquidHooks {
    pub fn new(environment: VenueEnvironment) -> Self {
        Self { environment }
    }
}

fn wss_url(environment: VenueEnvironment) -> &'static str {
    match environment {
        VenueEnvironment::Production => HYPERLIQUID_WSS_URL,
        VenueEnvironment::Testnet => HYPERLIQUID_TESTNET_WSS_URL,
    }
}

fn info_url(environment: VenueEnvironment) -> &'static str {
    match environment {
        VenueEnvironment::Production => HYPERLIQUID_INFO_URL,
        VenueEnvironment::Testnet => HYPERLIQUID_TESTNET_INFO_URL,
    }
}

impl ProtocolHooks for HyperliquidHooks {
    fn endpoint(&self) -> String {
        wss_url(self.environment).to_string()
    }

    fn subscribe_frames(
        &self,
        symbols: &[String],
        declared: &DeclaredSet,
    ) -> Vec<Message> {
        let channels = declared_channels(declared);
        let mut frames = Vec::new();
        for symbol in symbols {
            for channel in &channels {
                frames.push(subscribe_frame(channel, symbol));
            }
        }
        frames
    }

    fn unsubscribe_frames(
        &self,
        symbols: &[String],
        declared: &DeclaredSet,
    ) -> Vec<Message> {
        let channels = declared_channels(declared);
        let mut frames = Vec::new();
        for symbol in symbols {
            for channel in &channels {
                frames.push(method_frame("unsubscribe", channel, symbol));
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

    fn subscribe_ack_deadline(&self) -> Option<Duration> {
        Some(Duration::from_secs(ACK_DEADLINE_SECS))
    }

    fn stale_after(&self) -> Duration {
        Duration::from_secs(STALE_AFTER_SECS)
    }

    fn classify_ack(&self, text: &str) -> AckOutcome {
        if !text.contains("\"subscriptionResponse\"") && !text.contains("\"error\"") {
            return AckOutcome::NotAck;
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
            return AckOutcome::NotAck;
        };
        match v.get("channel").and_then(|c| c.as_str()) {
            Some("subscriptionResponse") => {
                match v.pointer("/data/method").and_then(|m| m.as_str()) {
                    Some("subscribe") => AckOutcome::Accepted,
                    _ => AckOutcome::NotAck,
                }
            }
            Some("error") => AckOutcome::Rejected(
                v.get("data")
                    .and_then(|d| d.as_str())
                    .unwrap_or("subscribe failed")
                    .to_string(),
            ),
            _ => AckOutcome::NotAck,
        }
    }
}

fn subscribe_frame(channel: &str, symbol: &str) -> Message {
    method_frame("subscribe", channel, symbol)
}

fn method_frame(method: &str, channel: &str, symbol: &str) -> Message {
    Message::Text(
        serde_json::json!({
            "method": method,
            "subscription": { "type": channel, "coin": symbol }
        })
        .to_string()
        .into(),
    )
}

fn declared_channels(declared: &DeclaredSet) -> Vec<&'static str> {
    use aetelier_types::config::markets::market_config::DeclaredDatatype as DD;
    let mut channels = Vec::new();
    if declared.contains(DD::Orderbook) {
        channels.push("l2Book");
    }
    if declared.contains(DD::Trades) {
        channels.push("trades");
    }
    if declared.contains(DD::FundingRates) || declared.contains(DD::OpenInterest) {
        channels.push("activeAssetCtx");
    }
    channels
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

const HYPERLIQUID_TESTNET_INFO_URL: &str = "https://api.hyperliquid-testnet.xyz/info";

const SETTLEMENT_FETCH_DELAY_SECS: u64 = 5;

const SETTLEMENT_BACKFILL_MS: u64 = 3600 * 1000;

const SETTLEMENT_OVERLAP_MS: u64 = 3600 * 1000;

fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

async fn settlement_poller(
    environment: VenueEnvironment,
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
    tracing::info!(
        symbols = %symbols.join(","),
        start_ms,
        interval_ms = 3_600_000u64,
        "hyperliquid.settlement.window — covers one interval from operational start; \
         context downtime is not recovered here"
    );
    loop {
        let cycle_hour_ms = (now_us() / 1_000 / 3_600_000) * 3_600_000;
        for symbol in &symbols {
            let mut attempts = 0;
            let mut sent: Option<usize> = None;
            while attempts < 2 {
                attempts += 1;
                match fetch_settlements(&client, environment, symbol, start_ms, &tx).await
                {
                    Ok(n) => {
                        sent = Some(n);
                        break;
                    }
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
            match sent {
                Some(n) => tracing::info!(
                    symbol = %symbol,
                    hour_ms = cycle_hour_ms,
                    rows = n,
                    "hyperliquid.settlement.cycle"
                ),
                None => tracing::warn!(
                    symbol = %symbol,
                    hour_ms = cycle_hour_ms,
                    "hyperliquid.settlement.gap — this hour was not collected; \
                     recover with an explicit backfill unit"
                ),
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
    environment: VenueEnvironment,
    symbol: &str,
    start_ms: u64,
    tx: &mpsc::Sender<DomainEvent>,
) -> Result<usize, String> {
    use crate::sources::hyperliquid::responses::HyperliquidFundingHistoryRow;
    let body = serde_json::json!({
        "type": "fundingHistory",
        "coin": symbol,
        "startTime": start_ms,
    });
    let sent_at = std::time::Instant::now();
    let resp = client
        .post(info_url(environment))
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
    let mut sent = 0usize;
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
            return Ok(sent);
        }
        sent += 1;
    }
    Ok(sent)
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

    fn resubscribe_replays_trades(&self) -> bool {
        true
    }

    fn max_declared_depth(&self) -> Option<usize> {
        Some(L2_BOOK_LEVELS)
    }

    fn supports_environment(&self, _environment: VenueEnvironment) -> bool {
        true
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
        self.spawn_env(
            VenueEnvironment::Production,
            symbols,
            declared,
            tx,
            shutdown,
            metrics,
        )
    }

    fn spawn_env(
        &self,
        environment: VenueEnvironment,
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
                    environment,
                    poller_symbols,
                    poller_tx,
                    poller_shutdown,
                ))
            });
            let exit =
                drive::<HyperliquidHooks, HyperliquidDecoder, HyperliquidNormalizer>(
                    Arc::new(HyperliquidHooks::new(environment)),
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
        HyperliquidHooks::default()
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

    const ACK_L2BOOK: &str = r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC","nSigFigs":null,"mantissa":null,"fast":false}}}"#;
    const ACK_TRADES: &str = r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}}"#;
    const ACK_ASSET_CTX: &str = r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"activeAssetCtx","coin":"BTC"}}}"#;

    #[test]
    fn classify_ack_accepts_real_subscription_response_echoes() {
        let hooks = HyperliquidHooks::default();
        for frame in [ACK_L2BOOK, ACK_TRADES, ACK_ASSET_CTX] {
            assert!(matches!(hooks.classify_ack(frame), AckOutcome::Accepted));
        }
        let echo: serde_json::Value = serde_json::from_str(ACK_L2BOOK).unwrap();
        let sub = echo.pointer("/data/subscription").unwrap();
        assert!(sub.get("nSigFigs").unwrap().is_null());
        assert!(sub.get("mantissa").unwrap().is_null());
        assert_eq!(sub.get("fast").and_then(|f| f.as_bool()), Some(false));
    }

    #[test]
    fn trades_stay_sequence_unarmed_so_replay_is_deduped_by_id() {
        let adapter = HyperliquidAdapter;
        assert!(adapter.resubscribe_replays_trades());
        let raw = r#"{"channel":"trades","data":[
            {"coin":"BTC","side":"B","px":"1","sz":"1","time":1,"tid":259289531908492,"users":["0xa","0xb"],"hash":"0xc"}
        ]}"#;
        let event = HyperliquidDecoder::decode(raw).unwrap().unwrap();
        let evs = HyperliquidNormalizer::default().normalize(event);
        match &evs[0] {
            DomainEvent::Trade { trade, sequence } => {
                assert_eq!(
                    sequence, &None,
                    "hyperliquid tids are unordered, so no sequence may be fabricated"
                );
                assert_eq!(trade.id, "259289531908492");
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn endpoint_follows_environment() {
        assert_eq!(
            HyperliquidHooks::default().endpoint(),
            "wss://api.hyperliquid.xyz/ws",
            "an unset environment stays on mainnet"
        );
        assert_eq!(
            HyperliquidHooks::new(VenueEnvironment::Production).endpoint(),
            "wss://api.hyperliquid.xyz/ws"
        );
        assert_eq!(
            HyperliquidHooks::new(VenueEnvironment::Testnet).endpoint(),
            "wss://api.hyperliquid-testnet.xyz/ws"
        );
    }

    #[test]
    fn settlement_info_url_follows_environment() {
        assert_eq!(
            info_url(VenueEnvironment::Production),
            "https://api.hyperliquid.xyz/info"
        );
        assert_eq!(
            info_url(VenueEnvironment::Testnet),
            "https://api.hyperliquid-testnet.xyz/info"
        );
    }

    #[test]
    fn stale_after_undercuts_the_venue_sixty_second_close_rule() {
        let stale = HyperliquidHooks::default().stale_after();
        assert!(
            stale < Duration::from_secs(60),
            "must detect silence before the venue closes the socket"
        );
        assert!(
            Duration::from_secs(PING_SECS) < stale,
            "our own ping cadence must refresh the socket inside the window"
        );
    }

    #[test]
    fn unsubscribe_frames_mirror_subscribe_frames_per_channel() {
        let symbols = vec!["BTC".to_string()];
        let declared = DeclaredSet::all();
        let subs = HyperliquidHooks::default().subscribe_frames(&symbols, &declared);
        let unsubs = HyperliquidHooks::default().unsubscribe_frames(&symbols, &declared);
        assert_eq!(subs.len(), unsubs.len());
        for (sub, unsub) in subs.iter().zip(unsubs.iter()) {
            let (Message::Text(s), Message::Text(u)) = (sub, unsub) else {
                panic!("expected text frames");
            };
            let s: serde_json::Value = serde_json::from_str(s).unwrap();
            let u: serde_json::Value = serde_json::from_str(u).unwrap();
            assert_eq!(s["method"], "subscribe");
            assert_eq!(u["method"], "unsubscribe");
            assert_eq!(s["subscription"], u["subscription"]);
        }
    }

    #[test]
    fn subscribe_ack_deadline_is_ten_seconds() {
        assert_eq!(
            HyperliquidHooks::default().subscribe_ack_deadline(),
            Some(Duration::from_secs(10))
        );
    }

    #[test]
    fn classify_ack_rejects_error_channel_with_reason() {
        let frame = r#"{"channel":"error","data":"Error parsing JSON into valid websocket request: {}"}"#;
        match HyperliquidHooks::default().classify_ack(frame) {
            AckOutcome::Rejected(reason) => {
                assert!(reason.contains("Error parsing JSON"), "{reason}");
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[test]
    fn classify_ack_ignores_data_pong_and_unsubscribe_echoes() {
        let hooks = HyperliquidHooks::default();
        let unsubscribe_echo = r#"{"channel":"subscriptionResponse","data":{"method":"unsubscribe","subscription":{"type":"trades","coin":"BTC"}}}"#;
        let trade = r#"{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"1","sz":"1","time":1,"tid":1,"users":["0xa","0xb"],"hash":"0xc"}]}"#;
        for frame in [unsubscribe_echo, trade, r#"{"channel":"pong"}"#] {
            assert!(matches!(hooks.classify_ack(frame), AckOutcome::NotAck));
        }
    }

    #[test]
    fn decoder_swallows_error_channel_and_unknown_channels_as_none() {
        for frame in [
            r#"{"channel":"error","data":"boom"}"#,
            r#"{"channel":"pong"}"#,
            ACK_L2BOOK,
            r#"{"channel":"someFutureChannel","data":{}}"#,
        ] {
            assert!(HyperliquidDecoder::decode(frame).unwrap().is_none());
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
        let both =
            HyperliquidHooks::default().subscribe_frames(&symbols, &DeclaredSet::all());
        assert_eq!(both.len(), 3);
        let ob_only = HyperliquidHooks::default()
            .subscribe_frames(&symbols, &DeclaredSet::only(DD::Orderbook));
        assert_eq!(ob_only.len(), 1);
        let Message::Text(body) = &ob_only[0] else {
            panic!("expected a text frame");
        };
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(v["method"], "subscribe");
        assert_eq!(v["subscription"]["type"], "l2Book");
        assert_eq!(v["subscription"]["coin"], "BTC");
        let trades_only = HyperliquidHooks::default()
            .subscribe_frames(&symbols, &DeclaredSet::only(DD::Trades));
        assert_eq!(trades_only.len(), 1);
        let none = HyperliquidHooks::default().subscribe_frames(
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
        let all =
            HyperliquidHooks::default().subscribe_frames(&symbols, &DeclaredSet::all());
        assert_eq!(all.len(), 3);
        let Message::Text(body) = &all[2] else {
            panic!("expected a text frame");
        };
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(v["subscription"]["type"], "activeAssetCtx");

        let funding_only = HyperliquidHooks::default()
            .subscribe_frames(&symbols, &DeclaredSet::only(DD::FundingRates));
        assert_eq!(funding_only.len(), 1);

        let book_trades = HyperliquidHooks::default().subscribe_frames(
            &symbols,
            &DeclaredSet::only(DD::Orderbook)
                .without(DD::Orderbook)
                .without(DD::Trades),
        );
        assert!(book_trades.is_empty());
        let spot_style = HyperliquidHooks::default().subscribe_frames(
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
