//! Shared ingestion core used by both [`DataWorker`] and [`MarketWorker`].
//!
//! [`IngestionCore`] encapsulates the full reconnection loop, exchange
//! client spawning, event classification, health monitoring, and gap
//! detection.  It sends classified [`TopicMessage`]s to an
//! `mpsc::Sender<TopicMessage>` — the owning worker decides what to do
//! with them (publish to sinks, feed into a synchroniser, etc.).
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────┐     mpsc     ┌──────────────┐    mpsc    ┌──────────┐
//! │ ExchangeWSS  │─────────────→│ IngestionCore │───────────→│  Worker  │
//! │ Client Task  │  raw events  │ (reconnect +  │  TopicMsg  │ (output  │
//! │              │              │  classify)    │            │  sinks)  │
//! └──────────────┘              └──────────────┘            └──────────┘
//! ```

// This is the legacy raw-ingestion engine: it is itself the deprecated path
// (superseded by the framework runtime), so it constructs the deprecated
// per-venue WssClients and ExchangeEvent by design. Suppress the deprecation
// lint here rather than at every internal call site.
#![allow(deprecated)]

use std::time::Duration;

use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::clients::connection_manager::{ConnectionManager, ConnectionManagerConfig};
use crate::clients::connection_state::ConnectionState;
use crate::clients::disconnect::DisconnectReason;
use crate::clients::reconnect::{HealthMonitor, ReconnectAction};
use crate::config::workers::CommonWorkerFields;
use crate::errors::ConnectError;
use crate::sources::ExchangeEvent;
use crate::sources::binance::client::BinanceWssClient;
use crate::sources::binance::events::BinanceWssEvent;
use crate::sources::bybit::client::BybitWssClient;
use crate::sources::bybit::events::BybitWssEvent;
use crate::sources::coinbase::client::CoinbaseWssClient;
use crate::sources::coinbase::events::CoinbaseWssEvent;
use crate::sources::gateio::client::GateioWssClient;
use crate::sources::gateio::events::GateioWssEvent;
use crate::sources::kraken::client::KrakenWssClient;
use crate::sources::kraken::events::KrakenWssEvent;
use crate::sources::okx::client::OkxWssClient;
use crate::sources::okx::events::OkxWssEvent;
use aetelier_types::config::markets::market_config::DataTypesSection;
use aetelier_types::exchanges::Exchange;

use super::gap_detector::{GapDetectorSet, GapStats};
use super::topic_publisher::TopicMessage;

#[cfg(feature = "telemetry")]
use aetelier_telemetry::attributes;
#[cfg(feature = "telemetry")]
use aetelier_telemetry::meters::IngestionMeters;

// ─────────────────────────────────────────────────────────────────────────────
// IngestionReport
// ─────────────────────────────────────────────────────────────────────────────

/// Summary statistics returned when an [`IngestionCore`] finishes.
#[derive(Debug, Clone)]
pub struct IngestionReport {
    /// Exchange name (e.g. `"bybit"`).
    pub exchange: String,
    /// Trading pair (e.g. `"BTCUSDT"`).
    pub symbol: String,
    /// Total raw events classified and forwarded.
    pub total_events: u64,
    /// Per-topic event counts.
    pub events_by_topic: Vec<(String, u64)>,
    /// Wall-clock seconds the core was active.
    pub elapsed_secs: f64,
    /// `total_events / elapsed_secs`.
    pub effective_rate: f64,
    /// Number of reconnection attempts.
    pub reconnect_count: u32,
    /// Per-topic gap statistics.
    pub gap_stats: Vec<GapStats>,
    /// Non-fatal errors logged during the run.
    pub errors: Vec<String>,
}

// ─────────────────────────────────────────────────────────────────────────────
// IngestionCore
// ─────────────────────────────────────────────────────────────────────────────

/// Shared ingestion engine composed by both worker types.
///
/// Owns the reconnection loop, exchange client lifecycle, event
/// classification, health monitoring, and gap detection.  Sends
/// classified events as [`TopicMessage`]s to the provided sender.
pub struct IngestionCore {
    /// Parsed exchange enum.
    exchange: Exchange,
    /// Shared config fields.
    common: CommonWorkerFields,
    /// Human-readable label (e.g. `"bybit:BTCUSDT"`).
    label: String,
    /// Gap detection silence threshold.
    gap_threshold: Duration,
    /// Reconnection configuration.
    reconnect_config: ConnectionManagerConfig,
    /// Staleness timeout for `HealthMonitor`.
    staleness_timeout: Duration,
    /// Topic names this core will classify events into.
    topics: Vec<String>,

    /// Optional OTel instrument handles for metrics recording.
    #[cfg(feature = "telemetry")]
    meters: Option<IngestionMeters>,
}

impl IngestionCore {
    /// Create a new ingestion core from common worker fields.
    pub fn new(common: CommonWorkerFields) -> Result<Self, ConnectError> {
        let exchange: Exchange =
            common.exchange.parse().map_err(ConnectError::Exchange)?;
        let label = format!("{}:{}", common.exchange, common.symbol);
        let gap_threshold = common.gap_threshold();
        let reconnect_config = common.reconnect_config();
        let staleness_timeout = common.staleness_timeout();
        let topics = wss_topic_names(&common.exchange, &common.symbol, &common.datatypes);

        Ok(Self {
            exchange,
            common,
            label,
            gap_threshold,
            reconnect_config,
            staleness_timeout,
            topics,
            #[cfg(feature = "telemetry")]
            meters: None,
        })
    }

    /// Attach OTel instrument handles for metrics recording.
    ///
    /// When set, the ingestion loop records `messages_received`,
    /// `event_latency_ms`, and connection state transitions.
    #[cfg(feature = "telemetry")]
    pub fn with_meters(mut self, meters: IngestionMeters) -> Self {
        self.meters = Some(meters);
        self
    }

    /// The topic names this core classifies events into.
    pub fn topics(&self) -> &[String] {
        &self.topics
    }

    /// Human-readable label.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// The exchange this core connects to.
    pub fn exchange_name(&self) -> &str {
        &self.common.exchange
    }

    /// The symbol this core collects.
    pub fn symbol(&self) -> &str {
        &self.common.symbol
    }

    /// The datatypes configuration.
    pub fn datatypes(&self) -> &DataTypesSection {
        &self.common.datatypes
    }

    /// The raw TOML `[reconnect]` section, when the config carried one.
    /// Raw (not the resolved [`ConnectionManagerConfig`]) so the framework
    /// path can apply its own defaults to unset fields.
    pub fn reconnect_section(
        &self,
    ) -> Option<&crate::config::workers::common::ReconnectSection> {
        self.common.reconnect.as_ref()
    }

    /// Run the ingestion loop, sending classified events to `event_tx`.
    ///
    /// Runs until `shutdown` signals `true` or a non-retryable error
    /// occurs.  Returns an [`IngestionReport`] with session statistics.
    pub async fn run(
        self,
        mut shutdown: watch::Receiver<bool>,
        event_tx: mpsc::Sender<TopicMessage>,
    ) -> anyhow::Result<IngestionReport> {
        let exchange = self.exchange;
        let exchange_name = self.common.exchange.clone();
        let symbol = self.common.symbol.clone();
        let ob_depth = self.common.datatypes.orderbook.depth;
        let datatypes = self.common.datatypes.clone();
        let start = Instant::now();

        // ── Gap detectors ───────────────────────────────────────────────
        let topic_refs: Vec<&str> = self.topics.iter().map(|s| s.as_str()).collect();
        let mut gaps = GapDetectorSet::new(&topic_refs, self.gap_threshold);

        // ── Connection manager ──────────────────────────────────────────
        let mut manager =
            ConnectionManager::new(self.label.clone(), self.reconnect_config.clone());

        // ── Counters ────────────────────────────────────────────────────
        let mut total_events: u64 = 0;
        let mut reconnect_count: u32 = 0;
        let mut events_by_topic: std::collections::HashMap<String, u64> =
            std::collections::HashMap::new();
        let mut errors: Vec<String> = Vec::new();

        tracing::info!(
            label = self.label.as_str(),
            exchange = %exchange,
            topics = ?topic_refs,
            gap_threshold_ms = self.gap_threshold.as_millis() as u64,
            staleness_timeout_ms = self.staleness_timeout.as_millis() as u64,
            "ingestion_core.started"
        );

        // ── Reconnection loop ───────────────────────────────────────────
        'reconnect: loop {
            if *shutdown.borrow() {
                break;
            }

            // ── Connect ─────────────────────────────────────────────────
            manager.transition(ConnectionState::Connecting, "initiating connection");

            let (tx, mut rx) = mpsc::channel::<ExchangeEvent>(2048);
            let mut wss_handle = Some(spawn_exchange_task(
                exchange,
                &exchange_name,
                &symbol,
                &datatypes,
                tx,
            ));

            manager.transition(
                ConnectionState::Subscribing,
                "exchange client spawned, awaiting first event",
            );

            let mut health = HealthMonitor::new(self.staleness_timeout);
            let mut first_event = true;

            // ── Event loop ──────────────────────────────────────────────
            let mut disconnect_reason = DisconnectReason::CleanClose;

            loop {
                tokio::select! {
                    result = shutdown.changed() => {
                        if result.is_err() || *shutdown.borrow() {
                            if let Some(h) = wss_handle.take() {
                                h.abort();
                            }
                            break 'reconnect;
                        }
                    }

                    _ = tokio::time::sleep_until(health.deadline()) => {
                        disconnect_reason = DisconnectReason::StaleConnection {
                            silence_duration: self.staleness_timeout,
                        };
                        tracing::warn!(
                            label = self.label.as_str(),
                            timeout_ms = self.staleness_timeout.as_millis() as u64,
                            "ingestion_core.stale_connection_detected"
                        );
                        break;
                    }

                    event = rx.recv() => {
                        match event {
                            Some(event) => {
                                health.record_activity();

                                if first_event {
                                    manager.transition(
                                        ConnectionState::Streaming,
                                        "first event received",
                                    );
                                    manager.on_connected();
                                    first_event = false;

                                    // ── OTel: record connection state → Streaming ──
                                    #[cfg(feature = "telemetry")]
                                    if let Some(ref meters) = self.meters {
                                        let state_code = aetelier_telemetry::meters::connection_state_code("streaming");
                                        let worker_attrs = attributes::worker_attributes(
                                            &exchange_name,
                                            &symbol,
                                            &format!("{}", self.common.market_type),
                                            &self.label,
                                        );
                                        meters.set_connection_state(state_code, &worker_attrs);
                                    }
                                }

                                let now_us = wall_clock_us();

                                let topic_names = classify_event(
                                    &event, &exchange_name, &symbol, ob_depth,
                                    &datatypes,
                                );

                                // ── OTel attribute set (computed once per event batch) ──
                                #[cfg(feature = "telemetry")]
                                let otel_attrs: Option<Vec<opentelemetry::KeyValue>> =
                                    self.meters.as_ref().map(|_| {
                                        attributes::event_attributes(
                                            &exchange_name,
                                            &symbol,
                                            &self.label,
                                            topic_names.first().map(|s| s.as_str()).unwrap_or(""),
                                        )
                                    });

                                for topic in &topic_names {
                                    let msg = TopicMessage {
                                        topic: topic.clone(),
                                        received_at_us: now_us,
                                        exchange: exchange_name.clone(),
                                        payload: event.clone(),
                                    };

                                    if event_tx.send(msg).await.is_err() {
                                        // Receiver dropped — caller shut down.
                                        tracing::info!(
                                            label = self.label.as_str(),
                                            "ingestion_core.event_tx_closed"
                                        );
                                        if let Some(h) = wss_handle.take() {
                                            h.abort();
                                        }
                                        break 'reconnect;
                                    }

                                    gaps.record_event(topic);
                                    *events_by_topic.entry(topic.clone()).or_insert(0) += 1;
                                    total_events += 1;

                                    // ── OTel: record message counter ──
                                    #[cfg(feature = "telemetry")]
                                    if let Some(ref meters) = self.meters {
                                        if let Some(ref attrs) = otel_attrs {
                                            meters.record_event(attrs);
                                        }
                                    }
                                }
                            }
                            None => {
                                if let Some(h) = wss_handle.take() {
                                    disconnect_reason = resolve_disconnect_reason(h).await;
                                }
                                break;
                            }
                        }
                    }
                }
            }

            // ── Clean up lingering exchange task ──────────────────────
            if let Some(h) = wss_handle.take() {
                h.abort();
            }

            // ── Handle disconnect ────────────────────────────────────────
            if *shutdown.borrow() {
                break;
            }

            reconnect_count += 1;
            let action = manager.on_disconnect(&disconnect_reason);

            match action {
                ReconnectAction::RetryAfter(d) => {
                    let msg = format!(
                        "reconnecting in {:?} (attempt {}): {}",
                        d,
                        manager.consecutive_failures(),
                        disconnect_reason,
                    );
                    tracing::warn!(label = self.label.as_str(), "{}", msg);
                    errors.push(msg);

                    tokio::select! {
                        _ = tokio::time::sleep(d) => {}
                        result = shutdown.changed() => {
                            if result.is_err() || *shutdown.borrow() {
                                break;
                            }
                        }
                    }
                }
                ReconnectAction::RetryImmediately => {
                    tracing::info!(
                        label = self.label.as_str(),
                        "ingestion_core.retry_immediately"
                    );
                }
                ReconnectAction::GiveUp { reason } => {
                    let msg = format!("giving up: {}", reason);
                    tracing::error!(label = self.label.as_str(), "{}", msg);
                    errors.push(msg);
                    break;
                }
                ReconnectAction::CircuitOpen { until } => {
                    let remaining = until.duration_since(Instant::now());
                    let msg = format!("circuit breaker open, waiting {:?}", remaining);
                    tracing::error!(label = self.label.as_str(), "{}", msg);
                    errors.push(msg);

                    tokio::select! {
                        _ = tokio::time::sleep(remaining) => {}
                        result = shutdown.changed() => {
                            if result.is_err() || *shutdown.borrow() {
                                break;
                            }
                        }
                    }
                }
            }
        }

        // ── Report ──────────────────────────────────────────────────────
        let elapsed = start.elapsed().as_secs_f64();

        let report = IngestionReport {
            exchange: exchange_name.clone(),
            symbol: symbol.clone(),
            total_events,
            events_by_topic: events_by_topic.into_iter().collect(),
            elapsed_secs: elapsed,
            effective_rate: if elapsed > 0.0 {
                total_events as f64 / elapsed
            } else {
                0.0
            },
            reconnect_count,
            gap_stats: gaps.stats(),
            errors,
        };

        tracing::info!(
            label = self.label.as_str(),
            total_events = total_events,
            elapsed_secs = format!("{:.1}", elapsed),
            transitions = manager.transitions().len(),
            "ingestion_core.stopped"
        );

        Ok(report)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Exchange task spawning — enum-driven factory
// ─────────────────────────────────────────────────────────────────────────────

/// Spawn the appropriate exchange WSS client task.
///
/// Dispatches on the [`Exchange`] enum — not a string match — so adding a
/// new exchange is a compiler-enforced exhaustive-match addition.
fn spawn_exchange_task(
    exchange: Exchange,
    exchange_name: &str,
    symbol: &str,
    datatypes: &DataTypesSection,
    tx: mpsc::Sender<ExchangeEvent>,
) -> JoinHandle<DisconnectReason> {
    let streams = wss_streams(exchange_name, symbol, datatypes);
    let symbol = symbol.to_string();

    match exchange {
        Exchange::Bybit => {
            let client = BybitWssClient::new(streams);
            tokio::spawn(async move {
                let (inner_tx, mut inner_rx) = mpsc::channel::<BybitWssEvent>(2048);
                let recv_handle =
                    tokio::spawn(async move { client.receive_data(inner_tx).await });
                while let Some(event) = inner_rx.recv().await {
                    if tx.send(ExchangeEvent::Bybit(event)).await.is_err() {
                        recv_handle.abort();
                        return DisconnectReason::ReceiverDropped;
                    }
                }
                collect_exit_reason(recv_handle, "bybit").await
            })
        }

        Exchange::Coinbase => {
            let product_ids = vec![symbol];
            let client = CoinbaseWssClient::new(streams, product_ids);
            tokio::spawn(async move {
                let (inner_tx, mut inner_rx) = mpsc::channel::<CoinbaseWssEvent>(2048);
                let recv_handle =
                    tokio::spawn(async move { client.receive_data(inner_tx).await });
                while let Some(event) = inner_rx.recv().await {
                    if tx.send(ExchangeEvent::Coinbase(event)).await.is_err() {
                        recv_handle.abort();
                        return DisconnectReason::ReceiverDropped;
                    }
                }
                collect_exit_reason(recv_handle, "coinbase").await
            })
        }

        Exchange::Kraken => {
            let symbols = vec![symbol];
            let book_depth = datatypes.orderbook.depth;
            let client = KrakenWssClient::new(streams, symbols, book_depth);
            tokio::spawn(async move {
                let (inner_tx, mut inner_rx) = mpsc::channel::<KrakenWssEvent>(2048);
                let recv_handle =
                    tokio::spawn(async move { client.receive_data(inner_tx).await });
                while let Some(event) = inner_rx.recv().await {
                    if tx.send(ExchangeEvent::Kraken(event)).await.is_err() {
                        recv_handle.abort();
                        return DisconnectReason::ReceiverDropped;
                    }
                }
                collect_exit_reason(recv_handle, "kraken").await
            })
        }

        Exchange::Binance => {
            let client = BinanceWssClient::new(streams);
            tokio::spawn(async move {
                let (inner_tx, mut inner_rx) = mpsc::channel::<BinanceWssEvent>(2048);
                let recv_handle =
                    tokio::spawn(async move { client.receive_data(inner_tx).await });
                while let Some(event) = inner_rx.recv().await {
                    if tx.send(ExchangeEvent::Binance(event)).await.is_err() {
                        recv_handle.abort();
                        return DisconnectReason::ReceiverDropped;
                    }
                }
                collect_exit_reason(recv_handle, "binance").await
            })
        }

        Exchange::Okx => {
            let inst_ids = vec![symbol];
            let client = OkxWssClient::new(streams, inst_ids);
            tokio::spawn(async move {
                let (inner_tx, mut inner_rx) = mpsc::channel::<OkxWssEvent>(2048);
                let recv_handle =
                    tokio::spawn(async move { client.receive_data(inner_tx).await });
                while let Some(event) = inner_rx.recv().await {
                    if tx.send(ExchangeEvent::Okx(event)).await.is_err() {
                        recv_handle.abort();
                        return DisconnectReason::ReceiverDropped;
                    }
                }
                collect_exit_reason(recv_handle, "okx").await
            })
        }

        Exchange::Gateio => {
            let ob_level = datatypes.orderbook.depth;
            let client = GateioWssClient::new(streams, symbol, ob_level);
            tokio::spawn(async move {
                let (inner_tx, mut inner_rx) = mpsc::channel::<GateioWssEvent>(2048);
                let recv_handle =
                    tokio::spawn(async move { client.receive_data(inner_tx).await });
                while let Some(event) = inner_rx.recv().await {
                    if tx.send(ExchangeEvent::Gateio(event)).await.is_err() {
                        recv_handle.abort();
                        return DisconnectReason::ReceiverDropped;
                    }
                }
                collect_exit_reason(recv_handle, "gateio").await
            })
        }

        // Framework-only venues (Upbit/Poloniex/HTX/KuCoin/Bitget/Bitso) have no
        // legacy raw-path source client — they are served by the framework
        // ingest path. Reaching here means a legacy DataWorker was configured for
        // one of them; return a non-retryable rejection rather than reconnecting.
        other => {
            tracing::error!(
                venue = %other,
                "ingestion_core.legacy_path_unsupported_venue (set framework_ingest = true)"
            );
            tokio::spawn(async move {
                DisconnectReason::ProtocolRejection {
                    code: 1003,
                    reason: format!(
                        "{other} has no legacy raw-path source; set framework_ingest = true"
                    ),
                }
            })
        }
    }
}

/// Collect the [`WssExitReason`] from a spawned exchange client task.
async fn collect_exit_reason(
    handle: JoinHandle<crate::clients::disconnect::WssExitReason>,
    exchange_label: &str,
) -> DisconnectReason {
    match handle.await {
        Ok(exit_reason) => {
            tracing::debug!(
                exchange = exchange_label,
                exit_reason = %exit_reason,
                "exchange_client.exited"
            );
            DisconnectReason::from(exit_reason)
        }
        Err(join_err) => {
            tracing::error!(
                exchange = exchange_label,
                error = %join_err,
                "exchange_client.task_panicked"
            );
            DisconnectReason::TransportError {
                source: format!("{exchange_label} task panicked: {join_err}").into(),
            }
        }
    }
}

/// Await the outer exchange task handle.
async fn resolve_disconnect_reason(
    handle: JoinHandle<DisconnectReason>,
) -> DisconnectReason {
    match handle.await {
        Ok(reason) => reason,
        Err(join_err) => DisconnectReason::TransportError {
            source: format!("exchange task join error: {join_err}").into(),
        },
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// WSS stream / topic construction
// ─────────────────────────────────────────────────────────────────────────────

/// Build exchange-specific WSS subscription topics from common fields.
///
/// This replicates the logic in `MarketSnapshotConfig::wss_streams()`
/// but takes decomposed parameters instead of the full config struct.
pub fn wss_streams(
    exchange: &str,
    symbol: &str,
    datatypes: &DataTypesSection,
) -> Vec<String> {
    match exchange.to_lowercase().as_str() {
        "bybit" => {
            let mut streams = Vec::new();
            if datatypes.orderbook.enabled {
                streams.push(format!(
                    "orderbook.{}.{}",
                    datatypes.orderbook.depth, symbol
                ));
            }
            if datatypes.trades.enabled {
                streams.push(format!("publicTrade.{}", symbol));
            }
            if datatypes.liquidations.enabled {
                streams.push(format!("allLiquidation.{}", symbol));
            }
            if datatypes.funding_rates.enabled || datatypes.open_interest.enabled {
                streams.push(format!("tickers.{}", symbol));
            }
            streams
        }
        "coinbase" => {
            let mut channels = Vec::new();
            if datatypes.orderbook.enabled {
                channels.push("level2".to_string());
            }
            if datatypes.trades.enabled {
                channels.push("market_trades".to_string());
            }
            channels
        }
        "kraken" => {
            let mut channels = Vec::new();
            if datatypes.orderbook.enabled {
                channels.push("book".to_string());
            }
            if datatypes.trades.enabled {
                channels.push("trade".to_string());
            }
            channels
        }
        "binance" => {
            let sym = symbol.to_lowercase();
            let mut streams = Vec::new();
            if datatypes.orderbook.enabled {
                streams.push(format!("{}@depth@100ms", sym));
            }
            if datatypes.trades.enabled {
                streams.push(format!("{}@trade", sym));
            }
            streams
        }
        "okx" => crate::sources::okx::tooling::channels_for_config(
            datatypes.orderbook.enabled,
            datatypes.trades.enabled,
        ),
        "gateio" => crate::sources::gateio::tooling::channels_for_config(
            datatypes.orderbook.enabled,
            datatypes.trades.enabled,
        ),
        other => {
            tracing::warn!("Unknown exchange '{}'; returning empty streams", other);
            vec![]
        }
    }
}

/// Build canonical topic names for event classification.
///
/// These are the topic names that events get classified into
/// (different from WSS subscription topics which are exchange-specific).
pub fn wss_topic_names(
    exchange: &str,
    symbol: &str,
    datatypes: &DataTypesSection,
) -> Vec<String> {
    let _ = exchange; // topic names are exchange-agnostic
    let mut topics = Vec::new();
    if datatypes.orderbook.enabled {
        topics.push(format!(
            "orderbook.{}.{}",
            datatypes.orderbook.depth, symbol
        ));
    }
    if datatypes.trades.enabled {
        topics.push(format!("trade.all.{}", symbol));
    }
    if datatypes.liquidations.enabled {
        topics.push(format!("liquidation.all.{}", symbol));
    }
    if datatypes.funding_rates.enabled {
        topics.push(format!("funding.all.{}", symbol));
    }
    if datatypes.open_interest.enabled {
        topics.push(format!("open_interest.all.{}", symbol));
    }
    topics
}

// ─────────────────────────────────────────────────────────────────────────────
// Event → topic classification
// ─────────────────────────────────────────────────────────────────────────────

/// Map an [`ExchangeEvent`] to zero or more canonical topic names.
pub fn classify_event(
    event: &ExchangeEvent,
    _exchange: &str,
    symbol: &str,
    ob_depth: usize,
    datatypes: &DataTypesSection,
) -> Vec<String> {
    match event {
        ExchangeEvent::Bybit(bybit) => classify_bybit(bybit, symbol, ob_depth, datatypes),
        ExchangeEvent::Coinbase(coinbase) => {
            classify_coinbase(coinbase, symbol, ob_depth, datatypes)
        }
        ExchangeEvent::Kraken(kraken) => {
            classify_kraken(kraken, symbol, ob_depth, datatypes)
        }
        ExchangeEvent::Binance(binance) => {
            classify_binance(binance, symbol, ob_depth, datatypes)
        }
        ExchangeEvent::Okx(okx) => classify_okx(okx, symbol, ob_depth, datatypes),
        ExchangeEvent::Gateio(gate) => classify_gate(gate, symbol, ob_depth, datatypes),
    }
}

fn classify_bybit(
    event: &BybitWssEvent,
    symbol: &str,
    ob_depth: usize,
    dt: &DataTypesSection,
) -> Vec<String> {
    match event {
        BybitWssEvent::OrderbookData(_) if dt.orderbook.enabled => {
            vec![format!("orderbook.{}.{}", ob_depth, symbol)]
        }
        BybitWssEvent::TradeData(_) if dt.trades.enabled => {
            vec![format!("trade.all.{}", symbol)]
        }
        BybitWssEvent::LiquidationData(_) if dt.liquidations.enabled => {
            vec![format!("liquidation.all.{}", symbol)]
        }
        BybitWssEvent::TickerData(data) => {
            let mut topics = Vec::new();
            if dt.funding_rates.enabled && data.funding_rate.is_some() {
                topics.push(format!("funding.all.{}", symbol));
            }
            if dt.open_interest.enabled && data.open_interest.is_some() {
                topics.push(format!("open_interest.all.{}", symbol));
            }
            topics
        }
        _ => vec![],
    }
}

fn classify_coinbase(
    event: &CoinbaseWssEvent,
    symbol: &str,
    ob_depth: usize,
    dt: &DataTypesSection,
) -> Vec<String> {
    match event {
        CoinbaseWssEvent::OrderbookData(_) if dt.orderbook.enabled => {
            vec![format!("orderbook.{}.{}", ob_depth, symbol)]
        }
        CoinbaseWssEvent::TradeData(_) if dt.trades.enabled => {
            vec![format!("trade.all.{}", symbol)]
        }
        _ => vec![],
    }
}

fn classify_kraken(
    event: &KrakenWssEvent,
    symbol: &str,
    ob_depth: usize,
    dt: &DataTypesSection,
) -> Vec<String> {
    match event {
        KrakenWssEvent::OrderbookData(_) if dt.orderbook.enabled => {
            vec![format!("orderbook.{}.{}", ob_depth, symbol)]
        }
        KrakenWssEvent::TradeData(_) if dt.trades.enabled => {
            vec![format!("trade.all.{}", symbol)]
        }
        _ => vec![],
    }
}

fn classify_binance(
    event: &BinanceWssEvent,
    symbol: &str,
    ob_depth: usize,
    dt: &DataTypesSection,
) -> Vec<String> {
    use crate::sources::binance::events::BinanceWssEvent;

    match event {
        BinanceWssEvent::DepthUpdate(_) | BinanceWssEvent::DepthSnapshot(_)
            if dt.orderbook.enabled =>
        {
            vec![format!("orderbook.{}.{}", ob_depth, symbol)]
        }
        BinanceWssEvent::TradeData(_) if dt.trades.enabled => {
            vec![format!("trade.all.{}", symbol)]
        }
        _ => vec![],
    }
}

fn classify_okx(
    event: &OkxWssEvent,
    symbol: &str,
    ob_depth: usize,
    dt: &DataTypesSection,
) -> Vec<String> {
    match event {
        OkxWssEvent::OrderbookData(_) if dt.orderbook.enabled => {
            vec![format!("orderbook.{}.{}", ob_depth, symbol)]
        }
        OkxWssEvent::TradeData(_) if dt.trades.enabled => {
            vec![format!("trade.all.{}", symbol)]
        }
        _ => vec![],
    }
}

fn classify_gate(
    event: &GateioWssEvent,
    symbol: &str,
    ob_depth: usize,
    dt: &DataTypesSection,
) -> Vec<String> {
    match event {
        GateioWssEvent::OrderbookData(_) if dt.orderbook.enabled => {
            vec![format!("orderbook.{}.{}", ob_depth, symbol)]
        }
        GateioWssEvent::TradeData(_) if dt.trades.enabled => {
            vec![format!("trade.all.{}", symbol)]
        }
        _ => vec![],
    }
}

/// Current wall-clock time as UTC epoch microseconds (platform standard).
pub fn wall_clock_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}
