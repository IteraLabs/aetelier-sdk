//! Lea  ingestion-only data worker.
//!
//! `DataWorker` is the core runtime for the `data_worker` binary.
//! It establishes a persistent WebSocket connection to a single exchange
//! + symbol pair, decodes raw frames through the existing exchange
//! decoders, and publishes them — **without any pre-processing** — to
//! configured output sinks.
//!
//! # Design
//!
//! | Concern | Approach |
//! |---------|----------|
//! | Output | Pluggable `OutputSink`s (channel, terminal, parquet) |
//! | Processing | **None** — raw decoded events |
//! | Reconnection | Delegated to `IngestionCore` |
//! | Gap tracking | Delegated to `IngestionCore` |
//! | Stale detection | Delegated to `IngestionCore` |
//!
//! # Event classification
//!
//! Each decoded `ExchangeEvent` variant is mapped to a canonical topic
//! name.  The mapping is exchange-specific:
//!
//! | Exchange | Event | Topic |
//! |----------|-------|-------|
//! | Bybit | `OrderbookData` | `orderbook.{depth}.{symbol}` |
//! | Bybit | `TradeData` | `publicTrade.{symbol}` |
//! | Bybit | `LiquidationData` | `lquidation.all.{symbol}` |
//! | Bybit | `TickerData` (funding) | `funding.all.{symbol}` |
//! | Bybit | `TickerData` (OI) | `open_interest.all.{symbol}` |
//!
//! | Binance | `DepthUpdate` | `orderbook.{depth}.{symbol}` |
//! | Binance | `DepthSnapshot` | `orderbook.{depth}.{symbol}` |
//! | Binance | `TradeData` | `trade.all.{symbol}` |
//!
//! | Coinbase | `OrderbookData` | `orderbook.{depth}.{symbol}` |
//! | Coinbase | `TradeData` | `trade.all.{symbol}` |
//!
//! | Kraken | `OrderbookData` | `orderbook.{depth}.{symbol}` |
//! | Kraken | `TradeData` | `trade.all.{symbol}` |

use tokio::sync::mpsc;
use tokio::sync::watch;

use crate::config::workers::{CommonWorkerFields, DataWorkerConfig, OutputSinkConfig};
use crate::errors::ConnectError;
use aetelier_types::config::MarketSnapshotConfig;

use aetelier_types::synchronizers::WorkerMode;

use super::gap_detector::GapStats;
use super::ingestion_core::{IngestionCore, IngestionReport};
use super::output::{
    DomainChannelSink, OutputSinkSet, TerminalEventCallback, build_sinks,
};
use super::registry::{WorkerCommand, WorkerStatus};
use super::topic_publisher::{DomainTopicRegistry, TopicRegistry};

/// How often (ms) the worker publishes its status to the registry.
const STATUS_PUBLISH_INTERVAL_MS: u64 = 250;

// ─────────────────────────────────────────────────────────────────────────────
// DataWorkerReport
// ─────────────────────────────────────────────────────────────────────────────

/// Summary statistics returned when a [`DataWorker`] finishes.
#[derive(Debug, Clone)]
pub struct DataWorkerReport {
    /// Exchange name (e.g. `"bybit"`).
    pub exchange: String,
    /// Trading pair (e.g. `"BTCUSDT"`).
    pub symbol: String,
    /// Total raw events published across all topics.
    pub total_events: u64,
    /// Per-topic event counts.
    pub events_by_topic: Vec<(String, u64)>,
    /// Wall-clock seconds the worker was active.
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

impl From<IngestionReport> for DataWorkerReport {
    fn from(r: IngestionReport) -> Self {
        Self {
            exchange: r.exchange,
            symbol: r.symbol,
            total_events: r.total_events,
            events_by_topic: r.events_by_topic,
            elapsed_secs: r.elapsed_secs,
            effective_rate: r.effective_rate,
            reconnect_count: r.reconnect_count,
            gap_stats: r.gap_stats,
            errors: r.errors,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DataWorker
// ─────────────────────────────────────────────────────────────────────────────

/// A single-symbol raw data ingestion worker.
///
/// Connects to the configured exchange, decodes WebSocket events, and
/// publishes them without pre-processing to the configured output sinks.
///
/// Composed from an [`IngestionCore`] (handles connection lifecycle) and
/// an [`OutputSinkSet`] (handles event delivery).
pub struct DataWorker {
    /// The ingestion engine.
    core: IngestionCore,
    /// Fan-out output sinks.
    sinks: OutputSinkSet,
    /// Channel capacity for the core → worker pipe.
    channel_capacity: usize,
    /// Command receiver from the registry (`None` when running standalone).
    cmd_rx: Option<mpsc::Receiver<WorkerCommand>>,
    /// Status publisher for the registry (`None` when running standalone).
    status_tx: Option<watch::Sender<WorkerStatus>>,
    /// Ingest through the framework path (registry adapter →
    /// `DomainEvent` → domain topics) rather than the legacy spawn sprawl.
    framework_ingest: bool,
    /// Domain-topic registry for the framework path (`Some` only when
    /// `framework_ingest` is set and a channel sink is configured). Exposed so
    /// downstream consumers can subscribe to the normalized stream.
    domain_registry: Option<DomainTopicRegistry>,
}

impl DataWorker {
    /// Create a DataWorker from a [`DataWorkerConfig`].
    ///
    /// The registry is built internally and wrapped in a `ChannelSink`
    /// if the config requests channel output.
    pub fn from_config(config: DataWorkerConfig) -> Result<Self, ConnectError> {
        Self::from_config_with_terminal_cb(config, None)
    }

    /// Create a DataWorker from a [`DataWorkerConfig`] with an optional
    /// terminal event callback for dashboard forwarding.
    pub fn from_config_with_terminal_cb(
        config: DataWorkerConfig,
        terminal_cb: Option<TerminalEventCallback>,
    ) -> Result<Self, ConnectError> {
        let channel_capacity = config.common.channel_capacity();
        let core = IngestionCore::new(config.common.clone())?;

        let has_channel = config
            .output
            .iter()
            .any(|s| matches!(s, OutputSinkConfig::Channel));
        let framework = config.framework_ingest;

        // The raw channel sink is built only for the legacy path; on the
        // framework path a `DomainChannelSink` serves the channel output, so the
        // raw `Channel` entry is stripped before `build_sinks` (avoids a dormant
        // raw registry + a misleading "no registry" warning).
        let raw_registry = if has_channel && !framework {
            Some(TopicRegistry::from_config(
                &config.common.exchange,
                &config.common.symbol,
                &config.common.datatypes,
                channel_capacity,
            ))
        } else {
            None
        };
        let raw_output: Vec<OutputSinkConfig> = if framework {
            config
                .output
                .iter()
                .filter(|s| !matches!(s, OutputSinkConfig::Channel))
                .cloned()
                .collect()
        } else {
            config.output.clone()
        };

        let mut sinks = build_sinks(
            &raw_output,
            raw_registry,
            None,
            None,
            terminal_cb,
            None,
            config.common.datatypes.declared_set(),
        )
        .map_err(|e| ConnectError::Sink(e.to_string()))?;

        let domain_registry = if framework && has_channel {
            let reg = DomainTopicRegistry::from_config(
                &config.common.symbol,
                &config.common.datatypes,
                channel_capacity,
            );
            sinks.push(Box::new(DomainChannelSink::new(
                reg.clone(),
                config.common.exchange.clone(),
            )));
            Some(reg)
        } else {
            None
        };

        Ok(Self {
            core,
            sinks,
            channel_capacity,
            cmd_rx: None,
            status_tx: None,
            framework_ingest: framework,
            domain_registry,
        })
    }

    /// Create a DataWorker from a legacy [`MarketSnapshotConfig`].
    ///
    /// Uses channel-only output for backward compatibility.
    /// The registry is returned separately via `build_registry()`.
    pub fn new(config: MarketSnapshotConfig) -> Self {
        let common = CommonWorkerFields::from(&config);
        let channel_capacity = common.channel_capacity();
        let core = IngestionCore::new(common).expect(
            "legacy MarketSnapshotConfig should always produce a valid IngestionCore",
        );

        // Don't build sinks here — legacy callers use build_registry() + run(rx, registry).
        Self {
            core,
            sinks: OutputSinkSet::new(),
            channel_capacity,
            cmd_rx: None,
            status_tx: None,
            framework_ingest: false,
            domain_registry: None,
        }
    }

    /// Build a topic registry for downstream subscription.
    ///
    /// This is the legacy API — callers build the registry, subscribe
    /// to topics, then pass it to `run_legacy()`.
    pub fn build_registry(&self) -> TopicRegistry {
        TopicRegistry::from_config(
            self.core.exchange_name(),
            self.core.symbol(),
            self.core.datatypes(),
            self.channel_capacity,
        )
    }

    /// The framework path's domain-topic registry, if this worker was built with
    /// `framework_ingest` and a channel sink. Subscribe to its normalized topics
    /// before calling [`Self::run`].
    pub fn domain_registry(&self) -> Option<&DomainTopicRegistry> {
        self.domain_registry.as_ref()
    }

    /// Decide whether to take the framework ingest path. Requires the flag, a
    /// registered framework adapter for the venue, and enabled datatypes that
    /// are a subset of {orderbook, trades} (the only datatypes `DomainEvent`
    /// models). When the flag is set but a precondition fails, logs once and the
    /// caller falls back to the legacy raw path so no datatype is dropped.
    fn use_framework_path(&self) -> bool {
        if !self.framework_ingest {
            return false;
        }
        let exchange = self.core.exchange_name();
        let registered = crate::framework::registry::registry()
            .get(exchange)
            .is_some();
        if !registered {
            tracing::warn!(
                exchange = exchange,
                "data_worker.framework_ingest.unregistered_venue_fallback_legacy"
            );
            return false;
        }
        let adapter = *crate::framework::registry::registry()
            .get(exchange)
            .unwrap();
        let supported = adapter.supported_datatypes();
        use aetelier_types::config::markets::market_config::DeclaredDatatype as DD;
        let dt = self.core.datatypes();
        let unsupported = (dt.liquidations.enabled
            && !supported.contains(&DD::Liquidations))
            || (dt.funding_rates.enabled && !supported.contains(&DD::FundingRates))
            || (dt.open_interest.enabled && !supported.contains(&DD::OpenInterest));
        if unsupported {
            tracing::warn!(
                exchange = exchange,
                "data_worker.framework_ingest.unsupported_datatypes_fallback_legacy"
            );
            return false;
        }
        true
    }

    /// Attach a command receiver from [`WorkerChannels`](super::registry::WorkerChannels).
    ///
    /// When set, the event loop will `select!` on this channel and honor
    /// `Pause`, `Resume`, `Stop`, and `Restart` commands.
    pub fn with_command_receiver(mut self, rx: mpsc::Receiver<WorkerCommand>) -> Self {
        self.cmd_rx = Some(rx);
        self
    }

    /// Attach a status publisher from [`WorkerChannels`](super::registry::WorkerChannels).
    ///
    /// When set, the worker periodically publishes [`WorkerStatus`] updates
    /// visible through the [`WorkerRegistry`](super::registry::WorkerRegistry).
    pub fn with_status_sender(mut self, tx: watch::Sender<WorkerStatus>) -> Self {
        self.status_tx = Some(tx);
        self
    }

    /// Inject additional output sinks beyond those declared in the TOML
    /// manifest (e.g. a streaming sink an embedding application supplies).
    ///
    /// Called by an orchestrating harness after `from_config`.
    pub fn with_extra_sinks(
        mut self,
        extra: Vec<Box<dyn super::output::OutputSink>>,
    ) -> Self {
        for sink in extra {
            self.sinks.push(sink);
        }
        self
    }

    /// Run the ingestion loop (new API with built-in sinks).
    ///
    /// If a command receiver was attached via [`Self::with_command_receiver`],
    /// the event loop also listens for `Pause`, `Resume`, `Stop`, and
    /// `Restart` commands.
    ///
    /// Returns a [`DataWorkerReport`] with session statistics.
    pub async fn run(
        self,
        shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<DataWorkerReport> {
        if self.use_framework_path() {
            return self.run_framework(shutdown).await;
        }
        let channel_capacity = self.channel_capacity;
        let mut cmd_rx = self.cmd_rx;
        let status_tx = self.status_tx;

        // Pipeline: passthrough for most exchanges, BookInitializer for Binance.
        let exchange_enum: aetelier_types::exchanges::Exchange = self
            .core
            .exchange_name()
            .parse()
            .unwrap_or(aetelier_types::exchanges::Exchange::Bybit);
        let datatypes = self.core.datatypes().clone();
        let symbol = self.core.symbol().to_string();
        let pair = aetelier_types::trading_pair::TradingPair::from_exchange_symbol(
            &symbol,
            exchange_enum,
        )
        .expect("valid exchange symbol");
        let exchange_name = self.core.exchange_name().to_string();

        // Snapshot initial status fields for periodic updates (before moving self).
        let initial_datatypes = datatypes.enabled_names();
        let (initial_market_type, initial_sinks) = status_tx
            .as_ref()
            .map(|tx| {
                let s = tx.borrow();
                (s.market_type, s.sinks.clone())
            })
            .unwrap_or_default();

        let (core_tx, core_rx) = mpsc::channel(channel_capacity);
        let sinks = self.sinks;
        let core_handle = tokio::spawn(self.core.run(shutdown.clone(), core_tx));

        let pipeline =
            crate::workers::pipeline::build_pipeline(exchange_enum, &symbol, &datatypes);
        let (pipeline_tx, mut rx) = mpsc::channel(channel_capacity);
        let _pipeline_handle = tokio::spawn(pipeline.run(core_rx, pipeline_tx, shutdown));

        let mut paused = false;

        // ── Live status tracking ───────────────────────────────────────
        let start = tokio::time::Instant::now();
        let mut total_events: u64 = 0;
        let mut events_since_last_tick: u64 = 0;
        let mut msgs_per_sec: f64 = 0.0;
        let mut status_interval = tokio::time::interval(
            std::time::Duration::from_millis(STATUS_PUBLISH_INTERVAL_MS),
        );
        // Don't delay the first event dispatch waiting for the status tick.
        status_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Mark as Streaming once the first event arrives.
        let mut connection_state = crate::ConnectionState::Connecting;

        /// Receive the next command from the optional command channel.
        async fn recv_cmd(
            cmd_rx: &mut Option<mpsc::Receiver<WorkerCommand>>,
        ) -> Option<WorkerCommand> {
            match cmd_rx {
                Some(rx) => rx.recv().await,
                None => std::future::pending().await,
            }
        }

        // Fan out events to sinks, with optional command handling.
        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Some(msg) => {
                            if !paused {
                                let _ = sinks.emit_raw(&msg.topic, &msg.payload, msg.received_at_us);
                            }
                            total_events += 1;
                            events_since_last_tick += 1;

                            // Transition to Streaming on first event
                            if matches!(connection_state, crate::ConnectionState::Connecting) {
                                connection_state = crate::ConnectionState::Streaming;
                            }
                        }
                        None => break, // pipeline exited
                    }
                }

                cmd = recv_cmd(&mut cmd_rx) => {
                    match cmd {
                        Some(WorkerCommand::Pause) => {
                            tracing::info!("data_worker.paused");
                            paused = true;
                            connection_state = crate::ConnectionState::Paused;
                        }
                        Some(WorkerCommand::Resume) => {
                            tracing::info!("data_worker.resumed");
                            paused = false;
                            connection_state = crate::ConnectionState::Streaming;
                        }
                        Some(WorkerCommand::Stop) => {
                            tracing::info!("data_worker.stop_requested");
                            break;
                        }
                        Some(WorkerCommand::Restart) => {
                            tracing::info!("data_worker.restart_requested");
                            break;
                        }
                        None => {} // sender dropped — continue standalone
                    }
                }

                _ = status_interval.tick() => {
                    // Compute EWMA-style messages/sec from events in this tick window
                    let tick_secs = STATUS_PUBLISH_INTERVAL_MS as f64 / 1000.0;
                    let instant_rate = events_since_last_tick as f64 / tick_secs;
                    // Exponential smoothing (α = 0.3)
                    msgs_per_sec = 0.3 * instant_rate + 0.7 * msgs_per_sec;
                    // Floor to zero when the EWMA decays below a visible
                    // threshold — prevents float noise (e.g. 8e-26) from
                    // confusing chart auto-scale on the dashboard.
                    if msgs_per_sec < 0.001 && instant_rate == 0.0 {
                        msgs_per_sec = 0.0;
                    }
                    events_since_last_tick = 0;

                    if let Some(ref tx) = status_tx {
                        let id_str = tx.borrow().id;
                        let _ = tx.send(WorkerStatus {
                            id: id_str,
                            exchange: exchange_name.parse().unwrap_or(
                                aetelier_types::exchanges::Exchange::Bybit,
                            ),
                            pair: pair.clone(),
                            market_type: initial_market_type,
                            mode: WorkerMode::Raw,
                            connection_state,
                            messages_per_sec: msgs_per_sec,
                            total_events,
                            reconnect_count: 0,
                            sinks: initial_sinks.clone(),
                            datatypes: initial_datatypes.clone(),
                            feeds: Vec::new(),
                            uptime_secs: start.elapsed().as_secs_f64(),
                            ws_latency_us: None,
                            source_metrics: Default::default(),
                        });
                    }
                }
            }
        }

        sinks.flush()?;

        let report = core_handle.await??;
        Ok(DataWorkerReport::from(report))
    }

    /// Framework ingest path: resolve the registry adapter, stream its
    /// normalized `DomainEvent`s, and fan them onto the domain topics via
    /// `emit_domain`. No book reconstruction (that is `MarketWorker`'s job) —
    /// this is best-effort normalized passthrough, the framework analog of the
    /// raw `DataWorker`. Reconnects on adapter end with exponential backoff,
    /// mirroring `MarketWorker::run_framework`.
    async fn run_framework(
        self,
        mut shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<DataWorkerReport> {
        use aetelier_types::exchanges::Exchange;

        let exchange_name = self.core.exchange_name().to_string();
        let symbol = self.core.symbol().to_string();
        let datatypes = self.core.datatypes().clone();
        let environment = self.core.environment();
        let channel_capacity = self.channel_capacity;
        let sinks = self.sinks;
        let mut cmd_rx = self.cmd_rx;
        let status_tx = self.status_tx;

        let adapter = crate::framework::registry::registry()
            .get(exchange_name.as_str())
            .copied()
            .expect("run_framework only entered for a registered venue");

        if let Err(e) = crate::framework::registry::admit_declared_depth(
            &exchange_name,
            adapter.max_declared_depth(),
            datatypes.orderbook.enabled,
            datatypes.orderbook.depth,
        ) {
            anyhow::bail!(e);
        }

        if let Err(e) = crate::framework::registry::admit_environment(
            &exchange_name,
            adapter.supports_environment(environment),
            environment,
        ) {
            anyhow::bail!(e);
        }

        let pair = adapter
            .profile()
            .symbol_codec
            .decode(&symbol)
            .ok_or_else(|| {
                anyhow::anyhow!("cannot decode symbol {symbol} for {exchange_name}")
            })?;

        // Topic strings derived once from the configured symbol + depth — byte-
        // identical to the legacy raw path's topic naming.
        let ob_topic = datatypes
            .orderbook
            .enabled
            .then(|| format!("orderbook.{}.{}", datatypes.orderbook.depth, symbol));
        let trade_topic = datatypes
            .trades
            .enabled
            .then(|| format!("trade.all.{}", symbol));
        let funding_topic = datatypes
            .funding_rates
            .enabled
            .then(|| format!("funding.all.{}", symbol));
        let oi_topic = datatypes
            .open_interest
            .enabled
            .then(|| format!("open_interest.all.{}", symbol));

        let exchange_enum: Exchange = exchange_name.parse().unwrap_or(Exchange::Bybit);
        let initial_datatypes = datatypes.enabled_names();
        let (initial_market_type, initial_sinks) = status_tx
            .as_ref()
            .map(|tx| {
                let s = tx.borrow();
                (s.market_type, s.sinks.clone())
            })
            .unwrap_or_default();

        let start = tokio::time::Instant::now();
        let mut total_events: u64 = 0;
        let mut events_since_last_tick: u64 = 0;
        let mut msgs_per_sec: f64 = 0.0;
        let mut reconnects: u32 = 0;
        // Feed-staleness EWMA (µs): local receipt minus the event's exchange
        // timestamp, over Book + Trade DomainEvents. Published with the status.
        let mut latency_us_ewma: f64 = 0.0;
        let mut status_interval = tokio::time::interval(
            std::time::Duration::from_millis(STATUS_PUBLISH_INTERVAL_MS),
        );
        status_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut connection_state = crate::ConnectionState::Connecting;
        let mut paused = false;
        let mut stopped = false;

        async fn recv_cmd(
            cmd_rx: &mut Option<mpsc::Receiver<WorkerCommand>>,
        ) -> Option<WorkerCommand> {
            match cmd_rx {
                Some(rx) => rx.recv().await,
                None => std::future::pending().await,
            }
        }

        // Shared per-worker transport counters; each spawn's clone bumps the
        // same atomics and the status publish snapshots them.
        let metrics = crate::framework::budget::SourceMetrics::default();

        // Reconnect loop: the adapter ending (disconnect / gap resubscribe) re-
        let declared_set = self.core.datatypes().declared_set();
        // establishes the socket; shutdown / Stop exits.
        while !stopped && !*shutdown.borrow() {
            let (dev_tx, mut dev_rx) = mpsc::channel(channel_capacity);
            let adapter_handle = adapter.spawn_env(
                environment,
                vec![symbol.clone()],
                declared_set.clone(),
                dev_tx,
                shutdown.clone(),
                metrics.clone(),
            );
            if !matches!(
                connection_state,
                crate::ConnectionState::Reconnecting { .. }
            ) {
                connection_state = crate::ConnectionState::Connecting;
            }

            loop {
                tokio::select! {
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() { stopped = true; break; }
                    }

                    ev = dev_rx.recv() => match ev {
                        Some(crate::framework::model::DomainEvent::ConnectionGap { dropped }) => {
                            // The adapter proved messages left the socket
                            // undelivered. The raw domain stream cannot be
                            // repaired in place — reconnect so subscribers
                            // get a fresh snapshot. Paced by the worker's
                            // jittered reconnect backoff.
                            tracing::warn!(dropped, "data_worker.connection_gap_reconnecting");
                            metrics.bump_gaps();
                            metrics.bump_resyncs();
                            break;
                        }
                        Some(ev) => {
                            total_events += 1;
                            events_since_last_tick += 1;
                            connection_state = crate::ConnectionState::Streaming;
                            if paused { continue; }
                            let ev_ts = match &ev {
                                crate::framework::model::DomainEvent::Book(d) => d.source_orderbook_ts_us,
                                crate::framework::model::DomainEvent::Trade { trade, .. } => {
                                    trade.source_trade_ts_us
                                }
                                crate::framework::model::DomainEvent::FundingRate(fr) => {
                                    fr.effective_ts_us()
                                }
                                crate::framework::model::DomainEvent::OpenInterest(oi) => {
                                    oi.effective_ts_us()
                                }
                                crate::framework::model::DomainEvent::FundingSettlement(fs) => {
                                    fs.funding_time_us
                                }
                                // Handled above; unreachable here.
                                crate::framework::model::DomainEvent::ConnectionGap { .. } => 0,
                            };
                            if ev_ts > 0 {
                                let now_us = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_micros() as u64;
                                let sample = now_us.saturating_sub(ev_ts) as f64;
                                latency_us_ewma = if latency_us_ewma <= 0.0 {
                                    sample
                                } else {
                                    0.2 * sample + 0.8 * latency_us_ewma
                                };
                            }
                            let topic = match &ev {
                                crate::framework::model::DomainEvent::Book(_) => ob_topic.as_deref(),
                                crate::framework::model::DomainEvent::Trade { .. } => {
                                    trade_topic.as_deref()
                                }
                                crate::framework::model::DomainEvent::FundingRate(_)
                                | crate::framework::model::DomainEvent::FundingSettlement(_) => {
                                    funding_topic.as_deref()
                                }
                                crate::framework::model::DomainEvent::OpenInterest(_) => {
                                    oi_topic.as_deref()
                                }
                                crate::framework::model::DomainEvent::ConnectionGap { .. } => None,
                            };
                            if let Some(topic) = topic {
                                let now_us = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map(|d| d.as_micros() as u64)
                                    .unwrap_or(0);
                                let _ = sinks.emit_domain(topic, &ev, now_us);
                            }
                        }
                        None => break, // adapter ended — reconnect
                    },

                    cmd = recv_cmd(&mut cmd_rx) => match cmd {
                        Some(WorkerCommand::Pause) => {
                            paused = true;
                            connection_state = crate::ConnectionState::Paused;
                        }
                        Some(WorkerCommand::Resume) => {
                            paused = false;
                            connection_state = crate::ConnectionState::Streaming;
                        }
                        Some(WorkerCommand::Stop) | Some(WorkerCommand::Restart) => {
                            stopped = true;
                            break;
                        }
                        None => {}
                    },

                    _ = status_interval.tick() => {
                        let tick_secs = STATUS_PUBLISH_INTERVAL_MS as f64 / 1000.0;
                        let instant_rate = events_since_last_tick as f64 / tick_secs;
                        msgs_per_sec = 0.3 * instant_rate + 0.7 * msgs_per_sec;
                        if msgs_per_sec < 0.001 && instant_rate == 0.0 {
                            msgs_per_sec = 0.0;
                        }
                        events_since_last_tick = 0;
                        if let Some(ref tx) = status_tx {
                            let id_str = tx.borrow().id;
                            let _ = tx.send(WorkerStatus {
                                id: id_str,
                                exchange: exchange_enum,
                                pair: pair.clone(),
                                market_type: initial_market_type,
                                mode: WorkerMode::Raw,
                                connection_state,
                                messages_per_sec: msgs_per_sec,
                                total_events,
                                reconnect_count: reconnects,
                                sinks: initial_sinks.clone(),
                                datatypes: initial_datatypes.clone(),
                                feeds: Vec::new(),
                                uptime_secs: start.elapsed().as_secs_f64(),
                                ws_latency_us: Some(latency_us_ewma),
                                source_metrics: metrics.snapshot(),
                            });
                        }
                    }
                }
            }

            let adapter_exit = adapter_handle
                .await
                .unwrap_or(crate::framework::registry::TaskExit::Completed);
            if matches!(
                adapter_exit,
                crate::framework::registry::TaskExit::Exhausted
            ) {
                metrics.mark_source_exhausted();
                tracing::info!(
                    exchange = exchange_name.as_str(),
                    symbol = symbol.as_str(),
                    "data_worker.source_exhausted"
                );
                break;
            }
            if stopped || *shutdown.borrow() {
                break;
            }
            reconnects += 1;
            metrics.bump_reconnects();
            connection_state = crate::ConnectionState::Reconnecting {
                attempt: reconnects,
            };
            tracing::warn!(
                exchange = exchange_name.as_str(),
                symbol = symbol.as_str(),
                attempt = reconnects,
                "data_worker.framework_reconnecting"
            );
            let backoff_ms = (250u64 * (1u64 << reconnects.min(5))).min(8_000);
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)) => {}
                _ = shutdown.changed() => { if *shutdown.borrow() { break; } }
            }
        }

        sinks.flush()?;

        let elapsed = start.elapsed().as_secs_f64();
        tracing::info!(
            exchange = exchange_name.as_str(),
            symbol = symbol.as_str(),
            total_events,
            reconnects,
            "data_worker.framework_stopped"
        );
        Ok(DataWorkerReport {
            exchange: exchange_name,
            symbol,
            total_events,
            events_by_topic: Vec::new(),
            elapsed_secs: elapsed,
            effective_rate: if elapsed > 0.0 {
                total_events as f64 / elapsed
            } else {
                0.0
            },
            reconnect_count: reconnects,
            gap_stats: Vec::new(),
            errors: Vec::new(),
        })
    }

    /// Run the ingestion loop (legacy API with external registry).
    ///
    /// This preserves the original `DataWorker::run(shutdown, registry)`
    /// signature for backward compatibility with existing binaries.
    pub async fn run_legacy(
        self,
        shutdown: watch::Receiver<bool>,
        registry: TopicRegistry,
    ) -> anyhow::Result<DataWorkerReport> {
        let channel_capacity = self.channel_capacity;
        let (tx, mut rx) = mpsc::channel(channel_capacity);

        let core_handle = tokio::spawn(self.core.run(shutdown, tx));

        // Publish to the legacy registry directly.
        while let Some(msg) = rx.recv().await {
            if let Err(e) = registry.publish(&msg.topic, msg.clone()) {
                tracing::warn!(
                    topic = msg.topic.as_str(),
                    error = %e,
                    "data_worker.publish_failed"
                );
            }
        }

        let report = core_handle.await??;
        Ok(DataWorkerReport::from(report))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::workers::DataWorkerManifest;

    fn cfg(exchange: &str, framework: bool) -> DataWorkerConfig {
        let toml = format!(
            r#"
[collect]
exchange = "{exchange}"
framework_ingest = {framework}

[collect.datatypes.orderbook]
enabled = true
depth = 50

[collect.datatypes.trades]
enabled = true

[[collect.output]]
type = "channel"

[[workers]]
symbol = "BTCUSDT"
"#
        );
        DataWorkerManifest::from_str(&toml)
            .unwrap()
            .resolve_all()
            .pop()
            .unwrap()
    }

    #[test]
    fn framework_ingest_exposes_domain_registry_with_book_and_trade_topics() {
        let w = DataWorker::from_config(cfg("bybit", true)).unwrap();
        let reg = w
            .domain_registry()
            .expect("framework path must expose a domain registry for channel output");
        assert_eq!(reg.len(), 2);
        assert!(reg.subscribe("orderbook.50.BTCUSDT").is_some());
        assert!(reg.subscribe("trade.all.BTCUSDT").is_some());
        // The framework gate holds for a registered venue with book/trade only.
        assert!(w.use_framework_path());
    }

    #[test]
    fn legacy_path_has_no_domain_registry() {
        let w = DataWorker::from_config(cfg("bybit", false)).unwrap();
        assert!(w.domain_registry().is_none());
        assert!(!w.use_framework_path());
    }

    #[test]
    fn framework_ingest_constructs_for_a_new_enum_venue() {
        // Before the Exchange-enum extension, `upbit` was not in `Exchange`, so
        // IngestionCore::new (which parses common.exchange into Exchange) could
        // not build a DataWorker for it at all. Now it constructs and the
        // framework gate holds (registered framework venue, book/trade only).
        let w = DataWorker::from_config(cfg("upbit", true)).unwrap();
        assert!(w.use_framework_path());
        assert!(w.domain_registry().is_some());
    }

    #[test]
    fn framework_ingest_falls_back_to_legacy_when_derivatives_enabled() {
        // DomainEvent models only Book|Trade, so a worker carrying liquidations
        // must NOT take the framework branch (it would drop them) — it stays
        // fully legacy.
        let toml = r#"
[collect]
exchange = "bybit"
framework_ingest = true

[collect.datatypes.orderbook]
enabled = true
depth = 50

[collect.datatypes.trades]
enabled = true

[collect.datatypes.liquidations]
enabled = true

[[collect.output]]
type = "channel"

[[workers]]
symbol = "BTCUSDT"
"#;
        let c = DataWorkerManifest::from_str(toml)
            .unwrap()
            .resolve_all()
            .pop()
            .unwrap();
        let w = DataWorker::from_config(c).unwrap();
        assert!(!w.use_framework_path());
    }
}
