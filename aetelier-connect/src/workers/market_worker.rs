//! Synchronised market data worker.
//!
//! `MarketWorker` composes an `IngestionCore` with a
//! `MarketSynchronizer` to produce grid-aligned `MarketSnapshot`s
//! from raw exchange events.
//!
//! # Design
//!
//! ```text
//! ┌──────────────┐     mpsc     ┌────────────────┐   sync    ┌──────────┐
//! │ IngestionCore│─────────────→│ MarketWorker   │──────────→│ Sinks    │
//! │ (WSS + reconn│  TopicMsg    │ (feed_event +  │ Snapshot  │ (channel │
//! │  + classify) │              │  synchronizer) │           │  / term) │
//! └──────────────┘              └────────────────┘           └──────────┘
//! ```
//!
//! The worker:
//! 1. Spawns an `IngestionCore` in a background task.
//! 2. Receives classified `TopicMessage`s.
//! 3. Converts raw exchange events into normalised types
//!    (`Trade`, `Orderbook`, etc.) and feeds them into the synchroniser.
//! 4. Drains ready `MarketSnapshot`s and emits them to output sinks.

use tokio::sync::mpsc;
use tokio::sync::watch;

use crate::clients::reconnect::{ReconnectAction, ReconnectPolicy};
use crate::framework::budget::SourceMetrics;
use crate::framework::registry::TaskExit;
use crate::framework::runtime::{ReconstructedEvent, RuntimeOutcome, SourceRuntime};

use crate::config::workers::{MarketWorkerConfig, OutputSinkConfig};
use crate::errors::ConnectError;
use crate::sources::ExchangeEvent;
use crate::synchronizers::{ClockMode, MarketSynchronizer};
use aetelier_types::config::markets::market_config::SyncMode;
use aetelier_types::exchanges::Exchange;
use aetelier_types::levels::Level;
use aetelier_types::orders::OrderSide;
use aetelier_types::trading_pair::TradingPair;

use aetelier_types::synchronizers::WorkerMode;

use super::ingestion_core::{IngestionCore, IngestionReport};
use super::output::{
    BufferedSinkFlushCallback, OutputSinkSet, SnapshotFlusher, TerminalEventCallback,
    build_sinks,
};
use super::registry::{WorkerCommand, WorkerStatus};
use super::topic_publisher::{TopicMessage, TopicRegistry};

/// How often (ms) the worker publishes its status to the registry.
const STATUS_PUBLISH_INTERVAL_MS: u64 = 250;

const REPLAY_DEDUP_CAPACITY: usize = 4096;

fn wire_ts_regressed(last_ts_us: &mut u64, ts_us: u64) -> bool {
    if ts_us < *last_ts_us {
        return true;
    }
    *last_ts_us = ts_us;
    false
}

#[derive(Debug)]
struct SeenTradeIds {
    order: std::collections::VecDeque<u64>,
    ids: std::collections::HashSet<u64>,
    capacity: usize,
}

impl SeenTradeIds {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            order: std::collections::VecDeque::with_capacity(capacity),
            ids: std::collections::HashSet::with_capacity(capacity),
            capacity,
        }
    }

    fn insert(&mut self, id: u64) -> bool {
        if !self.ids.insert(id) {
            return false;
        }
        self.order.push_back(id);
        if self.order.len() > self.capacity
            && let Some(evicted) = self.order.pop_front()
        {
            self.ids.remove(&evicted);
        }
        true
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MarketWorkerReport
// ─────────────────────────────────────────────────────────────────────────────

/// Summary statistics returned when a `MarketWorker` finishes.
#[derive(Debug, Clone)]
pub struct MarketWorkerReport {
    /// Ingestion-level statistics (events, reconnects, gaps).
    pub ingestion: IngestionReport,
    /// Total grid-aligned snapshots produced.
    pub snapshots_produced: u64,
    /// Number of flush cycles completed.
    pub flushes: u32,
}

// ─────────────────────────────────────────────────────────────────────────────
// MarketWorker
// ─────────────────────────────────────────────────────────────────────────────

/// A single-symbol synchronised market data worker.
///
/// Connects to the configured exchange, ingests raw events via
/// `IngestionCore`, feeds them through a `MarketSynchronizer`,
/// and emits grid-aligned `MarketSnapshot`s to the configured sinks.
pub struct MarketWorker {
    /// The ingestion engine (handles WSS connection + reconnection).
    core: IngestionCore,
    /// Time synchroniser producing grid-aligned snapshots.
    sync: MarketSynchronizer,
    /// Fan-out output sinks.
    sinks: OutputSinkSet,
    /// Grid ticks before a flush cycle.
    flush_threshold: usize,
    /// Trading pair symbol.
    symbol: String,
    /// Exchange name.
    exchange_name: String,
    /// Channel capacity for the core → worker pipe.
    channel_capacity: usize,
    /// Which clock mode drives snapshot production.
    clock_mode: ClockMode,
    /// Grid period in microseconds (used for the timer in ExternalClock mode).
    period_us: u64,
    /// Configured order book depth (number of price levels per side).
    ///
    /// For exchanges with unbounded wire data (Binance, Coinbase) this is
    /// used to `.take(ob_depth)` when constructing `Orderbook` levels in the
    /// `feed_*` functions.  For server-filtered exchanges (Bybit, Kraken)
    /// this is a no-op ceiling that never binds.
    ob_depth: usize,
    environment: aetelier_types::exchanges::VenueEnvironment,
    /// Command receiver from the registry (`None` when running standalone).
    cmd_rx: Option<mpsc::Receiver<WorkerCommand>>,
    /// Status publisher for the registry (`None` when running standalone).
    status_tx: Option<watch::Sender<WorkerStatus>>,
    /// When set, reconstruct through the framework adapter + runtime instead of
    /// the legacy ingestion path (falls back to legacy if the venue is not
    /// registered).
    framework_ingest: bool,
    /// Snapshot broadcast handle (`Some` when a Channel sink is configured);
    /// kept so consumers can subscribe to the synchronized stream.
    snapshot_channel: Option<crate::workers::topic_publisher::SnapshotChannel>,
    /// Persisted coverage ledger (`Some` when a Parquet sink dir exists —
    /// the ledger lives beside the datatype subdirs).
    gap_ledger: Option<crate::workers::gap_ledger::GapLedger>,
    /// Live-reconciliation settings (`Some` only when enabled + validated).
    reconcile: Option<crate::config::workers::market_worker_config::ReconcileSection>,
}

impl MarketWorker {
    /// Create a MarketWorker from a [`MarketWorkerConfig`].
    ///
    /// If the config declares a `Parquet` sink but no flusher is
    /// provided, construction FAILS (the data would otherwise be
    /// silently dropped).  Use [`Self::from_config_with_flusher`] to
    /// inject a concrete [`SnapshotFlusher`] (e.g. `ParquetSnapshotFlusher`
    /// from `aetelier-io`).
    pub fn from_config(config: MarketWorkerConfig) -> Result<Self, ConnectError> {
        Self::from_config_with_flusher(config, None)
    }

    /// Create a MarketWorker with an optional [`SnapshotFlusher`] for
    /// `Parquet` sinks.
    ///
    /// Pass `Some(Box::new(ParquetSnapshotFlusher))` to enable Parquet
    /// persistence, or `None` for channel / terminal-only output.
    pub fn from_config_with_flusher(
        config: MarketWorkerConfig,
        flusher: Option<Box<dyn SnapshotFlusher>>,
    ) -> Result<Self, ConnectError> {
        Self::from_config_full(config, flusher, None, None)
    }

    /// Create a MarketWorker with optional flusher, terminal callback, and flush callback.
    pub fn from_config_full(
        config: MarketWorkerConfig,
        flusher: Option<Box<dyn SnapshotFlusher>>,
        terminal_cb: Option<TerminalEventCallback>,
        flush_cb: Option<BufferedSinkFlushCallback>,
    ) -> Result<Self, ConnectError> {
        let period_us = config.sync.period_us();
        let clock_mode = match config.sync.sync_mode {
            SyncMode::OnOrderbook => ClockMode::OrderbookDriven,
            SyncMode::OnTrade => ClockMode::TradeDriven,
            SyncMode::OnLiquidation => ClockMode::LiquidationDriven,
            SyncMode::OnTime => ClockMode::ExternalClock,
        };
        let mut sync = MarketSynchronizer::with_clock_mode(period_us, clock_mode);
        let flush_threshold = config.sync.flush_threshold;
        let framework_ingest = config.framework_ingest;

        // Live reconciliation is gated to the synchronized path at cadence
        // ≥ 50ms (below that the hold-back buffers too many periods to be
        // meaningful) and requires the framework engine. Loud failures, never
        // a silent downgrade.
        let reconcile = config.reconcile.clone().filter(|r| r.enabled);
        if let Some(r) = &reconcile {
            if period_us < 50_000 {
                return Err(ConnectError::Build(format!(
                    "[collect.reconcile] requires a sync cadence of 50ms or \
                     greater (configured period is {period_us}us) — lower the \
                     update_frequency or disable reconcile"
                )));
            }
            if !framework_ingest {
                return Err(ConnectError::Build(
                    "[collect.reconcile] requires framework_ingest = true \
                     (the legacy path has no sentinel to trigger it)"
                        .to_string(),
                ));
            }
            if config.emission_delay.is_some() {
                return Err(ConnectError::Build(
                    "emission_delay is set at both [collect] and \
                     [collect.reconcile] — keep the one that owns the hold-back \
                     window and remove the other"
                        .to_string(),
                ));
            }
            // The disclosed latency cost: rows emit W after their boundary so
            // REST-recovered prints land in their true rows.
            sync.set_emission_delay_us(r.emission_delay_us());
        } else if let Some(w) = &config.emission_delay {
            sync.set_emission_delay_us(w.as_micros());
        }

        let channel_capacity = config.common.channel_capacity();
        let symbol = config.common.symbol.clone();
        let exchange_name = config.common.exchange.clone();

        let ob_depth = config.common.datatypes.orderbook.depth;
        let core = IngestionCore::new(config.common.clone())?;

        // Build registry for channel sink (if requested).
        let has_channel = config
            .output
            .iter()
            .any(|s| matches!(s, OutputSinkConfig::Channel));

        let registry = if has_channel {
            Some(TopicRegistry::from_config(
                &config.common.exchange,
                &config.common.symbol,
                &config.common.datatypes,
                channel_capacity,
            ))
        } else {
            None
        };

        // The worker keeps the snapshot-channel handle so downstream
        // consumers can subscribe to the synchronized stream before run().
        let snapshot_channel = has_channel.then(|| {
            crate::workers::topic_publisher::SnapshotChannel::new(
                crate::workers::topic_publisher::DEFAULT_SNAPSHOT_CHANNEL_CAPACITY,
            )
        });
        let sinks = build_sinks(
            &config.output,
            registry,
            snapshot_channel.clone(),
            flusher,
            terminal_cb,
            flush_cb,
            config.common.datatypes.declared_set(),
        )
        .map_err(|e| ConnectError::Sink(e.to_string()))?;

        // Coverage ledger rides beside the parquet output (no parquet sink →
        // metrics-only sentinel, no persisted ledger).
        let gap_ledger = config.output.iter().find_map(|s| match s {
            OutputSinkConfig::Parquet { dir } => {
                Some(crate::workers::gap_ledger::GapLedger::new(
                    std::path::Path::new(dir),
                    &config.common.exchange,
                    &config.common.symbol,
                ))
            }
            _ => None,
        });

        let environment = config.common.environment;
        Ok(Self {
            core,
            sync,
            sinks,
            flush_threshold,
            symbol,
            exchange_name,
            channel_capacity,
            clock_mode,
            period_us,
            ob_depth,
            environment,
            cmd_rx: None,
            status_tx: None,
            framework_ingest,
            snapshot_channel,
            gap_ledger,
            reconcile,
        })
    }

    /// The live broadcast handle for this worker's grid-aligned
    /// [`MarketSnapshot`](aetelier_types::snapshots::MarketSnapshot) stream —
    /// `Some` when the config carries a `Channel` sink. Subscribe before
    /// [`run`](Self::run); a slow subscriber observes `Lagged(n)` with the
    /// exact number of snapshots it missed.
    pub fn snapshot_channel(
        &self,
    ) -> Option<&crate::workers::topic_publisher::SnapshotChannel> {
        self.snapshot_channel.as_ref()
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

    /// Run the synchronised ingestion loop until `shutdown` signals `true`
    /// or a [`WorkerCommand::Stop`] is received.
    ///
    /// When the clock mode is [`ClockMode::ExternalClock`], a
    /// `tokio::time::interval` drives the grid via `sync.on_time()`.
    /// For all other modes the grid is driven by the data events
    /// themselves (orderbook / trade / liquidation timestamps).
    ///
    /// If a command receiver was attached via [`Self::with_command_receiver`],
    /// the event loop also listens for `Pause`, `Resume`, `Stop`, and
    /// `Restart` commands.
    ///
    /// Returns a [`MarketWorkerReport`] with session statistics.
    pub async fn run(
        self,
        shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<MarketWorkerReport> {
        if self.framework_ingest
            && let Some(adapter) = crate::framework::registry::registry()
                .get(self.exchange_name.as_str())
                .copied()
        {
            // Fall back to the legacy path only when an enabled derivative
            // datatype is outside this adapter's supported set, so those
            // channels are actually subscribed instead of being silently
            // filtered out of the FeedSet. Mirrors DataWorker's guard.
            use aetelier_types::config::markets::market_config::DeclaredDatatype as DD;
            let supported = adapter.supported_datatypes();
            let dt = self.core.datatypes();
            let unsupported = (dt.liquidations.enabled
                && !supported.contains(&DD::Liquidations))
                || (dt.funding_rates.enabled && !supported.contains(&DD::FundingRates))
                || (dt.open_interest.enabled && !supported.contains(&DD::OpenInterest));
            if unsupported {
                tracing::warn!(
                    venue = %self.exchange_name,
                    "framework_ingest set but an enabled derivative datatype is \
                     outside this adapter's supported set, using legacy path"
                );
            } else {
                // A REST-seeded venue needs a seeder. Binance (SeqDelta), KuCoin
                // (REST-seeded SeqDelta) and Bitso (L3 REST snapshot) have one
                // wired in `run_framework`; every other venue self-seeds
                // (FullRefresh / ChecksumDelta / WssSelfSeed) and needs none.
                // Fall back to the legacy path only if a seeder is required but
                // unavailable.
                let needs_rest = adapter.book_model("orders").needs_rest();
                if !needs_rest || adapter.rest_seeder().is_some() {
                    return self.run_framework(shutdown).await;
                }
                tracing::warn!(
                    venue = %self.exchange_name,
                    "framework_ingest set but no REST seeder for this venue; using legacy path"
                );
            }
        }

        // Destructure so `core` can be moved into the spawned task while
        // the remaining fields stay available for the event loop.
        let MarketWorker {
            core,
            mut sync,
            sinks,
            flush_threshold,
            channel_capacity,
            exchange_name,
            symbol,
            clock_mode,
            period_us,
            ob_depth,
            environment: _,
            cmd_rx,
            status_tx,
            framework_ingest: _,
            snapshot_channel: _,
            gap_ledger: _,
            reconcile: _,
        } = self;

        // Wrap cmd_rx so the select! arm compiles regardless of Some/None.
        let mut cmd_rx = cmd_rx;

        // Snapshot initial status fields for periodic updates.
        let initial_datatypes = core.datatypes().enabled_names();
        let (initial_market_type, initial_sinks) = status_tx
            .as_ref()
            .map(|tx| {
                let s = tx.borrow();
                (s.market_type, s.sinks.clone())
            })
            .unwrap_or_default();

        // Pipeline: passthrough for most exchanges, BookInitializer for Binance.
        let exchange_enum: aetelier_types::exchanges::Exchange = exchange_name
            .parse()
            .unwrap_or(aetelier_types::exchanges::Exchange::Bybit);
        let pair = TradingPair::from_exchange_symbol(&symbol, exchange_enum).ok_or_else(
            || {
                anyhow::anyhow!(
                    "invalid symbol '{symbol}' for exchange {exchange_enum:?}"
                )
            },
        )?;
        let pipeline = crate::workers::pipeline::build_pipeline(
            exchange_enum,
            &symbol,
            core.datatypes(),
        );

        let (core_tx, core_rx) = mpsc::channel(channel_capacity);
        let core_handle = tokio::spawn(core.run(shutdown.clone(), core_tx));

        let (pipeline_tx, mut rx) = mpsc::channel(channel_capacity);
        let _pipeline_handle = tokio::spawn(pipeline.run(core_rx, pipeline_tx, shutdown));

        let mut snapshots_produced: u64 = 0;
        let mut snapshots_since_flush: u64 = 0;
        let mut flushes: u32 = 0;

        // ── Live status tracking ───────────────────────────────────────
        let start = tokio::time::Instant::now();
        let mut total_events: u64 = 0;
        let mut events_since_last_tick: u64 = 0;
        let mut msgs_per_sec: f64 = 0.0;
        let mut status_interval = tokio::time::interval(
            std::time::Duration::from_millis(STATUS_PUBLISH_INTERVAL_MS),
        );
        status_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut connection_state = crate::ConnectionState::Connecting;

        // When true, the WSS connection stays open but events are
        // discarded and not fed to the synchroniser.
        let mut paused = false;

        // ── Event loop ──────────────────────────────────────────────────
        if clock_mode == ClockMode::ExternalClock {
            // Timer-driven path: a periodic tick calls sync.on_time() to
            // advance the grid. Data events are accumulated passively. Tick
            // at the grid period directly in microseconds — truncating to
            // milliseconds mis-ticks a sub-millisecond or non-round period.
            let tick_us = period_us.max(1);
            let mut timer =
                tokio::time::interval(std::time::Duration::from_micros(tick_us));

            tracing::info!(tick_us = tick_us, "market_worker.external_clock_started");

            loop {
                tokio::select! {
                    _ = timer.tick() => {
                        if paused { continue; }

                        let now_us = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_micros() as u64;

                        sync.on_time(now_us);

                        let n = drain_and_emit(&mut sync, &sinks);
                        snapshots_produced += n;
                        snapshots_since_flush += n;
                        maybe_flush(&sinks, flush_threshold, &mut snapshots_since_flush, &mut flushes)?;
                    }

                    msg = rx.recv() => {
                        match msg {
                            Some(msg) => {
                                total_events += 1;
                                events_since_last_tick += 1;
                                if matches!(connection_state, crate::ConnectionState::Connecting) {
                                    connection_state = crate::ConnectionState::Streaming;
                                }
                                if !paused {
                                    feed_event(&mut sync, &exchange_name, &pair, &msg, ob_depth);
                                    let _ = sinks.emit_raw(&msg.topic, &msg.payload, msg.received_at_us);
                                }
                            }
                            None => break, // IngestionCore exited
                        }
                    }

                    cmd = recv_cmd(&mut cmd_rx) => {
                        match cmd {
                            Some(WorkerCommand::Pause) => {
                                tracing::info!("market_worker.paused");
                                paused = true;
                                connection_state = crate::ConnectionState::Paused;
                            }
                            Some(WorkerCommand::Resume) => {
                                tracing::info!("market_worker.resumed");
                                paused = false;
                                connection_state = crate::ConnectionState::Streaming;
                            }
                            Some(WorkerCommand::Stop) => {
                                tracing::info!("market_worker.stop_requested");
                                break;
                            }
                            Some(WorkerCommand::Restart) => {
                                tracing::info!("market_worker.restart_requested");
                                break;
                            }
                            None => {}
                        }
                    }

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
                                mode: WorkerMode::Clock {
                                    clock: clock_mode,
                                    period_us,
                                },
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
        } else {
            // Data-driven path: events drive the grid.
            loop {
                tokio::select! {
                    msg = rx.recv() => {
                        match msg {
                            Some(msg) => {
                                total_events += 1;
                                events_since_last_tick += 1;
                                if matches!(connection_state, crate::ConnectionState::Connecting) {
                                    connection_state = crate::ConnectionState::Streaming;
                                }
                                if paused { continue; }
                                feed_event(&mut sync, &exchange_name, &pair, &msg, ob_depth);
                                let _ = sinks.emit_raw(&msg.topic, &msg.payload, msg.received_at_us);

                                let n = drain_and_emit(&mut sync, &sinks);
                                snapshots_produced += n;
                                snapshots_since_flush += n;
                                maybe_flush(&sinks, flush_threshold, &mut snapshots_since_flush, &mut flushes)?;
                            }
                            None => break, // IngestionCore exited
                        }
                    }

                    cmd = recv_cmd(&mut cmd_rx) => {
                        match cmd {
                            Some(WorkerCommand::Pause) => {
                                tracing::info!("market_worker.paused");
                                paused = true;
                                connection_state = crate::ConnectionState::Paused;
                            }
                            Some(WorkerCommand::Resume) => {
                                tracing::info!("market_worker.resumed");
                                paused = false;
                                connection_state = crate::ConnectionState::Streaming;
                            }
                            Some(WorkerCommand::Stop) => {
                                tracing::info!("market_worker.stop_requested");
                                break;
                            }
                            Some(WorkerCommand::Restart) => {
                                tracing::info!("market_worker.restart_requested");
                                break;
                            }
                            None => {}
                        }
                    }

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
                                mode: WorkerMode::Clock {
                                    clock: clock_mode,
                                    period_us,
                                },
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
        }

        // ── Final drain + flush ─────────────────────────────────────────
        sync.finalize();
        let final_snapshots = sync.drain();
        for snapshot in &final_snapshots {
            let _ = sinks.emit_snapshot(snapshot);
            snapshots_produced += 1;
        }
        sinks.flush()?;
        flushes += 1;

        let ingestion = core_handle.await??;

        tracing::info!(
            exchange = exchange_name.as_str(),
            symbol = symbol.as_str(),
            snapshots_produced = snapshots_produced,
            flushes = flushes,
            "market_worker.stopped"
        );

        Ok(MarketWorkerReport {
            ingestion,
            snapshots_produced,
            flushes,
        })
    }

    /// Framework ingestion path: the registered adapter feeds a `SourceRuntime`
    /// that reconstructs the book; reconstructed events drive the same
    /// `MarketSynchronizer` + sinks as the legacy path.
    async fn run_framework(
        self,
        mut shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<MarketWorkerReport> {
        let MarketWorker {
            core,
            mut sync,
            sinks,
            flush_threshold,
            symbol,
            exchange_name,
            channel_capacity,
            clock_mode,
            period_us,
            ob_depth,
            environment,
            mut cmd_rx,
            status_tx,
            gap_ledger,
            reconcile,
            ..
        } = self;

        // Auto-wire the subscribed order-book depth onto the framework books
        // (the standalone / persistence path); `0` means the config left it
        // unset, so keep the full book.
        let config_depth = (ob_depth != 0).then_some(ob_depth);

        let adapter = crate::framework::registry::registry()
            .get(exchange_name.as_str())
            .copied()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "run_framework entered for unregistered venue '{exchange_name}'"
                )
            })?;

        if let Err(e) = crate::framework::registry::admit_declared_depth(
            &exchange_name,
            adapter.max_declared_depth(),
            core.datatypes().orderbook.enabled,
            ob_depth,
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
        let model = adapter.book_model("orders");
        // The adapter is the single source of truth for the seeding
        // taxonomy: SnapshotSource drives needs_rest and the recovery
        // action, and the seeder mechanism ships on the same adapter.
        let seeder = adapter.rest_seeder();
        if model.needs_rest() && seeder.is_none() {
            anyhow::bail!(
                "venue '{exchange_name}' declares a REST-seeded model but its \
                 adapter provides no rest_seeder"
            );
        }
        let recovery = model.recovery_action();

        let exchange_enum: Exchange = exchange_name.parse().unwrap_or(Exchange::Binance);
        let initial_datatypes = core.datatypes().enabled_names();
        // One Feed per (instrument, datatype) this worker subscribes; owned
        // here so FeedIds survive socket reconnects. Liquidations remain on
        // the legacy path and enter the feed taxonomy when ported.
        let feeds = {
            use crate::framework::feed::{Feed, FeedDatatype, FeedSet};
            let list: Vec<Feed> = initial_datatypes
                .iter()
                .filter_map(|name| match name.as_str() {
                    "orderbook" => Some(FeedDatatype::Orders),
                    "trades" => Some(FeedDatatype::Trades),
                    "funding_rates" => Some(FeedDatatype::FundingRates),
                    "open_interest" => Some(FeedDatatype::OpenInterest),
                    _ => None,
                })
                .map(|dt| Feed::new(exchange_enum, pair.clone(), dt))
                .collect();
            FeedSet::new(list).into_shared()
        };
        let (initial_market_type, initial_sinks) = status_tx
            .as_ref()
            .map(|tx| {
                let s = tx.borrow();
                (s.market_type, s.sinks.clone())
            })
            .unwrap_or_default();

        let mut snapshots_produced: u64 = 0;
        let mut snapshots_since_flush: u64 = 0;
        let mut flushes: u32 = 0;
        let mut total_events: u64 = 0;
        let mut events_since_last_tick: u64 = 0;
        let mut msgs_per_sec: f64 = 0.0;
        let mut reconnects: u32 = 0;
        // Shared per-source counters: one handle, cloned into each spawn so the
        // transport/runtime/normalizer tasks increment the same atomics. Wired
        // for reconnects here; the deep transport/runtime/drop counters land with
        // the metrics-exposure follow-on (F4b).
        let metrics = SourceMetrics::default();
        // Feed-staleness EWMA in µs: local receipt minus the book's exchange
        // event timestamp. Updated per Book event, published with the status.
        let mut latency_us_ewma: f64 = 0.0;
        let start = tokio::time::Instant::now();
        let mut status_interval = tokio::time::interval(
            std::time::Duration::from_millis(STATUS_PUBLISH_INTERVAL_MS),
        );
        status_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut connection_state = crate::ConnectionState::Connecting;
        let mut paused = false;
        let mut stopped = false;

        let external_clock = clock_mode == ClockMode::ExternalClock;
        // Tick at the grid period in microseconds (see the ExternalClock
        // branch above); truncating to milliseconds mis-ticks fine periods.
        let tick_us = period_us.max(1);
        let mut timer = tokio::time::interval(std::time::Duration::from_micros(tick_us));
        let codec = adapter.profile().symbol_codec.clone();

        // Jittered reconnect policy from the TOML [reconnect] section, with
        // framework defaults for unset knobs: 1s -> 30s backoff, 0.5 jitter,
        // and NO max-attempts (the agent supervisor owns the catastrophic
        // tail; reconnect-forever is the shipped default). Setting
        // max_attempts arms the circuit breaker.
        let mut reconnect_policy = framework_reconnect_policy(core.reconnect_section());

        // Cross-reconnect trade-sequence carry: each new runtime seeds its
        // tradebooks from the prior connection's last applied sequence, so an
        // outage's lost prints are counted by the same exact arithmetic
        // (without it, `trades_lost` only ever saw within-connection gaps).
        let trade_seq_carry: std::sync::Arc<
            std::sync::Mutex<std::collections::HashMap<TradingPair, u64>>,
        > = std::sync::Arc::default();
        let mut seen_trade_ids = adapter
            .resubscribe_replays_trades()
            .then(|| SeenTradeIds::with_capacity(REPLAY_DEDUP_CAPACITY));
        // ── Live reconciliation (metered feature) ────────────────────────
        // A background task fetches venue REST trades AFTER the last print
        // the live stream delivered, on two bounded triggers: a gap-recovery
        // incident (once per incident) and an optional slow sweep. Recovered
        // prints flow back through `rec_rx`, are deduped against recently-
        // seen ids, stamped `origin = rest`, and land in their true grid
        // rows thanks to the emission hold-back window W.
        let reconciler = reconcile.as_ref().and_then(|r| {
            match crate::framework::reconcile::trades_rest_fetcher(exchange_name.as_str())
            {
                Some(f) => Some((r.clone(), f)),
                None => {
                    tracing::warn!(
                        exchange = %exchange_name,
                        "reconcile.enabled_but_venue_rest_cannot_repair_gaps"
                    );
                    None
                }
            }
        });
        let last_pos: std::sync::Arc<
            std::sync::Mutex<
                std::collections::HashMap<
                    TradingPair,
                    crate::framework::reconcile::TradePos,
                >,
            >,
        > = std::sync::Arc::default();
        let (rec_tx, mut rec_rx) = mpsc::channel::<aetelier_types::trades::Trade>(1024);
        let (trigger_tx, mut trigger_rx) = mpsc::channel::<()>(4);
        let mut recent_ids: std::collections::VecDeque<u64> =
            std::collections::VecDeque::new();
        if let Some((rcfg, fetcher)) = &reconciler {
            let fetcher = fetcher.clone();
            let last_pos = last_pos.clone();
            let metrics_c = metrics.clone();
            let wire = symbol.clone();
            let pair_c = pair.clone();
            let sweep = rcfg.sweep_secs;
            let mut shutdown_r = shutdown.clone();
            tokio::spawn(async move {
                let mut sweep_int = (sweep > 0).then(|| {
                    tokio::time::interval(std::time::Duration::from_secs(sweep))
                });
                if let Some(i) = &mut sweep_int {
                    i.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    i.tick().await; // the first tick fires immediately — skip it
                }
                loop {
                    tokio::select! {
                        _ = shutdown_r.changed() => {
                            if *shutdown_r.borrow() { break; }
                        }
                        t = trigger_rx.recv() => { if t.is_none() { break; } }
                        _ = async {
                            match &mut sweep_int {
                                Some(i) => { i.tick().await; }
                                None => std::future::pending().await,
                            }
                        } => {}
                    }
                    let pos = last_pos.lock().ok().and_then(|m| m.get(&pair_c).copied());
                    // No live print yet — nothing to anchor a fetch after.
                    let Some(pos) = pos else { continue };
                    metrics_c.bump_reconcile_fetches();
                    match fetcher.fetch_after(&wire, &pair_c, pos).await {
                        Ok(trades) => {
                            for t in trades {
                                if rec_tx.send(t).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            metrics_c.bump_reconcile_failures();
                            tracing::warn!(error = %e, "reconcile.fetch_failed");
                        }
                    }
                }
            });
        }
        let reconcile_active = reconciler.is_some();

        // Sentinel state: trailing trade arrivals (rate model for the
        // possible-loss estimate on unarmed venues) + the open gap window
        // (monotonic instant for the duration, epoch µs + cause for the
        // persisted ledger record).
        let mut recent_trades: std::collections::VecDeque<tokio::time::Instant> =
            std::collections::VecDeque::new();
        let mut gap_open: Option<(
            tokio::time::Instant,
            u64,
            crate::workers::gap_ledger::GapCause,
        )> = None;
        let epoch_us_now = || {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64
        };

        let mut drain_partial = false;
        // Reconnect loop: a runtime that ends on `ResyncRequired` (a self-seeded
        // book gapped) or a socket disconnect re-establishes the connection — a
        let declared_set = core.datatypes().declared_set();
        // fresh subscribe re-seeds. Shutdown / `Stop` exits.
        while !stopped && !*shutdown.borrow() {
            // Ingest: adapter -> DomainEvent. Sync: DomainEvent -> ReconstructedEvent.
            if let Ok(mut fs) = feeds.lock() {
                fs.mark_all_subscribing();
            }
            let mut last_book_ts_us = 0u64;
            let mut last_trade_ts_us = 0u64;
            let (dev_tx, dev_rx) = mpsc::channel(channel_capacity);
            let adapter_handle = adapter.spawn_env(
                environment,
                vec![symbol.clone()],
                declared_set.clone(),
                dev_tx,
                shutdown.clone(),
                metrics.clone(),
            );
            let runtime = SourceRuntime::new(
                exchange_name.clone(),
                codec.clone(),
                vec![symbol.clone()],
                model.clone(),
                recovery,
                metrics.clone(),
                declared_set.clone(),
            )
            .with_feeds(feeds.clone())
            .with_trade_seq_carry(trade_seq_carry.clone())
            .with_max_depth(config_depth);
            let (recon_tx, mut recon_rx) = mpsc::channel(channel_capacity);
            let runtime_handle = tokio::spawn(runtime.run(
                dev_rx,
                seeder.clone(),
                recon_tx,
                shutdown.clone(),
            ));
            if !matches!(
                connection_state,
                crate::ConnectionState::Reconnecting { .. }
            ) {
                connection_state = crate::ConnectionState::Connecting;
            }
            // Confirmed once per connection, when the first reconstructed event
            // proves the link is live — mirrors the legacy path's
            // `manager.on_connected()` so the failure counter and circuit
            // breaker reset on recovery rather than accumulating for the
            // worker's whole lifetime.
            let mut connection_live = false;

            loop {
                tokio::select! {
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() { stopped = true; break; }
                    }

                    _ = timer.tick(), if external_clock => {
                        if !paused {
                            let now_us = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_micros() as u64;
                            sync.on_time(now_us);
                            let n = drain_and_emit(&mut sync, &sinks);
                            snapshots_produced += n;
                            snapshots_since_flush += n;
                            maybe_flush(&sinks, flush_threshold, &mut snapshots_since_flush, &mut flushes)?;
                        }
                    }

                    ev = recon_rx.recv() => match ev {
                        Some(ev) => {
                            total_events += 1;
                            events_since_last_tick += 1;
                            connection_state = crate::ConnectionState::Streaming;
                            // The first event on a fresh connection confirms the
                            // link: reset the failure counter and close the
                            // circuit breaker. Every event resets the backoff
                            // ladder so the next disconnect starts from initial.
                            if !connection_live {
                                connection_live = true;
                                reconnect_policy.on_connected();
                                // Sentinel: the gap window closes at proven
                                // recovery. Record the coverage incident +
                                // ledger line, and — only where loss
                                // accounting is Estimated (no dense trade
                                // sequence) — a labeled rate-model estimate of
                                // prints possibly dropped in the window. Armed
                                // venues need no estimate: the seq carry
                                // counts an outage exactly.
                                if let Some((opened, opened_epoch, cause)) =
                                    gap_open.take()
                                {
                                    // Reconcile AFTER recovery: the REST
                                    // window now covers the whole outage.
                                    // Once per incident by construction.
                                    if reconcile_active {
                                        let _ = trigger_tx.try_send(());
                                    }
                                    let window_us =
                                        opened.elapsed().as_micros() as u64;
                                    metrics.record_book_gap_incident(window_us);
                                    let estimated = metrics
                                        .snapshot()
                                        .trade_loss_confidence
                                        == crate::framework::budget::TradeLossConfidence::Estimated as u64;
                                    let mut estimate = None;
                                    if estimated {
                                        while recent_trades.front().is_some_and(
                                            |t| t.elapsed().as_secs() > 60,
                                        ) {
                                            recent_trades.pop_front();
                                        }
                                        let n = crate::workers::gap_ledger::estimate_possible_dropped(
                                            recent_trades.len(),
                                            window_us,
                                        );
                                        metrics.record_trade_suspicion(
                                            window_us, n,
                                        );
                                        estimate = Some(n);
                                    }
                                    if let Some(ledger) = &gap_ledger {
                                        let incident =
                                            crate::workers::gap_ledger::GapIncident {
                                                opened_epoch_us: opened_epoch,
                                                closed_epoch_us: epoch_us_now(),
                                                window_us,
                                                cause,
                                                exchange: exchange_name.clone(),
                                                symbol: symbol.clone(),
                                                possible_dropped_trades: estimate,
                                            };
                                        if let Err(e) = ledger.append(&incident) {
                                            metrics.bump_flush_failures();
                                            tracing::warn!(
                                                error = %e,
                                                "market_worker.gap_ledger_append_failed"
                                            );
                                        }
                                    }
                                }
                            }
                            reconnect_policy.on_message_received();
                            if paused { continue; }
                            match ev {
                                ReconstructedEvent::Book { pair, ts_us, book } => {
                                    if ts_us > 0
                                        && wire_ts_regressed(&mut last_book_ts_us, ts_us)
                                    {
                                        metrics.bump_out_of_order_frames();
                                    }
                                    if ts_us > 0 {
                                        let now_us = std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .unwrap_or_default()
                                            .as_micros() as u64;
                                        let sample = now_us.saturating_sub(ts_us) as f64;
                                        latency_us_ewma = if latency_us_ewma <= 0.0 {
                                            sample
                                        } else {
                                            0.2 * sample + 0.8 * latency_us_ewma
                                        };
                                    }
                                    sync.on_orderbook(&pair, ts_us, book);
                                }
                                ReconstructedEvent::FundingRate(fr) => {
                                    sync.on_funding(fr);
                                }
                                ReconstructedEvent::OpenInterest(oi) => {
                                    sync.on_open_interest(oi);
                                }
                                ReconstructedEvent::FundingSettlement(fs) => {
                                    sync.on_funding_settlement(fs);
                                }
                                ReconstructedEvent::Trade(trade) => {
                                    if let Some(seen) = seen_trade_ids.as_mut()
                                        && let Ok(tid) = trade.id.parse::<u64>()
                                        && !seen.insert(tid)
                                    {
                                        metrics.bump_replay_duplicates();
                                        continue;
                                    }
                                    if trade.source_trade_ts_us > 0
                                        && wire_ts_regressed(
                                            &mut last_trade_ts_us,
                                            trade.source_trade_ts_us,
                                        )
                                    {
                                        metrics.bump_out_of_order_frames();
                                    }
                                    if trade.source_trade_ts_us > 0 {
                                        let now_us = std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .unwrap_or_default()
                                            .as_micros() as u64;
                                        let sample = now_us.saturating_sub(trade.source_trade_ts_us) as f64;
                                        latency_us_ewma = if latency_us_ewma <= 0.0 {
                                            sample
                                        } else {
                                            0.2 * sample + 0.8 * latency_us_ewma
                                        };
                                    }
                                    // Trailing-rate sample for the sentinel
                                    // estimator (bounded: 60s window, hard cap).
                                    recent_trades.push_back(tokio::time::Instant::now());
                                    while recent_trades
                                        .front()
                                        .is_some_and(|t| t.elapsed().as_secs() > 60)
                                        || recent_trades.len() > 4096
                                    {
                                        recent_trades.pop_front();
                                    }
                                    // Reconciler anchor + dedup ring: the last
                                    // delivered venue id/ts is what REST
                                    // fetches resume AFTER.
                                    if reconcile_active
                                        && let Ok(idn) = trade.id.parse::<u64>()
                                    {
                                        if let Ok(mut m) = last_pos.lock() {
                                            m.insert(
                                                trade.pair.clone(),
                                                crate::framework::reconcile::TradePos {
                                                    id: idn,
                                                    ts_us: trade.source_trade_ts_us,
                                                },
                                            );
                                        }
                                        recent_ids.push_back(idn);
                                        if recent_ids.len() > 2048 {
                                            recent_ids.pop_front();
                                        }
                                    }
                                    sync.on_trade(trade);
                                }
                            }
                            if !external_clock {
                                let n = drain_and_emit(&mut sync, &sinks);
                                snapshots_produced += n;
                                snapshots_since_flush += n;
                                maybe_flush(&sinks, flush_threshold, &mut snapshots_since_flush, &mut flushes)?;
                            }
                        }
                        None => break, // runtime ended — leave the inner loop
                    },

                    // REST-recovered prints (live reconciliation): dedup
                    // against recently-seen ids (a fetch can race the live
                    // stream), count, and feed the synchronizer — the
                    // hold-back window homes them in their true rows. When
                    // the feature is off, `rec_tx` stays alive and unfired so
                    // this arm never wakes.
                    Some(mut rt) = rec_rx.recv() => {
                        if !paused {
                            let idn = rt.id.parse::<u64>().unwrap_or(0);
                            let dup = idn != 0 && recent_ids.contains(&idn);
                            if !dup {
                                if idn != 0 {
                                    recent_ids.push_back(idn);
                                    if recent_ids.len() > 2048 {
                                        recent_ids.pop_front();
                                    }
                                    if let Ok(mut m) = last_pos.lock() {
                                        let e = m.entry(rt.pair.clone()).or_default();
                                        if idn > e.id {
                                            *e = crate::framework::reconcile::TradePos {
                                                id: idn,
                                                ts_us: rt.source_trade_ts_us,
                                            };
                                        }
                                    }
                                }
                                rt.origin = aetelier_types::trades::TradeOrigin::Rest;
                                metrics.add_trades_recovered(1);
                                tracing::info!(
                                    id = %rt.id,
                                    ts_us = rt.source_trade_ts_us,
                                    "reconcile.print_recovered"
                                );
                                sync.on_trade(rt);
                            }
                        }
                    }

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
                                mode: WorkerMode::Clock {
                                    clock: clock_mode,
                                    period_us,
                                },
                                connection_state,
                                messages_per_sec: msgs_per_sec,
                                total_events,
                                reconnect_count: reconnects,
                                sinks: initial_sinks.clone(),
                                datatypes: initial_datatypes.clone(),
                                feeds: feeds
                                    .lock()
                                    .map(|f| f.snapshot())
                                    .unwrap_or_default(),
                                uptime_secs: start.elapsed().as_secs_f64(),
                                ws_latency_us: Some(latency_us_ewma),
                                source_metrics: metrics.snapshot(),
                            });
                        }
                    }
                }
            }

            // Drop the receiver so a runtime parked on a full `out.send`
            // unblocks (its send fails) and returns instead of hanging.
            drop(recon_rx);
            let adapter_exit = adapter_handle.await.unwrap_or_else(|_| {
                TaskExit::Failed(
                    crate::clients::disconnect::DisconnectReason::TransportError {
                        source: "adapter task panicked".into(),
                    },
                )
            });
            let outcome = runtime_handle.await.unwrap_or(RuntimeOutcome::Finished);

            if stopped || *shutdown.borrow() {
                drain_partial = matches!(adapter_exit, TaskExit::DrainTimedOut);
                break;
            }
            // Unexpected end (resync or disconnect) — reconnect under the
            // jittered policy. A runtime-driven resync (healthy gap recovery)
            // reads as CleanClose and retries immediately; venue-socket
            // failures back off with jitter so a fleet never thunders.
            //
            // Sentinel: the coverage gap opens here (covers both resync and
            // disconnect causes) and closes at the next connection's first
            // reconstructed event. Repeated failed reconnects extend the same
            // window rather than opening new incidents.
            if gap_open.is_none() {
                let cause = if matches!(outcome, RuntimeOutcome::ResyncRequired) {
                    crate::workers::gap_ledger::GapCause::Resync
                } else {
                    crate::workers::gap_ledger::GapCause::Disconnect
                };
                gap_open = Some((tokio::time::Instant::now(), epoch_us_now(), cause));
            }
            reconnects += 1;
            metrics.bump_reconnects();
            connection_state = crate::ConnectionState::Reconnecting {
                attempt: reconnects,
            };
            if let Ok(mut fs) = feeds.lock() {
                fs.mark_all_reconnecting();
            }
            let policy_reason = match adapter_exit {
                TaskExit::Failed(reason) => reason,
                TaskExit::Completed | TaskExit::DrainTimedOut => {
                    crate::clients::disconnect::DisconnectReason::CleanClose
                }
            };
            tracing::warn!(
                exchange = exchange_name.as_str(),
                symbol = symbol.as_str(),
                attempt = reconnects,
                outcome = ?outcome,
                reason = %policy_reason,
                "market_worker.framework_reconnecting"
            );
            match reconnect_policy.next_action(&policy_reason) {
                ReconnectAction::RetryImmediately => {}
                ReconnectAction::RetryAfter(delay) => {
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = shutdown.changed() => { if *shutdown.borrow() { break; } }
                    }
                }
                ReconnectAction::CircuitOpen { until } => {
                    // Unreachable with max_attempts(None); guarded anyway.
                    tokio::select! {
                        _ = tokio::time::sleep_until(until) => {}
                        _ = shutdown.changed() => { if *shutdown.borrow() { break; } }
                    }
                }
                ReconnectAction::GiveUp { reason } => {
                    // Non-retryable (venue protocol rejection): retrying would
                    // hammer a rejection loop — stop this worker loudly.
                    tracing::error!(
                        exchange = exchange_name.as_str(),
                        symbol = symbol.as_str(),
                        %reason,
                        "market_worker.framework_giving_up"
                    );
                    connection_state = crate::ConnectionState::Disconnected;
                    if let Ok(mut fs) = feeds.lock() {
                        if matches!(
                            policy_reason,
                            crate::clients::disconnect::DisconnectReason::ProtocolRejection { .. }
                        ) {
                            fs.mark_all_rejected(&reason.to_string());
                        } else {
                            fs.mark_all_failed(&reason.to_string());
                        }
                    }
                    stopped = true;
                }
            }
        }

        // A gap still open at stop (GiveUp / shutdown mid-outage) is recorded
        // up to the stop instant so the coverage ledger never under-reports.
        if let Some((opened, opened_epoch, cause)) = gap_open.take() {
            let window_us = opened.elapsed().as_micros() as u64;
            metrics.record_book_gap_incident(window_us);
            if let Some(ledger) = &gap_ledger {
                let incident = crate::workers::gap_ledger::GapIncident {
                    opened_epoch_us: opened_epoch,
                    closed_epoch_us: epoch_us_now(),
                    window_us,
                    cause,
                    exchange: exchange_name.clone(),
                    symbol: symbol.clone(),
                    possible_dropped_trades: None,
                };
                if let Err(e) = ledger.append(&incident) {
                    metrics.bump_flush_failures();
                    tracing::warn!(error = %e, "market_worker.gap_ledger_append_failed");
                }
            }
        }
        if let Ok(mut fs) = feeds.lock() {
            fs.mark_all_draining();
        }
        sync.finalize();
        for snapshot in &sync.drain() {
            let _ = sinks.emit_snapshot(snapshot);
            snapshots_produced += 1;
        }
        sinks.flush()?;
        flushes += 1;
        if let Ok(mut fs) = feeds.lock() {
            fs.mark_all_closed(drain_partial);
        }

        let m = metrics.snapshot();
        tracing::info!(
            exchange = exchange_name.as_str(),
            symbol = symbol.as_str(),
            reconnects = m.reconnects,
            decode_err = m.decode_err,
            dropped_frames = m.dropped_frames,
            gaps = m.gaps,
            resyncs = m.resyncs,
            checksum_fail = m.checksum_fail,
            flush_failures = m.flush_failures,
            "market_worker.framework_stopped_metrics"
        );

        let elapsed = start.elapsed().as_secs_f64();
        let ingestion = IngestionReport {
            exchange: exchange_name.clone(),
            symbol: symbol.clone(),
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
        };

        tracing::info!(
            exchange = exchange_name.as_str(),
            symbol = symbol.as_str(),
            snapshots_produced = snapshots_produced,
            flushes = flushes,
            "market_worker.framework_stopped"
        );

        Ok(MarketWorkerReport {
            ingestion,
            snapshots_produced,
            flushes,
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared loop helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Drain ready snapshots and emit them to sinks. Returns the count produced.
#[inline]
fn drain_and_emit(sync: &mut MarketSynchronizer, sinks: &OutputSinkSet) -> u64 {
    let ready = sync.drain();
    let n = ready.len() as u64;
    for snapshot in &ready {
        let _ = sinks.emit_snapshot(snapshot);
    }
    n
}

/// Flush if the running snapshot counter has reached the threshold.
#[inline]
fn maybe_flush(
    sinks: &OutputSinkSet,
    flush_threshold: usize,
    snapshots_since_flush: &mut u64,
    flushes: &mut u32,
) -> anyhow::Result<()> {
    if flush_threshold > 0 && *snapshots_since_flush >= flush_threshold as u64 {
        sinks.flush()?;
        *snapshots_since_flush = 0;
        *flushes += 1;
    }
    Ok(())
}

/// Receive the next command, or pend forever when no receiver is attached.
/// Resolve the framework path's ReconnectPolicy from the raw TOML
/// `[reconnect]` section. Unset fields take the framework defaults (1 s
/// initial, 30 s cap, infinite attempts, 0.5 jitter) rather than the
/// legacy `ConnectionManagerConfig` defaults.
fn framework_reconnect_policy(
    section: Option<&crate::config::workers::common::ReconnectSection>,
) -> crate::clients::reconnect::ReconnectPolicy {
    let default = crate::config::workers::common::ReconnectSection::default();
    let s = section.unwrap_or(&default);
    ReconnectPolicy::builder()
        .initial_delay(
            s.initial_delay_ms
                .map(std::time::Duration::from_millis)
                .unwrap_or(std::time::Duration::from_secs(1)),
        )
        .max_delay(
            s.max_delay_ms
                .map(std::time::Duration::from_millis)
                .unwrap_or(std::time::Duration::from_secs(30)),
        )
        .max_attempts(s.max_attempts)
        .jitter_factor(s.jitter_factor.unwrap_or(0.5))
        .build()
}

async fn recv_cmd(
    cmd_rx: &mut Option<mpsc::Receiver<WorkerCommand>>,
) -> Option<WorkerCommand> {
    match cmd_rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Free-function event routing (avoids partial-move issues with `self`)
// ─────────────────────────────────────────────────────────────────────────────

/// Route a [`TopicMessage`] into the synchroniser.
///
/// Converts exchange-specific events into normalised types and calls
/// the appropriate `sync.on_*()` method.
fn feed_event(
    sync: &mut MarketSynchronizer,
    _exchange_name: &str,
    pair: &TradingPair,
    msg: &TopicMessage,
    ob_depth: usize,
) {
    match &msg.payload {
        ExchangeEvent::Bybit(bybit_event) => feed_bybit(sync, pair, bybit_event),
        ExchangeEvent::Coinbase(coinbase_event) => {
            feed_coinbase(sync, pair, coinbase_event, ob_depth)
        }
        ExchangeEvent::Kraken(kraken_event) => feed_kraken(sync, pair, kraken_event),
        ExchangeEvent::Binance(binance_event) => {
            feed_binance(sync, pair, binance_event, ob_depth)
        }
        ExchangeEvent::Okx(okx_event) => feed_okx(sync, pair, okx_event, ob_depth),
        ExchangeEvent::Gateio(gate_event) => feed_gate(sync, pair, gate_event, ob_depth),
    }
}

fn feed_bybit(
    sync: &mut MarketSynchronizer,
    pair: &TradingPair,
    event: &crate::sources::bybit::events::BybitWssEvent,
) {
    use crate::sources::bybit::events::BybitWssEvent;

    match event {
        BybitWssEvent::TradeData(trades) => {
            // A frame may batch many fills — feed each one (no head-drop).
            for data in trades {
                let Ok(side) = data.side.parse::<aetelier_types::TradeSide>() else {
                    tracing::warn!(side = %data.side, "bybit.trade.unknown_side");
                    continue;
                };
                let trade = aetelier_types::trades::Trade {
                    source_trade_ts_us: data.trade_ts * 1_000,
                    local_trade_ts_us: 0,
                    source_trade_rtt_us: 0,
                    pair: pair.clone(),
                    side,
                    amount: data
                        .amount
                        .parse::<rust_decimal::Decimal>()
                        .unwrap_or(rust_decimal::Decimal::ZERO),
                    price: data
                        .price
                        .parse::<rust_decimal::Decimal>()
                        .unwrap_or(rust_decimal::Decimal::ZERO),
                    exchange: "bybit".to_string(),
                    id: data.trade_id.clone(),
                    origin: Default::default(),
                };
                sync.on_trade(trade);
            }
        }
        BybitWssEvent::OrderbookData(resp) => {
            // BybitOrderbookResponse.orderbook_ts_ms is the exchange timestamp (venue ms).
            // BybitOrderbookData.bids / .asks are Vec<BybitPriceLevel>.
            // BybitPriceLevel is a tuple struct with .price_str() and .size_str() methods.
            use aetelier_types::orderbooks::f64_to_decimal;

            let bids: Vec<Level> = resp
                .data
                .bids
                .iter()
                .enumerate()
                .map(|(i, level)| {
                    Level::new(
                        i as u32,
                        OrderSide::Bids,
                        f64_to_decimal(level.price()),
                        f64_to_decimal(level.size()),
                        vec![],
                    )
                })
                .collect();
            let asks: Vec<Level> = resp
                .data
                .asks
                .iter()
                .enumerate()
                .map(|(i, level)| {
                    Level::new(
                        i as u32,
                        OrderSide::Asks,
                        f64_to_decimal(level.price()),
                        f64_to_decimal(level.size()),
                        vec![],
                    )
                })
                .collect();
            let ts_us = resp.orderbook_ts_ms * 1_000;
            let ob = aetelier_types::orderbooks::Orderbook::from_levels(
                0,
                ts_us,
                pair.clone(),
                "bybit".to_string(),
                bids,
                asks,
            );
            sync.on_orderbook(pair, ts_us, ob);
        }
        BybitWssEvent::LiquidationData(data) => {
            let liq = aetelier_types::liquidations::Liquidation {
                liquidation_ts_us: data.liquidation_ts_ms * 1_000,
                pair: pair.clone(),
                side: data
                    .side
                    .parse::<aetelier_types::TradeSide>()
                    .unwrap_or(aetelier_types::TradeSide::Sell),
                price: data
                    .price
                    .parse::<rust_decimal::Decimal>()
                    .unwrap_or(rust_decimal::Decimal::ZERO),
                amount: data
                    .amount
                    .parse::<rust_decimal::Decimal>()
                    .unwrap_or(rust_decimal::Decimal::ZERO),
                exchange: "bybit".to_string(),
            };
            sync.on_liquidation(liq);
        }
        BybitWssEvent::TickerData(data) => {
            if let Some(ref fr_str) = data.funding_rate
                && let Ok(fr_val) = fr_str.parse::<rust_decimal::Decimal>()
            {
                // next_funding_time is Option<String> (venue ms as string);
                // convert to the platform microsecond standard. 0 stays 0
                // (unknown), matching the sibling funding_rate_ts_us.
                let next_ts_ms: u64 = data
                    .next_funding_time
                    .as_deref()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);
                let interval_hours = data
                    .funding_interval_hour
                    .as_deref()
                    .and_then(|s| s.parse::<u32>().ok())
                    .filter(|h| *h > 0)
                    .unwrap_or(8);
                let fr = aetelier_types::funding::FundingRate {
                    funding_rate_ts_us: data.ts.unwrap_or(0) * 1_000,
                    local_funding_ts_us: 0,
                    recv_seq: 0,
                    conn_epoch: 0,
                    pair: pair.clone(),
                    funding_rate: fr_val,
                    premium: None,
                    interval_hours,
                    next_funding_ts_us: next_ts_ms * 1_000,
                    exchange: "bybit".to_string(),
                };
                sync.on_funding(fr);
            }
            if let Some(ref oi_str) = data.open_interest
                && let Ok(oi_val) = oi_str.parse::<rust_decimal::Decimal>()
            {
                let oi_value = data
                    .open_interest_value
                    .as_deref()
                    .and_then(|s| s.parse::<rust_decimal::Decimal>().ok());
                let oi = aetelier_types::open_interest::OpenInterest {
                    open_interest_ts_us: data.ts.unwrap_or(0) * 1_000,
                    local_oi_ts_us: 0,
                    recv_seq: 0,
                    conn_epoch: 0,
                    pair: pair.clone(),
                    open_interest: oi_val,
                    open_interest_value: oi_value,
                    mark_px: data
                        .mark_price
                        .as_deref()
                        .and_then(|s| s.parse::<rust_decimal::Decimal>().ok()),
                    exchange: "bybit".to_string(),
                };
                sync.on_open_interest(oi);
            }
        }
    }
}

fn feed_coinbase(
    sync: &mut MarketSynchronizer,
    pair: &TradingPair,
    event: &crate::sources::coinbase::events::CoinbaseWssEvent,
    ob_depth: usize,
) {
    use crate::sources::coinbase::events::CoinbaseWssEvent;

    match event {
        CoinbaseWssEvent::TradeData(trades) => {
            // A frame may batch many trades — feed each one (no head-drop).
            for data in trades {
                // CoinbaseTradeData.time is ISO 8601 string → use timestamp_us() helper.
                let Ok(side) = data.side.parse::<aetelier_types::TradeSide>() else {
                    tracing::warn!(side = %data.side, "coinbase.trade.unknown_side");
                    continue;
                };
                let trade = aetelier_types::trades::Trade {
                    source_trade_ts_us: data.timestamp_us(),
                    local_trade_ts_us: 0,
                    source_trade_rtt_us: 0,
                    pair: pair.clone(),
                    side,
                    amount: data
                        .size
                        .parse::<rust_decimal::Decimal>()
                        .unwrap_or(rust_decimal::Decimal::ZERO),
                    price: data
                        .price
                        .parse::<rust_decimal::Decimal>()
                        .unwrap_or(rust_decimal::Decimal::ZERO),
                    exchange: "coinbase".to_string(),
                    id: data.trade_id.clone(),
                    origin: Default::default(),
                };
                sync.on_trade(trade);
            }
        }
        CoinbaseWssEvent::OrderbookData(resp) => {
            // CoinbaseOrderbookResponse has:
            //   timestamp: String (ISO 8601)
            //   events: Vec<CoinbaseL2Event>
            // Each CoinbaseL2Event has product_id + updates: Vec<CoinbaseL2Update>.
            // Each update has side ("bid"/"offer"), price_level, new_quantity.
            let Some(event) = resp.events.first() else {
                return;
            };
            let ts_us: u64 = chrono::DateTime::parse_from_rfc3339(&resp.timestamp)
                .map(|dt| dt.timestamp_micros() as u64)
                .unwrap_or(0);

            use aetelier_types::orderbooks::f64_to_decimal;

            let mut bids = Vec::new();
            let mut asks = Vec::new();
            for (i, update) in event.updates.iter().enumerate() {
                let price =
                    f64_to_decimal(update.price_level.parse::<f64>().unwrap_or(0.0));
                let volume =
                    f64_to_decimal(update.new_quantity.parse::<f64>().unwrap_or(0.0));
                match update.side.as_str() {
                    "bid" => bids.push(Level::new(
                        i as u32,
                        OrderSide::Bids,
                        price,
                        volume,
                        vec![],
                    )),
                    "offer" | "ask" => asks.push(Level::new(
                        i as u32,
                        OrderSide::Asks,
                        price,
                        volume,
                        vec![],
                    )),
                    _ => {}
                }
            }

            // Coinbase sends the full book — truncate to configured depth.
            bids.truncate(ob_depth);
            asks.truncate(ob_depth);

            let ob = aetelier_types::orderbooks::Orderbook::from_levels(
                0,
                ts_us,
                pair.clone(),
                "coinbase".to_string(),
                bids,
                asks,
            );
            sync.on_orderbook(pair, ts_us, ob);
        }
        // Sequenced control frames feed the FRAMEWORK path's continuity
        // tracker; the legacy feed has no per-connection arithmetic.
        CoinbaseWssEvent::Heartbeat { .. } | CoinbaseWssEvent::Control { .. } => {}
    }
}

fn feed_kraken(
    sync: &mut MarketSynchronizer,
    pair: &TradingPair,
    event: &crate::sources::kraken::events::KrakenWssEvent,
) {
    use crate::sources::kraken::events::KrakenWssEvent;

    match event {
        KrakenWssEvent::TradeData(trades) => {
            // Kraken v2 batches multiple trades per frame — feed each one (no
            // head-drop).
            for data in trades {
                // KrakenTradeData: price/qty are f64 (not strings).
                // timestamp is String → use timestamp_us() helper.
                let Ok(side) = data.side.parse::<aetelier_types::TradeSide>() else {
                    tracing::warn!(side = %data.side, "kraken.trade.unknown_side");
                    continue;
                };
                let trade = aetelier_types::trades::Trade {
                    source_trade_ts_us: data.timestamp_us(),
                    local_trade_ts_us: 0,
                    source_trade_rtt_us: 0,
                    pair: pair.clone(),
                    side,
                    amount: aetelier_types::orderbooks::f64_to_decimal(data.qty),
                    price: aetelier_types::orderbooks::f64_to_decimal(data.price),
                    exchange: "kraken".to_string(),
                    id: data.trade_id.to_string(),
                    origin: Default::default(),
                };
                sync.on_trade(trade);
            }
        }
        KrakenWssEvent::OrderbookData(resp) => {
            // KrakenBookResponse.data: Vec<KrakenBookData> — take first entry.
            // KrakenBookData has symbol, bids, asks, checksum, timestamp.
            // KrakenPriceLevel has price: f64, qty: f64.
            let Some(book) = resp.data.first() else {
                return;
            };
            let ts_us: u64 = chrono::DateTime::parse_from_rfc3339(&book.timestamp)
                .map(|dt| dt.timestamp_micros() as u64)
                .unwrap_or(0);

            use aetelier_types::orderbooks::f64_to_decimal;

            let bids: Vec<Level> = book
                .bids
                .iter()
                .enumerate()
                .map(|(i, level)| {
                    Level::new(
                        i as u32,
                        OrderSide::Bids,
                        f64_to_decimal(level.price.parse::<f64>().unwrap_or(0.0)),
                        f64_to_decimal(level.qty.parse::<f64>().unwrap_or(0.0)),
                        vec![],
                    )
                })
                .collect();
            let asks: Vec<Level> = book
                .asks
                .iter()
                .enumerate()
                .map(|(i, level)| {
                    Level::new(
                        i as u32,
                        OrderSide::Asks,
                        f64_to_decimal(level.price.parse::<f64>().unwrap_or(0.0)),
                        f64_to_decimal(level.qty.parse::<f64>().unwrap_or(0.0)),
                        vec![],
                    )
                })
                .collect();
            let ob = aetelier_types::orderbooks::Orderbook::from_levels(
                0,
                ts_us,
                pair.clone(),
                "kraken".to_string(),
                bids,
                asks,
            );
            sync.on_orderbook(pair, ts_us, ob);
        }
    }
}

fn feed_binance(
    sync: &mut MarketSynchronizer,
    pair: &TradingPair,
    event: &crate::sources::binance::events::BinanceWssEvent,
    ob_depth: usize,
) {
    use crate::sources::binance::events::BinanceWssEvent;

    match event {
        BinanceWssEvent::TradeData(data) => {
            let Ok(side) = data.taker_side().parse::<aetelier_types::TradeSide>() else {
                tracing::warn!(side = %data.taker_side(), "binance.trade.unknown_side");
                return;
            };
            let trade = aetelier_types::trades::Trade {
                source_trade_ts_us: data.trade_time * 1_000,
                local_trade_ts_us: 0,
                source_trade_rtt_us: 0,
                pair: pair.clone(),
                side,
                amount: data
                    .quantity
                    .parse::<rust_decimal::Decimal>()
                    .unwrap_or(rust_decimal::Decimal::ZERO),
                price: data
                    .price
                    .parse::<rust_decimal::Decimal>()
                    .unwrap_or(rust_decimal::Decimal::ZERO),
                exchange: "binance".to_string(),
                id: data.trade_id.to_string(),
                origin: Default::default(),
            };
            sync.on_trade(trade);
        }
        BinanceWssEvent::DepthUpdate(upd) => {
            // After BookInitializer, delta events flow here.
            // Convert to Orderbook for the synchronizer.
            //
            // NOTE: no `.take(ob_depth)` here — diff depth entries are
            // *unordered* changed levels at arbitrary prices.  Truncating
            // by array position would silently drop valid updates.
            // Depth capping for the accumulated book happens downstream in
            // `OrderbookDelta::prune_to_depth()`.
            use aetelier_types::orderbooks::f64_to_decimal;

            let bids: Vec<Level> = upd
                .bids
                .iter()
                .enumerate()
                .map(|(i, level)| {
                    Level::new(
                        i as u32,
                        OrderSide::Bids,
                        f64_to_decimal(level[0].parse::<f64>().unwrap_or(0.0)),
                        f64_to_decimal(level[1].parse::<f64>().unwrap_or(0.0)),
                        vec![],
                    )
                })
                .collect();
            let asks: Vec<Level> = upd
                .asks
                .iter()
                .enumerate()
                .map(|(i, level)| {
                    Level::new(
                        i as u32,
                        OrderSide::Asks,
                        f64_to_decimal(level[0].parse::<f64>().unwrap_or(0.0)),
                        f64_to_decimal(level[1].parse::<f64>().unwrap_or(0.0)),
                        vec![],
                    )
                })
                .collect();
            let ob = aetelier_types::orderbooks::Orderbook::from_levels(
                0,
                upd.event_time,
                pair.clone(),
                "binance".to_string(),
                bids,
                asks,
            );
            sync.on_orderbook(pair, upd.event_time, ob);
        }
        BinanceWssEvent::DepthSnapshot(snap) => {
            // Synthesised by BookInitializer from REST response.
            // Cap at `ob_depth` levels per side.
            use aetelier_types::orderbooks::f64_to_decimal;

            let bids: Vec<Level> = snap
                .bids
                .iter()
                .take(ob_depth)
                .enumerate()
                .map(|(i, level)| {
                    Level::new(
                        i as u32,
                        OrderSide::Bids,
                        f64_to_decimal(level[0].parse::<f64>().unwrap_or(0.0)),
                        f64_to_decimal(level[1].parse::<f64>().unwrap_or(0.0)),
                        vec![],
                    )
                })
                .collect();
            let asks: Vec<Level> = snap
                .asks
                .iter()
                .take(ob_depth)
                .enumerate()
                .map(|(i, level)| {
                    Level::new(
                        i as u32,
                        OrderSide::Asks,
                        f64_to_decimal(level[0].parse::<f64>().unwrap_or(0.0)),
                        f64_to_decimal(level[1].parse::<f64>().unwrap_or(0.0)),
                        vec![],
                    )
                })
                .collect();
            let ob = aetelier_types::orderbooks::Orderbook::from_levels(
                0,
                0,
                pair.clone(),
                "binance".to_string(),
                bids,
                asks,
            );
            sync.on_orderbook(pair, 0, ob);
        }
    }
}

fn feed_okx(
    sync: &mut MarketSynchronizer,
    pair: &TradingPair,
    event: &crate::sources::okx::events::OkxWssEvent,
    ob_depth: usize,
) {
    use crate::sources::okx::events::OkxWssEvent;

    match event {
        OkxWssEvent::TradeData(trades) => {
            // A push may batch multiple prints — feed each one (no head-drop).
            for data in trades {
                let Ok(side) = data.side.parse::<aetelier_types::TradeSide>() else {
                    tracing::warn!(side = %data.side, "okx.trade.unknown_side");
                    continue;
                };
                let trade = aetelier_types::trades::Trade {
                    source_trade_ts_us: data.ts_ms() * 1_000,
                    local_trade_ts_us: 0,
                    source_trade_rtt_us: 0,
                    pair: pair.clone(),
                    side,
                    amount: data
                        .sz
                        .parse::<rust_decimal::Decimal>()
                        .unwrap_or(rust_decimal::Decimal::ZERO),
                    price: data
                        .px
                        .parse::<rust_decimal::Decimal>()
                        .unwrap_or(rust_decimal::Decimal::ZERO),
                    exchange: "okx".to_string(),
                    id: data.trade_id.clone(),
                    origin: Default::default(),
                };
                sync.on_trade(trade);
            }
        }
        OkxWssEvent::OrderbookData(resp) => {
            // `books5` pushes a full top-5 snapshot every 100ms — replace
            // the book each frame (no incremental reconstruction).
            use aetelier_types::orderbooks::f64_to_decimal;

            let Some(book) = resp.data.first() else {
                return;
            };
            let ts_us = book.ts_ms() * 1_000;

            let bids: Vec<Level> = book
                .bids
                .iter()
                .take(ob_depth)
                .enumerate()
                .map(|(i, level)| {
                    Level::new(
                        i as u32,
                        OrderSide::Bids,
                        f64_to_decimal(level.price()),
                        f64_to_decimal(level.size()),
                        vec![],
                    )
                })
                .collect();
            let asks: Vec<Level> = book
                .asks
                .iter()
                .take(ob_depth)
                .enumerate()
                .map(|(i, level)| {
                    Level::new(
                        i as u32,
                        OrderSide::Asks,
                        f64_to_decimal(level.price()),
                        f64_to_decimal(level.size()),
                        vec![],
                    )
                })
                .collect();
            let ob = aetelier_types::orderbooks::Orderbook::from_levels(
                0,
                ts_us,
                pair.clone(),
                "okx".to_string(),
                bids,
                asks,
            );
            sync.on_orderbook(pair, ts_us, ob);
        }
    }
}

fn feed_gate(
    sync: &mut MarketSynchronizer,
    pair: &TradingPair,
    event: &crate::sources::gateio::events::GateioWssEvent,
    ob_depth: usize,
) {
    use crate::sources::gateio::events::GateioWssEvent;

    match event {
        GateioWssEvent::TradeData(data) => {
            let Ok(side) = data.side.parse::<aetelier_types::TradeSide>() else {
                tracing::warn!(side = %data.side, "gateio.trade.unknown_side");
                return;
            };
            let trade = aetelier_types::trades::Trade {
                source_trade_ts_us: data.ts_ms() * 1_000,
                local_trade_ts_us: 0,
                source_trade_rtt_us: 0,
                pair: pair.clone(),
                side,
                amount: data
                    .amount
                    .parse::<rust_decimal::Decimal>()
                    .unwrap_or(rust_decimal::Decimal::ZERO),
                price: data
                    .price
                    .parse::<rust_decimal::Decimal>()
                    .unwrap_or(rust_decimal::Decimal::ZERO),
                exchange: "gateio".to_string(),
                id: data.id.to_string(),
                origin: Default::default(),
            };
            sync.on_trade(trade);
        }
        GateioWssEvent::OrderbookData(resp) => {
            // `spot.order_book` pushes a full limited-depth snapshot at a
            // fixed interval — replace the book each frame.
            use aetelier_types::orderbooks::f64_to_decimal;

            let book = &resp.result;
            let ts_us = book.ts_ms * 1_000;

            let bids: Vec<Level> = book
                .bids
                .iter()
                .take(ob_depth)
                .enumerate()
                .map(|(i, level)| {
                    Level::new(
                        i as u32,
                        OrderSide::Bids,
                        f64_to_decimal(level.price()),
                        f64_to_decimal(level.size()),
                        vec![],
                    )
                })
                .collect();
            let asks: Vec<Level> = book
                .asks
                .iter()
                .take(ob_depth)
                .enumerate()
                .map(|(i, level)| {
                    Level::new(
                        i as u32,
                        OrderSide::Asks,
                        f64_to_decimal(level.price()),
                        f64_to_decimal(level.size()),
                        vec![],
                    )
                })
                .collect();
            let ob = aetelier_types::orderbooks::Orderbook::from_levels(
                0,
                ts_us,
                pair.clone(),
                "gateio".to_string(),
                bids,
                asks,
            );
            sync.on_orderbook(pair, ts_us, ob);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::disconnect::DisconnectReason;
    use crate::clients::reconnect::CircuitState;
    use crate::config::workers::common::ReconnectSection;

    fn venue_failure() -> DisconnectReason {
        DisconnectReason::TransportError {
            source: "test".into(),
        }
    }

    fn reconcile_manifest(cadence_ms: u64, framework: bool) -> MarketWorkerConfig {
        let toml = format!(
            r#"
[collect]
exchange = "binance"
framework_ingest = {framework}

[collect.datatypes.orderbook]
enabled = true
depth = 25

[collect.datatypes.trades]
enabled = true

[collect.sync]
sync_mode = "on_time"
flush_threshold = 600

[collect.sync.update_frequency]
value = {cadence_ms}
unit = "Millis"

[collect.reconcile]
enabled = true

[[workers]]
symbol = "BTCUSDT"
"#
        );
        crate::config::workers::MarketWorkerManifest::from_str(&toml)
            .unwrap()
            .resolve_all()
            .remove(0)
    }

    #[test]
    fn reconcile_below_50ms_cadence_is_rejected_loudly() {
        let err = match MarketWorker::from_config(reconcile_manifest(20, true)) {
            Ok(_) => panic!("sub-50ms cadence must not silently enable reconcile"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("50ms"),
            "error names the constraint: {err}"
        );
    }

    #[test]
    fn reconcile_requires_the_framework_engine() {
        let err = match MarketWorker::from_config(reconcile_manifest(100, false)) {
            Ok(_) => panic!("legacy path must not silently enable reconcile"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("framework_ingest"), "{err}");
    }

    #[test]
    fn reconcile_valid_config_sets_the_holdback_window() {
        let w = MarketWorker::from_config(reconcile_manifest(100, true)).unwrap();
        assert_eq!(
            w.sync.emission_delay_us(),
            1_000_000,
            "default 1s hold-back applied to the synchronizer"
        );
    }

    fn hold_back_manifest(collect_delay: Option<&str>, reconcile: bool) -> String {
        let delay = collect_delay
            .map(|d| format!("\n[collect.emission_delay]\n{d}\n"))
            .unwrap_or_default();
        let reconcile = if reconcile {
            "\n[collect.reconcile]\nenabled = true\n"
        } else {
            ""
        };
        format!(
            r#"
[collect]
exchange = "binance"
framework_ingest = true

[collect.datatypes.orderbook]
enabled = true
depth = 25

[collect.datatypes.trades]
enabled = true

[collect.sync]
sync_mode = "on_time"
flush_threshold = 600

[collect.sync.update_frequency]
value = 100
unit = "Millis"
{delay}{reconcile}
[[workers]]
symbol = "BTCUSDT"
"#
        )
    }

    fn resolve_hold_back(toml: &str) -> MarketWorkerConfig {
        crate::config::workers::MarketWorkerManifest::from_str(toml)
            .unwrap()
            .resolve_all()
            .remove(0)
    }

    #[test]
    fn collect_emission_delay_parses_and_resolves() {
        let cfg = resolve_hold_back(&hold_back_manifest(
            Some("value = 500\nunit = \"Millis\""),
            false,
        ));
        assert_eq!(cfg.emission_delay.as_ref().unwrap().as_micros(), 500_000);
    }

    #[test]
    fn emission_delay_absent_resolves_none() {
        let cfg = resolve_hold_back(&hold_back_manifest(None, false));
        assert!(cfg.emission_delay.is_none());
    }

    #[test]
    fn collect_emission_delay_applies_without_reconcile() {
        let cfg = resolve_hold_back(&hold_back_manifest(
            Some("value = 250\nunit = \"Millis\""),
            false,
        ));
        let w = MarketWorker::from_config(cfg).unwrap();
        assert_eq!(w.sync.emission_delay_us(), 250_000);
    }

    #[test]
    fn emission_delay_conflicts_with_enabled_reconcile_loudly() {
        let cfg = resolve_hold_back(&hold_back_manifest(
            Some("value = 250\nunit = \"Millis\""),
            true,
        ));
        match MarketWorker::from_config(cfg) {
            Ok(_) => panic!("two hold-back sources must not resolve silently"),
            Err(e) => assert!(e.to_string().contains("emission_delay"), "{e}"),
        }
    }

    #[test]
    fn reconcile_default_hold_back_stays_one_second() {
        let cfg = resolve_hold_back(&hold_back_manifest(None, true));
        let w = MarketWorker::from_config(cfg).unwrap();
        assert_eq!(w.sync.emission_delay_us(), 1_000_000);
    }

    #[test]
    fn regressed_timestamp_counts_once_and_does_not_advance() {
        let mut last = 0u64;
        assert!(!wire_ts_regressed(&mut last, 100));
        assert!(!wire_ts_regressed(&mut last, 200));
        assert!(
            wire_ts_regressed(&mut last, 150),
            "a step back is a regress"
        );
        assert_eq!(
            last, 200,
            "a regressing frame must not rewind the watermark"
        );
        assert!(!wire_ts_regressed(&mut last, 250));
    }

    #[test]
    fn equal_timestamps_are_not_out_of_order() {
        let mut last = 0u64;
        assert!(!wire_ts_regressed(&mut last, 1_700_000_000_000_000));
        assert!(
            !wire_ts_regressed(&mut last, 1_700_000_000_000_000),
            "two frames inside the same wire millisecond are legal"
        );
    }

    #[test]
    fn seen_trade_ids_flags_duplicates_and_evicts_at_capacity() {
        let mut seen = SeenTradeIds::with_capacity(3);
        assert!(seen.insert(1), "first sighting is fresh");
        assert!(!seen.insert(1), "a replayed print is a duplicate");
        assert!(seen.insert(2));
        assert!(seen.insert(3));
        assert!(seen.insert(4), "capacity reached, oldest evicted");
        assert!(seen.insert(1), "evicted ids are forgotten, not remembered");
        assert!(!seen.insert(3), "ids still inside the window stay known");
    }

    #[test]
    fn declares_resubscribe_trade_replay_and_unarmed_sequence() {
        use crate::framework::registry::registry;
        let hl = registry().get("hyperliquid").copied().unwrap();
        assert!(
            hl.resubscribe_replays_trades(),
            "hyperliquid replays ~30 prints on every subscribe"
        );
        let bybit = registry().get("bybit").copied().unwrap();
        assert!(
            !bybit.resubscribe_replays_trades(),
            "dedup stays off for venues that do not replay"
        );
    }

    #[test]
    fn framework_policy_defaults_never_arm_the_circuit() {
        let mut policy = framework_reconnect_policy(None);
        for _ in 0..64 {
            let action = policy.next_action(&venue_failure());
            assert!(
                matches!(action, ReconnectAction::RetryAfter(_)),
                "default policy must keep retrying, got {action:?}"
            );
        }
        assert_eq!(policy.circuit_state(), CircuitState::Closed);
    }

    #[test]
    fn configured_max_attempts_arms_the_circuit() {
        let section = ReconnectSection {
            initial_delay_ms: Some(10),
            max_delay_ms: Some(50),
            max_attempts: Some(2),
            jitter_factor: Some(0.0),
        };
        let mut policy = framework_reconnect_policy(Some(&section));
        assert!(matches!(
            policy.next_action(&venue_failure()),
            ReconnectAction::RetryAfter(_)
        ));
        assert!(matches!(
            policy.next_action(&venue_failure()),
            ReconnectAction::CircuitOpen { .. }
        ));
        assert_eq!(policy.circuit_state(), CircuitState::Open);
    }
}
