//! Pluggable output sinks for worker event delivery.
//!
//! The `OutputSink` trait defines the interface that both `DataWorker` and
//! `MarketWorker` use to emit data.  Multiple sinks can be active
//! simultaneously via `OutputSinkSet`, which fans out every call.
//!
//! # Implemented sinks
//!
//! | Sink | Status | Description |
//! |------|--------|-------------|
//! | `ChannelSink` | Working | Wraps existing `TopicRegistry` broadcast channels |
//! | `TerminalSink` | Stub | Debug/tracing terminal output |
//! | `ParquetSink` | Working | Buffers `MarketSnapshot`s, decomposes and flushes to per-datatype Parquet files |
//!
//! # Adding a new sink
//!
//! 1. Implement `OutputSink` for your type.
//! 2. Add a variant to `OutputSinkConfig` in `config::workers::common`.
//! 3. Handle the new variant in `build_sinks()`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::config::workers::OutputSinkConfig;
use crate::framework::model::DomainEvent;
use crate::sources::ExchangeEvent;
use crate::workers::topic_publisher::{
    DEFAULT_SNAPSHOT_CHANNEL_CAPACITY, DomainTopicMessage, DomainTopicRegistry,
    SnapshotChannel, TopicMessage, TopicRegistry,
};
use aetelier_types::config::markets::market_config::DeclaredSet as DeclaredSetAlias;
use aetelier_types::snapshots::MarketSnapshot;

// ─────────────────────────────────────────────────────────────────────────────
// SinkState / SinkStatus — runtime introspection types
// ─────────────────────────────────────────────────────────────────────────────

/// Runtime operational state of an output sink.
///
/// Used by the dashboard to show per-sink status badges
/// (e.g. TERMINAL: streaming, PARQUET: writing 142 MB).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SinkState {
    /// The sink has been created but has not yet received any events.
    Idle,
    /// The sink is actively receiving and processing events.
    Streaming,
    /// The sink is performing a write or flush operation (e.g. Parquet I/O).
    Writing,
    /// The sink's output buffer is full or receivers are lagging.
    Backpressured,
    /// The sink has encountered an error and may not be functional.
    Error,
}

impl std::fmt::Display for SinkState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Idle => write!(f, "idle"),
            Self::Streaming => write!(f, "streaming"),
            Self::Writing => write!(f, "writing"),
            Self::Backpressured => write!(f, "backpressured"),
            Self::Error => write!(f, "error"),
        }
    }
}

/// Snapshot of a sink's runtime status at a point in time.
///
/// All `Option` fields are sink-type-specific — a `TerminalSink` will
/// never report `bytes_written`, while a `ChannelSink` will never report
/// `bytes_written` but will report `queue_depth`.
#[derive(Debug, Clone)]
pub struct SinkStatus {
    /// Human-readable sink name (same as [`OutputSink::name()`]).
    pub name: &'static str,
    /// Current operational state.
    pub state: SinkState,
    /// Total events successfully emitted through this sink.
    pub events_emitted: u64,
    /// Cumulative bytes written to persistent storage (e.g. Parquet files).
    pub bytes_written: Option<u64>,
    /// Current number of pending messages in the sink's output queue.
    pub queue_depth: Option<u64>,
    /// Maximum capacity of the output queue (if bounded).
    pub queue_capacity: Option<u64>,
    /// Total events dropped due to receiver lag or backpressure.
    pub lag_events: Option<u64>,
    /// Filesystem path where this sink writes its output (e.g. Parquet dir).
    ///
    /// Only meaningful for sinks that write to disk. `None` for in-memory
    /// sinks like `TerminalSink` and `ChannelSink`.
    pub output_dir: Option<String>,
}

impl SinkStatus {
    /// Helper to construct a minimal status with only required fields.
    pub fn new(name: &'static str, state: SinkState, events_emitted: u64) -> Self {
        Self {
            name,
            state,
            events_emitted,
            bytes_written: None,
            queue_depth: None,
            queue_capacity: None,
            lag_events: None,
            output_dir: None,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// OutputSink trait
// ─────────────────────────────────────────────────────────────────────────────

/// A destination for worker output.
///
/// Workers call `emit_raw` for unsynchronised events and `emit_snapshot`
/// for grid-aligned snapshots.  Not every sink needs to support both —
/// the default implementations are no-ops.
pub trait OutputSink: Send + Sync {
    /// Emit a raw, unsynchronised event (used by `DataWorker`).
    fn emit_raw(
        &self,
        topic: &str,
        event: &ExchangeEvent,
        received_at_us: u64,
    ) -> anyhow::Result<()> {
        let _ = (topic, event, received_at_us);
        Ok(())
    }

    /// Emit a synchronised snapshot (used by `MarketWorker`).
    fn emit_snapshot(&self, snapshot: &MarketSnapshot) -> anyhow::Result<()> {
        let _ = snapshot;
        Ok(())
    }

    /// Emit a normalized, un-reconstructed event (used by the framework
    /// `DataWorker` path). Default no-op — only `DomainChannelSink` (and any
    /// sink that opts in) handles it, exactly as `emit_raw`/`emit_snapshot` are
    /// handled only by the sinks that care.
    fn emit_domain(
        &self,
        topic: &str,
        event: &DomainEvent,
        received_at_us: u64,
    ) -> anyhow::Result<()> {
        let _ = (topic, event, received_at_us);
        Ok(())
    }

    /// Flush any buffered data.  No-op for non-buffered sinks.
    fn flush(&self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Human-readable sink name for logging.
    fn name(&self) -> &'static str;

    /// Return the sink's current runtime status.
    ///
    /// The default implementation returns an `Idle` status with zero
    /// events emitted.  Concrete sinks should override this to report
    /// meaningful counters and state.
    fn status(&self) -> SinkStatus {
        SinkStatus::new(self.name(), SinkState::Idle, 0)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ChannelSink — wraps existing TopicRegistry
// ─────────────────────────────────────────────────────────────────────────────

/// Publishes raw events to broadcast channels via [`TopicRegistry`].
///
/// This is the primary sink — it preserves the existing pub/sub
/// architecture where downstream consumers subscribe to specific
/// topics and receive `TopicMessage` clones.
pub struct ChannelSink {
    registry: TopicRegistry,
    /// Live broadcast for grid-aligned snapshots (the synchronized stream).
    snapshots: SnapshotChannel,
    /// Running count of events successfully published (raw + snapshot).
    events_emitted: AtomicU64,
    /// Running count of events that failed to publish (unknown topic).
    events_dropped: AtomicU64,
}

impl ChannelSink {
    /// Create a new channel sink wrapping the given registry, with a
    /// default-capacity snapshot channel.
    pub fn new(registry: TopicRegistry) -> Self {
        Self::with_snapshot_channel(
            registry,
            SnapshotChannel::new(DEFAULT_SNAPSHOT_CHANNEL_CAPACITY),
        )
    }

    /// Create a channel sink publishing snapshots into `snapshots` — the
    /// caller keeps a clone as the subscription point (see
    /// `MarketWorker::snapshot_channel`).
    pub fn with_snapshot_channel(
        registry: TopicRegistry,
        snapshots: SnapshotChannel,
    ) -> Self {
        Self {
            registry,
            snapshots,
            events_emitted: AtomicU64::new(0),
            events_dropped: AtomicU64::new(0),
        }
    }

    /// Borrow the underlying registry (for downstream subscriptions).
    pub fn registry(&self) -> &TopicRegistry {
        &self.registry
    }

    /// The snapshot broadcast handle (for downstream subscriptions).
    pub fn snapshots(&self) -> &SnapshotChannel {
        &self.snapshots
    }
}

impl OutputSink for ChannelSink {
    fn emit_raw(
        &self,
        topic: &str,
        event: &ExchangeEvent,
        received_at_us: u64,
    ) -> anyhow::Result<()> {
        let msg = TopicMessage {
            topic: topic.to_string(),
            received_at_us,
            exchange: match event {
                ExchangeEvent::Bybit(_) => "bybit".to_string(),
                ExchangeEvent::Coinbase(_) => "coinbase".to_string(),
                ExchangeEvent::Kraken(_) => "kraken".to_string(),
                ExchangeEvent::Binance(_) => "binance".to_string(),
                ExchangeEvent::Okx(_) => "okx".to_string(),
                ExchangeEvent::Gateio(_) => "gateio".to_string(),
            },
            payload: event.clone(),
        };

        match self.registry.publish(topic, msg) {
            Ok(_) => {
                self.events_emitted.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                self.events_dropped.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(topic = topic, error = %e, "channel_sink.publish_failed");
            }
        }
        Ok(())
    }

    fn emit_snapshot(&self, snapshot: &MarketSnapshot) -> anyhow::Result<()> {
        // Clone into the Arc only when someone is listening; a slow
        // subscriber observes Lagged(n) — the collector never blocks.
        if self.snapshots.receiver_count() > 0
            && self.snapshots.publish(Arc::new(snapshot.clone())).is_ok()
        {
            self.events_emitted.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "channel"
    }

    fn status(&self) -> SinkStatus {
        let events = self.events_emitted.load(Ordering::Relaxed);
        let dropped = self.events_dropped.load(Ordering::Relaxed);

        let (total_depth, total_capacity) = self.registry.queue_stats();
        // Backpressure keys on the WORST single channel, not the aggregate —
        // one saturated hot topic must not hide in the average.
        let mut worst_fill = self.registry.max_fill_ratio().unwrap_or(0.0);
        if self.snapshots.capacity() > 0 {
            worst_fill = worst_fill
                .max(self.snapshots.len() as f64 / self.snapshots.capacity() as f64);
        }

        // State machine:
        //   Error        → publish failures (unknown topic = config bug)
        //   Backpressured → worst single-channel fill ratio > 80%
        //   Streaming     → at least one event has been published
        //   Idle          → no events yet
        let state = if dropped > 0 {
            SinkState::Error
        } else if worst_fill > 0.8 {
            SinkState::Backpressured
        } else if events > 0 {
            SinkState::Streaming
        } else {
            SinkState::Idle
        };

        SinkStatus {
            name: "channel",
            state,
            events_emitted: events,
            bytes_written: None,
            queue_depth: Some(total_depth),
            queue_capacity: Some(total_capacity),
            lag_events: None,
            output_dir: None,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DomainChannelSink — normalized broadcast (framework DataWorker path)
// ─────────────────────────────────────────────────────────────────────────────

/// Publishes normalized [`DomainEvent`]s to broadcast channels via a
/// [`DomainTopicRegistry`]. The framework-path analog of [`ChannelSink`]; it
/// implements `emit_domain` instead of `emit_raw`, so the two coexist without
/// either touching the other's payload type.
pub struct DomainChannelSink {
    registry: DomainTopicRegistry,
    /// Venue id stamped onto each message (`DomainEvent` carries no venue tag).
    exchange: String,
    events_emitted: AtomicU64,
    events_dropped: AtomicU64,
}

impl DomainChannelSink {
    /// Create a domain channel sink over the given registry, stamping `exchange`.
    pub fn new(registry: DomainTopicRegistry, exchange: impl Into<String>) -> Self {
        Self {
            registry,
            exchange: exchange.into(),
            events_emitted: AtomicU64::new(0),
            events_dropped: AtomicU64::new(0),
        }
    }

    /// Borrow the underlying registry (for downstream subscriptions).
    pub fn registry(&self) -> &DomainTopicRegistry {
        &self.registry
    }
}

impl OutputSink for DomainChannelSink {
    fn emit_domain(
        &self,
        topic: &str,
        event: &DomainEvent,
        received_at_us: u64,
    ) -> anyhow::Result<()> {
        let msg = DomainTopicMessage {
            topic: topic.to_string(),
            received_at_us,
            exchange: self.exchange.clone(),
            payload: event.clone(),
        };
        match self.registry.publish(topic, msg) {
            Ok(_) => {
                self.events_emitted.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                self.events_dropped.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(topic = topic, error = %e, "domain_channel_sink.publish_failed");
            }
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "domain_channel"
    }

    fn status(&self) -> SinkStatus {
        let events = self.events_emitted.load(Ordering::Relaxed);
        let dropped = self.events_dropped.load(Ordering::Relaxed);
        let state = if dropped > 0 {
            SinkState::Error
        } else if events > 0 {
            SinkState::Streaming
        } else {
            SinkState::Idle
        };
        SinkStatus::new("domain_channel", state, events)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TerminalSink — stub
// ─────────────────────────────────────────────────────────────────────────────

/// A raw terminal sink event forwarded to the dashboard.
///
/// Lightweight struct carrying just the fields the frontend needs.
/// Sent through an optional callback so the worker manager can relay
/// events to WebSocket clients.
#[derive(Debug, Clone)]
pub struct TerminalSinkRawEvent {
    pub topic: String,
    pub received_at_us: u64,
}

/// Callback type for forwarding terminal sink events.
///
/// The worker manager supplies a closure that broadcasts each event
/// to connected dashboard clients via the WS broadcast channel.
/// Using a boxed closure keeps the SDK decoupled from `tokio::sync::broadcast`.
pub type TerminalEventCallback = Box<dyn Fn(TerminalSinkRawEvent) + Send + Sync>;

/// Event emitted by [`BufferedSink`] after each successful flush.
///
/// Carries enough information for the dashboard to display Parquet write activity.
#[derive(Debug, Clone)]
pub struct BufferedSinkFlushEvent {
    /// Number of snapshots in this flush batch.
    pub snapshot_count: usize,
    /// Bytes written to disk in this flush.
    pub bytes_written: u64,
    /// Number of files written in this flush.
    pub files_written: u32,
    /// Cumulative bytes written across all flushes (running total).
    pub total_bytes: u64,
    /// Cumulative files written across all flushes (running total).
    pub total_files: u64,
    /// Wall-clock UTC epoch microsecond timestamp (from `std::time` or caller).
    pub flushed_at_us: u64,
}

/// Callback type for forwarding buffered sink (Parquet) flush events.
pub type BufferedSinkFlushCallback = Box<dyn Fn(BufferedSinkFlushEvent) + Send + Sync>;

/// Prints events to the terminal via `tracing::debug!`.
///
/// Tracks a running event count so that [`OutputSink::status()`] can
/// report how many events have been logged.
///
/// Optionally forwards raw events to a callback (e.g. for the dashboard
/// terminal viewer) when [`TerminalSink::with_callback`] is used.
pub struct TerminalSink {
    /// Running count of events emitted (raw + snapshot).
    events: AtomicU64,
    /// Optional callback for forwarding raw events to the dashboard.
    event_callback: Option<TerminalEventCallback>,
}

impl TerminalSink {
    /// Create a new terminal sink (no forwarding).
    pub fn new() -> Self {
        Self {
            events: AtomicU64::new(0),
            event_callback: None,
        }
    }

    /// Create a terminal sink that forwards raw events via a callback.
    pub fn with_callback(callback: TerminalEventCallback) -> Self {
        Self {
            events: AtomicU64::new(0),
            event_callback: Some(callback),
        }
    }
}

impl Default for TerminalSink {
    fn default() -> Self {
        Self::new()
    }
}

impl OutputSink for TerminalSink {
    fn emit_raw(
        &self,
        topic: &str,
        _event: &ExchangeEvent,
        received_at_us: u64,
    ) -> anyhow::Result<()> {
        self.events.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            topic = topic,
            received_at_us = received_at_us,
            "terminal_sink.raw_event"
        );
        // Forward to dashboard if callback is wired
        if let Some(ref cb) = self.event_callback {
            cb(TerminalSinkRawEvent {
                topic: topic.to_string(),
                received_at_us,
            });
        }
        Ok(())
    }

    fn emit_snapshot(&self, snapshot: &MarketSnapshot) -> anyhow::Result<()> {
        self.events.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            ts_us = snapshot.ts_us,
            has_ob = snapshot.orderbook.is_some(),
            n_trades = snapshot.trades.len(),
            "terminal_sink.snapshot"
        );
        // Forward snapshot metadata to dashboard callback (same as emit_raw)
        if let Some(ref cb) = self.event_callback {
            // Build a descriptive topic from the snapshot contents
            let mut parts = Vec::new();
            if snapshot.orderbook.is_some() {
                parts.push("orderbook");
            }
            if !snapshot.trades.is_empty() {
                parts.push("trades");
            }
            if !snapshot.liquidations.is_empty() {
                parts.push("liquidations");
            }
            if !snapshot.funding_rate.is_empty() {
                parts.push("funding");
            }
            if !snapshot.open_interest.is_empty() {
                parts.push("oi");
            }
            let topic = if parts.is_empty() {
                "snapshot".to_string()
            } else {
                format!("snapshot:{}", parts.join("+"))
            };
            cb(TerminalSinkRawEvent {
                topic,
                received_at_us: snapshot.ts_us,
            });
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "terminal"
    }

    fn status(&self) -> SinkStatus {
        let count = self.events.load(Ordering::Relaxed);
        let state = if count > 0 {
            SinkState::Streaming
        } else {
            SinkState::Idle
        };
        SinkStatus::new("terminal", state, count)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SnapshotFlusher — callback trait for pluggable persistence
// ─────────────────────────────────────────────────────────────────────────────

/// Report returned by a [`SnapshotFlusher`] after a successful flush.
///
/// Captures how much data was written, enabling `BufferedSink` to
/// track cumulative I/O metrics for the dashboard.
#[derive(Debug, Clone, Default)]
pub struct FlushReport {
    /// Total bytes written to persistent storage in this flush.
    pub bytes_written: u64,
    /// Number of files written or updated.
    pub files_written: u32,
}

impl FlushReport {
    /// Create a new flush report.
    pub fn new(bytes_written: u64, files_written: u32) -> Self {
        Self {
            bytes_written,
            files_written,
        }
    }
}

/// Trait for flushing buffered [`MarketSnapshot`]s to persistent storage.
///
/// `aetelier-connect` defines the interface; concrete implementations
/// (e.g. Parquet, CSV) live in downstream crates such as `aetelier-io`.
/// This decouples the worker runtime from any specific serialization
/// library.
///
/// # Example
///
/// ```rust,ignore
/// // In aetelier-io (behind `connect` + `parquet` features):
/// pub struct ParquetSnapshotFlusher { /* … */ }
///
/// impl SnapshotFlusher for ParquetSnapshotFlusher {
///     fn flush_snapshots(
///         &self,
///         snapshots: &[MarketSnapshot],
///         output_dir: &str,
///     ) -> anyhow::Result<FlushReport> {
///         // … write Parquet files …
///         Ok(FlushReport::new(bytes, files))
///     }
/// }
/// ```
pub trait SnapshotFlusher: Send + Sync {
    /// Persist a batch of snapshots to the given output directory.
    ///
    /// Returns a [`FlushReport`] with the number of bytes and files
    /// written, so that the calling sink can track cumulative I/O.
    fn flush_snapshots(
        &self,
        snapshots: &[MarketSnapshot],
        output_dir: &str,
    ) -> Result<FlushReport, aetelier_types::errors::PersistError>;
}

// ─────────────────────────────────────────────────────────────────────────────
// BufferedSink — snapshot buffering + pluggable flush
// ─────────────────────────────────────────────────────────────────────────────

/// Buffers [`MarketSnapshot`]s in memory and delegates persistence to an
/// injected [`SnapshotFlusher`] on [`OutputSink::flush`].
///
/// Raw events (`emit_raw`) are **not** persisted — converting exchange-
/// specific events to normalised types would duplicate the classifier logic.
/// Use the [`TerminalSink`] or [`ChannelSink`] for raw event output.
///
/// # Construction
///
/// ```rust,ignore
/// use aetelier_io::ParquetSnapshotFlusher; // from aetelier-io
///
/// let flusher = ParquetSnapshotFlusher::new();
/// let sink = BufferedSink::new("./output".into(), Box::new(flusher));
/// ```
pub struct BufferedSink {
    output_dir: String,
    snapshot_buffer: std::sync::Mutex<Vec<MarketSnapshot>>,
    flusher: Box<dyn SnapshotFlusher>,
    /// Running count of snapshots emitted into the buffer.
    events_emitted: AtomicU64,
    /// Cumulative bytes written across all flushes.
    bytes_written: AtomicU64,
    /// Total number of files written across all flushes.
    files_written: AtomicU64,
    /// Count of failed flush batches (the buffer is retained for retry).
    flush_failures: AtomicU64,
    /// Count of snapshots dropped oldest-first on buffer overflow (a dead disk
    /// degrades loudly instead of growing the buffer without bound).
    dropped_snapshots: AtomicU64,
    /// Optional callback fired after each successful flush.
    flush_callback: Option<BufferedSinkFlushCallback>,
    declared: DeclaredSetAlias,
    undeclared_stripped: AtomicU64,
}

/// Hard cap on buffered snapshots before the oldest are dropped. At the 250 ms
/// grid this is ~14k/hour per symbol, so the cap tolerates a multi-hour flush
/// outage before shedding, then sheds loudly rather than OOMing.
const MAX_BUFFERED_SNAPSHOTS: usize = 200_000;
/// Batch shed on overflow, to amortize the cost of dropping from the front.
const OVERFLOW_DROP_CHUNK: usize = 8_192;

impl BufferedSink {
    /// Create a new buffered sink that will delegate flushes to `flusher`.
    pub fn new(output_dir: String, flusher: Box<dyn SnapshotFlusher>) -> Self {
        Self {
            output_dir,
            snapshot_buffer: std::sync::Mutex::new(Vec::new()),
            flusher,
            events_emitted: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            files_written: AtomicU64::new(0),
            flush_failures: AtomicU64::new(0),
            dropped_snapshots: AtomicU64::new(0),
            flush_callback: None,
            declared: DeclaredSetAlias::all(),
            undeclared_stripped: AtomicU64::new(0),
        }
    }

    pub fn with_declared(mut self, declared: DeclaredSetAlias) -> Self {
        self.declared = declared;
        self
    }

    pub fn total_undeclared_stripped(&self) -> u64 {
        self.undeclared_stripped.load(Ordering::Relaxed)
    }

    fn strip_undeclared(&self, snapshot: &MarketSnapshot) -> MarketSnapshot {
        use aetelier_types::config::markets::market_config::DeclaredDatatype as DD;
        let mut out = snapshot.clone();
        let mut stripped = 0u64;
        for dt in DD::ALL {
            if self.declared.contains(dt) {
                continue;
            }
            match dt {
                DD::Orderbook => {
                    if out.orderbook.take().is_some() {
                        stripped += 1;
                    }
                }
                DD::Trades => {
                    if !out.trades.is_empty() {
                        stripped += out.trades.len() as u64;
                        out.trades.clear();
                    }
                }
                DD::Liquidations => {
                    if !out.liquidations.is_empty() {
                        stripped += out.liquidations.len() as u64;
                        out.liquidations.clear();
                    }
                }
                DD::FundingRates => {
                    if !out.funding_rate.is_empty() {
                        stripped += out.funding_rate.len() as u64;
                        out.funding_rate.clear();
                    }
                }
                DD::OpenInterest => {
                    if !out.open_interest.is_empty() {
                        stripped += out.open_interest.len() as u64;
                        out.open_interest.clear();
                    }
                }
            }
        }
        if stripped > 0 {
            let total = self
                .undeclared_stripped
                .fetch_add(stripped, Ordering::Relaxed)
                + stripped;
            if total == stripped {
                tracing::warn!(
                    dir = self.output_dir.as_str(),
                    stripped,
                    "buffered_sink.undeclared_datatypes_stripped"
                );
            }
        }
        out
    }

    /// Count of failed flush batches (buffer retained for retry).
    pub fn total_flush_failures(&self) -> u64 {
        self.flush_failures.load(Ordering::Relaxed)
    }

    /// Count of snapshots dropped on buffer overflow.
    pub fn total_dropped_snapshots(&self) -> u64 {
        self.dropped_snapshots.load(Ordering::Relaxed)
    }

    /// Create a new buffered sink with a flush callback for dashboard streaming.
    pub fn with_flush_callback(
        output_dir: String,
        flusher: Box<dyn SnapshotFlusher>,
        cb: BufferedSinkFlushCallback,
    ) -> Self {
        Self {
            flush_callback: Some(cb),
            ..Self::new(output_dir, flusher)
        }
    }

    /// Cumulative bytes written to disk across all flushes.
    pub fn total_bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// Total number of files written across all flushes.
    pub fn total_files_written(&self) -> u64 {
        self.files_written.load(Ordering::Relaxed)
    }
}

impl OutputSink for BufferedSink {
    fn emit_raw(
        &self,
        topic: &str,
        _event: &ExchangeEvent,
        _received_at_us: u64,
    ) -> anyhow::Result<()> {
        tracing::trace!(
            topic = topic,
            "buffered_sink.emit_raw (raw events not persisted)"
        );
        Ok(())
    }

    fn emit_snapshot(&self, snapshot: &MarketSnapshot) -> anyhow::Result<()> {
        self.events_emitted.fetch_add(1, Ordering::Relaxed);
        let snapshot = self.strip_undeclared(snapshot);
        let mut buf = self.snapshot_buffer.lock().unwrap();
        if buf.len() >= MAX_BUFFERED_SNAPSHOTS {
            // A sustained flush outage would otherwise grow the buffer without
            // bound — shed the oldest batch and count it, loudly.
            let drop = OVERFLOW_DROP_CHUNK.min(buf.len());
            buf.drain(0..drop);
            let total = self
                .dropped_snapshots
                .fetch_add(drop as u64, Ordering::Relaxed)
                + drop as u64;
            tracing::warn!(
                dropped = drop,
                total_dropped = total,
                dir = self.output_dir.as_str(),
                "buffered_sink.buffer_full_dropping_oldest"
            );
        }
        buf.push(snapshot);
        Ok(())
    }

    fn flush(&self) -> anyhow::Result<()> {
        let mut buf = self.snapshot_buffer.lock().unwrap();
        if buf.is_empty() {
            return Ok(());
        }

        // On a write failure the buffer is retained (not cleared) and the error
        // propagates so the caller can retry at the next threshold; count it.
        let report = match self.flusher.flush_snapshots(&buf, &self.output_dir) {
            Ok(report) => report,
            Err(e) => {
                self.flush_failures.fetch_add(1, Ordering::Relaxed);
                return Err(e.into());
            }
        };

        let n = buf.len();
        buf.clear();

        self.bytes_written
            .fetch_add(report.bytes_written, Ordering::Relaxed);
        self.files_written
            .fetch_add(report.files_written as u64, Ordering::Relaxed);

        let total_bytes = self.bytes_written.load(Ordering::Relaxed);
        let total_files = self.files_written.load(Ordering::Relaxed);

        tracing::info!(
            dir = self.output_dir.as_str(),
            snapshot_count = n,
            bytes_written = report.bytes_written,
            files_written = report.files_written,
            "buffered_sink.flushed"
        );

        // Forward flush event to dashboard callback
        if let Some(ref cb) = self.flush_callback {
            let now_us = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_micros() as u64)
                .unwrap_or(0);
            cb(BufferedSinkFlushEvent {
                snapshot_count: n,
                bytes_written: report.bytes_written,
                files_written: report.files_written,
                total_bytes,
                total_files,
                flushed_at_us: now_us,
            });
        }

        Ok(())
    }

    fn name(&self) -> &'static str {
        "buffered"
    }

    fn status(&self) -> SinkStatus {
        let events = self.events_emitted.load(Ordering::Relaxed);
        let bytes = self.bytes_written.load(Ordering::Relaxed);

        // Determine state from buffer occupancy.
        let buffer_len = self.snapshot_buffer.lock().map(|b| b.len()).unwrap_or(0);

        let state = if buffer_len > 0 {
            SinkState::Writing
        } else if events > 0 {
            SinkState::Streaming
        } else {
            SinkState::Idle
        };

        SinkStatus {
            name: "buffered",
            state,
            events_emitted: events,
            bytes_written: Some(bytes),
            queue_depth: Some(buffer_len as u64),
            queue_capacity: None,
            lag_events: None,
            output_dir: Some(self.output_dir.clone()),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// OutputSinkSet — fan-out wrapper
// ─────────────────────────────────────────────────────────────────────────────

/// Fan-out wrapper that delegates every call to all contained sinks.
pub struct OutputSinkSet {
    sinks: Vec<Box<dyn OutputSink>>,
}

impl OutputSinkSet {
    /// Create an empty sink set.
    pub fn new() -> Self {
        Self { sinks: Vec::new() }
    }

    /// Add a sink to the set.
    pub fn push(&mut self, sink: Box<dyn OutputSink>) {
        self.sinks.push(sink);
    }

    /// Number of sinks in the set.
    pub fn len(&self) -> usize {
        self.sinks.len()
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.sinks.is_empty()
    }

    /// Collect status snapshots from all contained sinks.
    pub fn statuses(&self) -> Vec<SinkStatus> {
        self.sinks.iter().map(|s| s.status()).collect()
    }

    /// Emit a raw event to all sinks.
    pub fn emit_raw(
        &self,
        topic: &str,
        event: &ExchangeEvent,
        received_at_us: u64,
    ) -> anyhow::Result<()> {
        for sink in &self.sinks {
            if let Err(e) = sink.emit_raw(topic, event, received_at_us) {
                tracing::warn!(
                    sink = sink.name(),
                    topic = topic,
                    error = %e,
                    "output_sink_set.emit_raw_failed"
                );
            }
        }
        Ok(())
    }

    /// Emit a normalized domain event to all sinks (framework path).
    pub fn emit_domain(
        &self,
        topic: &str,
        event: &DomainEvent,
        received_at_us: u64,
    ) -> anyhow::Result<()> {
        for sink in &self.sinks {
            if let Err(e) = sink.emit_domain(topic, event, received_at_us) {
                tracing::warn!(
                    sink = sink.name(),
                    topic = topic,
                    error = %e,
                    "output_sink_set.emit_domain_failed"
                );
            }
        }
        Ok(())
    }

    /// Emit a snapshot to all sinks.
    pub fn emit_snapshot(&self, snapshot: &MarketSnapshot) -> anyhow::Result<()> {
        for sink in &self.sinks {
            if let Err(e) = sink.emit_snapshot(snapshot) {
                tracing::warn!(
                    sink = sink.name(),
                    error = %e,
                    "output_sink_set.emit_snapshot_failed"
                );
            }
        }
        Ok(())
    }

    /// Flush all sinks.
    pub fn flush(&self) -> anyhow::Result<()> {
        for sink in &self.sinks {
            if let Err(e) = sink.flush() {
                tracing::warn!(
                    sink = sink.name(),
                    error = %e,
                    "output_sink_set.flush_failed"
                );
            }
        }
        Ok(())
    }
}

impl Default for OutputSinkSet {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Factory
// ─────────────────────────────────────────────────────────────────────────────

/// Build an [`OutputSinkSet`] from config entries.
///
/// The `registry` parameter is consumed by the `Channel` sink — pass
/// `None` if no channel sink is configured (a Channel request without a
/// registry is skipped with a warning).
///
/// For `Parquet` sinks, pass a [`SnapshotFlusher`] implementation via
/// `flusher`. A Parquet config without a flusher is a startup error —
/// accepting it would silently drop every collected row.
///
/// The optional `terminal_cb` is wired into the `TerminalSink` so that
/// raw events are forwarded to the dashboard via the WS broadcast channel.
pub fn build_sinks(
    configs: &[OutputSinkConfig],
    registry: Option<TopicRegistry>,
    snapshots: Option<SnapshotChannel>,
    flusher: Option<Box<dyn SnapshotFlusher>>,
    terminal_cb: Option<TerminalEventCallback>,
    flush_cb: Option<BufferedSinkFlushCallback>,
    declared: aetelier_types::config::markets::market_config::DeclaredSet,
) -> anyhow::Result<OutputSinkSet> {
    let mut set = OutputSinkSet::new();
    let mut registry = registry;
    let mut snapshots = snapshots;
    let mut flusher = flusher;
    let mut terminal_cb = terminal_cb;
    let mut flush_cb = flush_cb;

    for config in configs {
        match config {
            OutputSinkConfig::Channel => {
                if let Some(reg) = registry.take() {
                    tracing::info!("output.channel_sink_created");
                    let snaps = snapshots.take().unwrap_or_else(|| {
                        SnapshotChannel::new(DEFAULT_SNAPSHOT_CHANNEL_CAPACITY)
                    });
                    set.push(Box::new(ChannelSink::with_snapshot_channel(reg, snaps)));
                } else {
                    tracing::warn!("output.channel_sink_requested_but_no_registry");
                }
            }
            OutputSinkConfig::Terminal => {
                tracing::info!("output.terminal_sink_created");
                let sink = match terminal_cb.take() {
                    Some(cb) => TerminalSink::with_callback(cb),
                    None => TerminalSink::new(),
                };
                set.push(Box::new(sink));
            }
            OutputSinkConfig::Parquet { dir } => {
                // A declared Parquet output with no flusher would be silently
                // dropped (data loss with no diagnostic) — fail loudly at
                // construction instead. The worker that owns a snapshot flusher
                // (MarketWorker via `from_config_with_flusher`) supplies one;
                // paths that cannot flush (a non-parquet build, or DataWorker's
                // raw-event output) must not declare a Parquet sink.
                let Some(f) = flusher.take() else {
                    anyhow::bail!(
                        "config declares a Parquet output sink (dir = {dir}) but no \
                         SnapshotFlusher was provided — the data would be silently dropped. \
                         Provide a flusher (e.g. `ParquetSnapshotFlusher` from aetelier-io via \
                         `MarketWorker::from_config_with_flusher`), build with the `parquet` \
                         feature, or remove the Parquet sink from the config."
                    );
                };
                tracing::info!(dir = dir.as_str(), "output.buffered_sink_created");
                let sink = match flush_cb.take() {
                    Some(cb) => BufferedSink::with_flush_callback(dir.clone(), f, cb),
                    None => BufferedSink::new(dir.clone(), f),
                }
                .with_declared(declared.clone());
                set.push(Box::new(sink));
            }
        }
    }

    Ok(set)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use aetelier_types::snapshots::MarketSnapshot;

    // ── Helper: minimal no-op sink that uses default status() ────────────

    struct NoopSink;

    impl OutputSink for NoopSink {
        fn name(&self) -> &'static str {
            "noop"
        }
    }

    // ── Helper: mock flusher that reports known bytes ────────────────────

    struct MockFlusher {
        /// Bytes to report per flush call.
        bytes_per_flush: u64,
        /// Files to report per flush call.
        files_per_flush: u32,
    }

    impl SnapshotFlusher for MockFlusher {
        fn flush_snapshots(
            &self,
            _snapshots: &[MarketSnapshot],
            _output_dir: &str,
        ) -> Result<FlushReport, aetelier_types::errors::PersistError> {
            Ok(FlushReport::new(self.bytes_per_flush, self.files_per_flush))
        }
    }

    /// Construct a minimal `ExchangeEvent` for testing.
    fn dummy_exchange_event() -> crate::sources::ExchangeEvent {
        use crate::sources::bybit::events::BybitWssEvent;
        use crate::sources::bybit::responses::BybitTradeData;
        crate::sources::ExchangeEvent::Bybit(BybitWssEvent::TradeData(vec![
            BybitTradeData {
                trade_ts: 1_700_000_000_000,
                symbol: "BTCUSDT".into(),
                side: "Buy".into(),
                amount: "0.001".into(),
                price: "42000.0".into(),
                direction: Some("PlusTick".into()),
                trade_id: "test-001".into(),
                block_trade: false,
                rpi_trade: false,
                sequence: 1,
            },
        ]))
    }

    /// Helper to create a minimal MarketSnapshot for testing.
    fn dummy_snapshot() -> MarketSnapshot {
        MarketSnapshot {
            ts_us: 1_700_000_000_000_000_000,
            orderbook: None,
            trades: vec![],
            liquidations: vec![],
            funding_rate: vec![],
            open_interest: vec![],
            funding_settlements: vec![],
        }
    }

    fn channel_registry(topics: &[&str], capacity: usize) -> TopicRegistry {
        TopicRegistry::with_topics(topics, capacity)
    }

    #[tokio::test]
    async fn snapshot_channel_round_trips_the_synchronized_stream() {
        let snaps = SnapshotChannel::new(16);
        let sink =
            ChannelSink::with_snapshot_channel(channel_registry(&[], 8), snaps.clone());
        let mut rx = snaps.subscribe();

        sink.emit_snapshot(&dummy_snapshot()).unwrap();
        let got = rx.recv().await.unwrap();
        assert_eq!(got.ts_us, dummy_snapshot().ts_us);
        assert_eq!(sink.status().events_emitted, 1);
    }

    #[test]
    fn snapshot_emit_without_subscribers_is_ok_and_uncounted() {
        let snaps = SnapshotChannel::new(16);
        let sink =
            ChannelSink::with_snapshot_channel(channel_registry(&[], 8), snaps.clone());
        sink.emit_snapshot(&dummy_snapshot()).unwrap();
        assert_eq!(sink.status().events_emitted, 0);
        assert_eq!(snaps.len(), 0);
    }

    #[tokio::test]
    async fn slow_snapshot_subscriber_observes_quantified_lag() {
        let snaps = SnapshotChannel::new(4);
        let mut rx = snaps.subscribe();
        for i in 0..6u64 {
            let mut s = dummy_snapshot();
            s.ts_us = i;
            snaps.publish(std::sync::Arc::new(s)).unwrap();
        }
        match rx.recv().await {
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                assert_eq!(n, 2, "exactly the overwritten snapshots are reported");
            }
            other => panic!("expected Lagged, got {other:?}"),
        }
        assert_eq!(rx.recv().await.unwrap().ts_us, 2);
    }

    #[tokio::test]
    async fn external_snapshot_handle_reaches_through_build_sinks() {
        let snaps = SnapshotChannel::new(8);
        let set = build_sinks(
            &[OutputSinkConfig::Channel],
            Some(channel_registry(&[], 8)),
            Some(snaps.clone()),
            None,
            None,
            None,
            DeclaredSetAlias::all(),
        )
        .unwrap();
        let mut rx = snaps.subscribe();
        set.emit_snapshot(&dummy_snapshot()).unwrap();
        assert_eq!(rx.recv().await.unwrap().ts_us, dummy_snapshot().ts_us);
    }

    #[test]
    fn parquet_sink_without_flusher_fails_loudly() {
        // A declared Parquet output with no flusher used to be silently dropped
        // (data loss). It must now be a hard construction error.
        let result = build_sinks(
            &[OutputSinkConfig::Parquet {
                dir: "/tmp/does-not-matter".into(),
            }],
            None,
            None,
            None,
            None,
            None,
            DeclaredSetAlias::all(),
        );
        let err = match result {
            Ok(_) => panic!(
                "a Parquet sink with no flusher must fail loudly, not drop silently"
            ),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("no SnapshotFlusher"),
            "error should explain the missing flusher, got: {err}"
        );
    }

    #[test]
    fn backpressure_keys_on_the_worst_topic_not_the_aggregate() {
        let registry = channel_registry(&["hot", "cold"], 10);
        let hot_rx = registry.subscribe("hot").unwrap();
        let sink = ChannelSink::new(registry);
        for _ in 0..9 {
            sink.emit_raw("hot", &dummy_exchange_event(), 0).unwrap();
        }
        // Aggregate fill = 9/20 (45%) — the old check would report Streaming;
        // the hot topic alone is at 90%.
        assert_eq!(sink.status().state, SinkState::Backpressured);
        drop(hot_rx);
    }

    /// Mock flusher that always fails, to exercise the retain-on-error path.
    struct FailingFlusher;
    impl SnapshotFlusher for FailingFlusher {
        fn flush_snapshots(
            &self,
            _snapshots: &[MarketSnapshot],
            _output_dir: &str,
        ) -> Result<FlushReport, aetelier_types::errors::PersistError> {
            Err(aetelier_types::errors::PersistError::Parse(
                "simulated write failure".to_string(),
            ))
        }
    }

    #[test]
    fn flush_failure_retains_buffer_and_counts() {
        let sink = BufferedSink::new("/tmp/test".into(), Box::new(FailingFlusher));
        sink.emit_snapshot(&dummy_snapshot()).unwrap();
        sink.emit_snapshot(&dummy_snapshot()).unwrap();
        // A failed write must surface as Err (not a silently-flushed batch) and
        // bump the counter.
        assert!(sink.flush().is_err());
        assert_eq!(sink.total_flush_failures(), 1);
        // The buffer is retained: a second flush fails again (had it cleared, an
        // empty buffer would early-return Ok and leave the counter at 1).
        assert!(sink.flush().is_err());
        assert_eq!(sink.total_flush_failures(), 2);
    }

    // ── Gap 1: SinkStatus default impl tests ────────────────────────────

    #[test]
    fn test_default_status_returns_idle_zero_events() {
        let sink = NoopSink;
        let status = sink.status();
        assert_eq!(status.name, "noop");
        assert_eq!(status.state, SinkState::Idle);
        assert_eq!(status.events_emitted, 0);
        assert!(status.bytes_written.is_none());
        assert!(status.queue_depth.is_none());
        assert!(status.queue_capacity.is_none());
        assert!(status.lag_events.is_none());
    }

    #[test]
    fn test_sink_status_new_helper() {
        let status = SinkStatus::new("test", SinkState::Streaming, 42);
        assert_eq!(status.name, "test");
        assert_eq!(status.state, SinkState::Streaming);
        assert_eq!(status.events_emitted, 42);
        assert!(status.bytes_written.is_none());
    }

    // ── Gap 2: TerminalSink status tests ────────────────────────────────

    #[test]
    fn test_terminal_sink_starts_idle() {
        let sink = TerminalSink::new();
        let status = sink.status();
        assert_eq!(status.state, SinkState::Idle);
        assert_eq!(status.events_emitted, 0);
    }

    #[test]
    fn test_terminal_sink_counts_events() {
        let sink = TerminalSink::new();
        let snap = dummy_snapshot();

        sink.emit_snapshot(&snap).unwrap();
        sink.emit_snapshot(&snap).unwrap();
        sink.emit_snapshot(&snap).unwrap();

        let status = sink.status();
        assert_eq!(status.state, SinkState::Streaming);
        assert_eq!(status.events_emitted, 3);
    }

    // ── Gap 3: BufferedSink bytes tracking tests ────────────────────────

    #[test]
    fn test_buffered_sink_starts_idle_zero_bytes() {
        let flusher = MockFlusher {
            bytes_per_flush: 1024,
            files_per_flush: 1,
        };
        let sink = BufferedSink::new("/tmp/test".into(), Box::new(flusher));

        let status = sink.status();
        assert_eq!(status.state, SinkState::Idle);
        assert_eq!(status.events_emitted, 0);
        assert_eq!(status.bytes_written, Some(0));
        assert_eq!(status.queue_depth, Some(0));
    }

    #[test]
    fn test_buffered_sink_tracks_bytes_after_flush() {
        let flusher = MockFlusher {
            bytes_per_flush: 4096,
            files_per_flush: 2,
        };
        let sink = BufferedSink::new("/tmp/test".into(), Box::new(flusher));

        // Emit 3 snapshots.
        for _ in 0..3 {
            sink.emit_snapshot(&dummy_snapshot()).unwrap();
        }

        // Before flush: buffer has items, no bytes written yet.
        let status = sink.status();
        assert_eq!(status.state, SinkState::Writing); // items in buffer
        assert_eq!(status.events_emitted, 3);
        assert_eq!(status.bytes_written, Some(0));
        assert_eq!(status.queue_depth, Some(3));

        // Flush.
        sink.flush().unwrap();

        // After flush: bytes accumulated, buffer empty.
        let status = sink.status();
        assert_eq!(status.state, SinkState::Streaming); // events emitted, buffer empty
        assert_eq!(status.events_emitted, 3);
        assert_eq!(status.bytes_written, Some(4096));
        assert_eq!(status.queue_depth, Some(0));
        assert_eq!(sink.total_bytes_written(), 4096);
        assert_eq!(sink.total_files_written(), 2);
    }

    #[test]
    fn test_buffered_sink_accumulates_bytes_across_flushes() {
        let flusher = MockFlusher {
            bytes_per_flush: 1000,
            files_per_flush: 1,
        };
        let sink = BufferedSink::new("/tmp/test".into(), Box::new(flusher));

        // First batch + flush.
        sink.emit_snapshot(&dummy_snapshot()).unwrap();
        sink.flush().unwrap();
        assert_eq!(sink.total_bytes_written(), 1000);

        // Second batch + flush.
        sink.emit_snapshot(&dummy_snapshot()).unwrap();
        sink.emit_snapshot(&dummy_snapshot()).unwrap();
        sink.flush().unwrap();
        assert_eq!(sink.total_bytes_written(), 2000);
        assert_eq!(sink.total_files_written(), 2);

        // Status reflects cumulative totals.
        let status = sink.status();
        assert_eq!(status.events_emitted, 3);
        assert_eq!(status.bytes_written, Some(2000));
    }

    #[test]
    fn test_buffered_sink_empty_flush_noop() {
        let flusher = MockFlusher {
            bytes_per_flush: 9999,
            files_per_flush: 5,
        };
        let sink = BufferedSink::new("/tmp/test".into(), Box::new(flusher));

        // Flush with empty buffer — should not call flusher.
        sink.flush().unwrap();
        assert_eq!(sink.total_bytes_written(), 0);
        assert_eq!(sink.total_files_written(), 0);
    }

    // ── OutputSinkSet statuses aggregation ───────────────────────────────

    #[test]
    fn test_sink_set_statuses() {
        let mut set = OutputSinkSet::new();
        set.push(Box::new(TerminalSink::new()));
        set.push(Box::new(NoopSink));

        let statuses = set.statuses();
        assert_eq!(statuses.len(), 2);
        assert_eq!(statuses[0].name, "terminal");
        assert_eq!(statuses[1].name, "noop");
    }

    // ── DomainChannelSink (framework path) ───────────────────────────────

    fn domain_datatypes()
    -> aetelier_types::config::markets::market_config::DataTypesSection {
        use aetelier_types::config::markets::market_config::{
            DataTypesSection, FeedToggle, OrderbookConfig,
        };
        DataTypesSection {
            orderbook: OrderbookConfig {
                enabled: true,
                depth: 50,
            },
            trades: FeedToggle { enabled: true },
            liquidations: FeedToggle { enabled: false },
            funding_rates: FeedToggle { enabled: false },
            open_interest: FeedToggle { enabled: false },
        }
    }

    #[test]
    fn domain_channel_sink_emits_domain_to_subscriber() {
        let reg = DomainTopicRegistry::from_config("BTCUSDT", &domain_datatypes(), 64);
        let mut rx = reg.subscribe("trade.all.BTCUSDT").unwrap();
        let sink = DomainChannelSink::new(reg, "bybit");

        let ev = DomainEvent::Trade {
            trade: aetelier_types::trades::Trade::random(),
            sequence: None,
        };
        sink.emit_domain("trade.all.BTCUSDT", &ev, 99).unwrap();

        let got = rx
            .try_recv()
            .expect("subscriber should receive the domain event");
        assert_eq!(got.topic, "trade.all.BTCUSDT");
        assert_eq!(got.exchange, "bybit");
        assert_eq!(got.received_at_us, 99);
        assert!(matches!(sink.status().state, SinkState::Streaming));
    }

    #[test]
    fn default_emit_domain_is_noop() {
        // A sink that doesn't override emit_domain (TerminalSink) must accept it.
        let ev = DomainEvent::Trade {
            trade: aetelier_types::trades::Trade::random(),
            sequence: None,
        };
        TerminalSink::new()
            .emit_domain("trade.all.BTCUSDT", &ev, 0)
            .unwrap();
    }
}
