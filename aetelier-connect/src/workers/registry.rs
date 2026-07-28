//! Worker registry for fleet-level state aggregation and control.
//!
//! The [`WorkerRegistry`] is the single source of truth for "which workers
//! exist, what state are they in, and how do I talk to them."  It backs the
//! Data Collector Workers sidebar in the dashboard.
//!
//! # Architecture
//!
//! ```text
//!                      ┌──────────────┐
//!                      │   Registry   │
//!                      │ DashMap<id,  │
//!                      │  WorkerHandle│
//!                      └──────┬───────┘
//!                             │
//!            ┌────────────────┼────────────────┐
//!            ▼                ▼                 ▼
//!     ┌─────────────┐ ┌─────────────┐   ┌─────────────┐
//!     │ WorkerHandle│ │ WorkerHandle│   │ WorkerHandle│
//!     │  cmd_tx ────│ │  cmd_tx ────│   │  cmd_tx ────│
//!     │  status ◄───│ │  status ◄───│   │  status ◄───│
//!     └─────────────┘ └─────────────┘   └─────────────┘
//!            │                │                 │
//!            ▼                ▼                 ▼
//!        DataWorker      MarketWorker      DataWorker
//! ```
//!
//! Each worker holds the receiving end of a command channel and periodically
//! publishes its [`WorkerStatus`] through a `watch` channel.  The registry
//! owns the sending halves and the `watch` receivers.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};

use aetelier_types::WorkerId;
use aetelier_types::exchanges::{Exchange, MarketType};
use aetelier_types::synchronizers::WorkerMode;
use aetelier_types::trading_pair::TradingPair;

use crate::clients::connection_state::ConnectionState;

// ─────────────────────────────────────────────────────────────────────────────
// WorkerCommand
// ─────────────────────────────────────────────────────────────────────────────

/// Commands that can be sent to a running worker via the registry.
///
/// The worker's event loop checks for incoming commands between event
/// processing iterations.  Commands are non-blocking: the sender fires
/// and the worker acts on it at the next opportunity.
#[derive(Debug, Clone)]
pub enum WorkerCommand {
    /// Pause event forwarding to sinks.  The WebSocket connection remains
    /// open but events are discarded.
    Pause,
    /// Resume event forwarding after a pause.
    Resume,
    /// Initiate graceful shutdown: flush sinks, close connection, return
    /// the final [`DataWorkerReport`](super::DataWorkerReport) /
    /// [`MarketWorkerReport`](super::MarketWorkerReport).
    Stop,
    /// Tear down and re-create the worker with the same configuration.
    /// Equivalent to Stop followed by a fresh spawn.
    Restart,
}

// ─────────────────────────────────────────────────────────────────────────────
// WorkerStatus
// ─────────────────────────────────────────────────────────────────────────────

/// Snapshot of a worker's current state, designed to be rendered directly
/// by the dashboard sidebar.
///
/// Published by the worker through a `watch` channel on every state
/// change and periodically (e.g. every 500 ms) for throughput updates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerStatus {
    /// Unique worker identifier.
    pub id: WorkerId,
    /// Exchange this worker is connected to.
    pub exchange: Exchange,
    /// Canonical trading pair.
    pub pair: TradingPair,
    /// Instrument market type.
    pub market_type: MarketType,
    /// Processing mode (Raw or Clock-driven with parameters).
    pub mode: WorkerMode,
    /// Current connection lifecycle state.
    pub connection_state: ConnectionState,
    /// Messages received per second (exponentially weighted moving average).
    pub messages_per_sec: f64,
    /// Total events received since the worker started.
    pub total_events: u64,
    /// Number of reconnection attempts since the worker started.
    pub reconnect_count: u32,
    /// Which output sinks are configured.
    pub sinks: Vec<String>,
    /// Enabled datatype feeds (e.g. `["orderbook", "trades"]`).
    #[serde(default)]
    pub datatypes: Vec<String>,
    /// Per-feed lifecycle snapshots (framework ingest path; legacy paths
    /// leave it empty).
    #[serde(default)]
    pub feeds: Vec<crate::framework::feed::FeedSnapshot>,
    /// Wall-clock seconds since the worker was spawned.
    pub uptime_secs: f64,
    /// Feed latency in microseconds (EWMA): local receipt time minus the
    /// exchange event timestamp. `Some` on the framework ingest path (which
    /// measures it per timestamped event); `None` on the legacy path, which
    /// does not measure it — never a fabricated zero.
    #[serde(default)]
    pub ws_latency_us: Option<f64>,
    /// Transport-integrity counters (msgs, decode errors, dropped frames,
    /// gaps, resyncs, checksum failures). Populated by the framework ingest
    /// path; legacy paths leave the default zeros.
    #[serde(default)]
    pub source_metrics: crate::framework::budget::SourceMetricsSnapshot,
}

impl WorkerStatus {
    /// Convenience: is this worker actively streaming data?
    pub fn is_live(&self) -> bool {
        self.connection_state.is_live()
    }

    /// Convenience: is this worker in an error state?
    pub fn is_error(&self) -> bool {
        self.connection_state.is_error()
    }

    /// Convenience: is this worker paused?
    pub fn is_paused(&self) -> bool {
        self.connection_state.is_paused()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// WorkerHandle
// ─────────────────────────────────────────────────────────────────────────────

/// Control surface for a single registered worker.
///
/// The registry holds one `WorkerHandle` per active worker.  It provides:
///
/// - A command channel (`cmd_tx`) to send [`WorkerCommand`]s.
/// - A status watch (`status_rx`) to read the latest [`WorkerStatus`].
pub struct WorkerHandle {
    /// Send commands to the worker's event loop.
    cmd_tx: mpsc::Sender<WorkerCommand>,
    /// Latest worker status, updated by the worker.
    status_rx: watch::Receiver<WorkerStatus>,
}

impl WorkerHandle {
    /// Create a new handle from the channel halves.
    pub fn new(
        cmd_tx: mpsc::Sender<WorkerCommand>,
        status_rx: watch::Receiver<WorkerStatus>,
    ) -> Self {
        Self { cmd_tx, status_rx }
    }

    /// Read the most recent status snapshot.
    pub fn status(&self) -> WorkerStatus {
        self.status_rx.borrow().clone()
    }

    /// Send a command to the worker.
    ///
    /// Returns `Err` only if the worker's event loop has exited (channel
    /// closed).
    pub async fn send_command(
        &self,
        cmd: WorkerCommand,
    ) -> Result<(), mpsc::error::SendError<WorkerCommand>> {
        self.cmd_tx.send(cmd).await
    }

    /// Non-blocking command send (best-effort).
    pub fn try_send_command(
        &self,
        cmd: WorkerCommand,
    ) -> Result<(), mpsc::error::TrySendError<WorkerCommand>> {
        self.cmd_tx.try_send(cmd)
    }

    /// Subscribe to status changes.
    ///
    /// Returns a clone of the watch receiver; the caller can `.changed().await`
    /// to be notified of updates.
    pub fn subscribe_status(&self) -> watch::Receiver<WorkerStatus> {
        self.status_rx.clone()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// WorkerChannels — returned to the worker at registration time
// ─────────────────────────────────────────────────────────────────────────────

/// Channel halves that the worker keeps to receive commands and publish status.
///
/// Returned by [`WorkerRegistry::register`].
pub struct WorkerChannels {
    /// Receive commands from the registry / dashboard.
    pub cmd_rx: mpsc::Receiver<WorkerCommand>,
    /// Publish status updates visible to the registry.
    pub status_tx: watch::Sender<WorkerStatus>,
}

// ─────────────────────────────────────────────────────────────────────────────
// WorkerRegistry
// ─────────────────────────────────────────────────────────────────────────────

/// Central registry of all active workers.
///
/// Thread-safe (wrapped in `Arc` internally).  Clone is cheap — all clones
/// share the same underlying map.
#[derive(Clone)]
pub struct WorkerRegistry {
    inner: Arc<std::sync::RwLock<HashMap<WorkerId, WorkerHandle>>>,
}

impl WorkerRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(std::sync::RwLock::new(HashMap::new())),
        }
    }

    /// Register a new worker, returning the channels it should use.
    ///
    /// The `initial_status` is the status snapshot at construction time
    /// (before the worker has connected).  The worker should update it
    /// via `status_tx.send(...)` as state changes.
    ///
    /// `cmd_buffer` controls how many commands can be queued before the
    /// sender blocks (default: 8).
    pub fn register(
        &self,
        initial_status: WorkerStatus,
        cmd_buffer: usize,
    ) -> WorkerChannels {
        let id = initial_status.id;

        let (cmd_tx, cmd_rx) = mpsc::channel(cmd_buffer);
        let (status_tx, status_rx) = watch::channel(initial_status);

        let handle = WorkerHandle::new(cmd_tx, status_rx);

        {
            let mut map = self.inner.write().expect("registry lock poisoned");
            map.insert(id, handle);
        }

        WorkerChannels { cmd_rx, status_tx }
    }

    /// Remove a worker from the registry (e.g. after it exits).
    pub fn deregister(&self, id: &WorkerId) {
        let mut map = self.inner.write().expect("registry lock poisoned");
        map.remove(id);
    }

    /// Number of registered workers.
    pub fn len(&self) -> usize {
        let map = self.inner.read().expect("registry lock poisoned");
        map.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the latest status of all workers.
    pub fn all_statuses(&self) -> Vec<WorkerStatus> {
        let map = self.inner.read().expect("registry lock poisoned");
        map.values().map(|h| h.status()).collect()
    }

    /// Get the status of a specific worker.
    pub fn status(&self, id: &WorkerId) -> Option<WorkerStatus> {
        let map = self.inner.read().expect("registry lock poisoned");
        map.get(id).map(|h| h.status())
    }

    /// Send a command to a specific worker.
    pub async fn send_command(
        &self,
        id: &WorkerId,
        cmd: WorkerCommand,
    ) -> anyhow::Result<()> {
        let handle = {
            let map = self.inner.read().expect("registry lock poisoned");
            map.get(id)
                .ok_or_else(|| anyhow::anyhow!("worker {} not found", id))?
                .cmd_tx
                .clone()
        };
        handle
            .send(cmd)
            .await
            .map_err(|_| anyhow::anyhow!("worker {} channel closed", id))
    }

    /// List workers filtered by predicate on their status.
    pub fn filter<F>(&self, predicate: F) -> Vec<WorkerStatus>
    where
        F: Fn(&WorkerStatus) -> bool,
    {
        let map = self.inner.read().expect("registry lock poisoned");
        map.values()
            .map(|h| h.status())
            .filter(|s| predicate(s))
            .collect()
    }

    /// All live (streaming) workers.
    pub fn live_workers(&self) -> Vec<WorkerStatus> {
        self.filter(|s| s.is_live())
    }

    /// All workers in an error state.
    pub fn error_workers(&self) -> Vec<WorkerStatus> {
        self.filter(|s| s.is_error())
    }

    /// All paused workers.
    pub fn paused_workers(&self) -> Vec<WorkerStatus> {
        self.filter(|s| s.is_paused())
    }

    /// Summary counts for the sidebar filter tabs.
    pub fn counts(&self) -> RegistryCounts {
        let map = self.inner.read().expect("registry lock poisoned");
        let mut counts = RegistryCounts::default();
        for handle in map.values() {
            let status = handle.status();
            counts.all += 1;
            if status.is_live() {
                counts.live += 1;
            }
            if status.is_error() {
                counts.error += 1;
            }
            if status.is_paused() {
                counts.paused += 1;
            }
        }
        counts
    }
}

impl Default for WorkerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// RegistryCounts
// ─────────────────────────────────────────────────────────────────────────────

/// Summary counts for the dashboard's filter tabs: ALL / LIVE / ERR / PAUSED.
#[derive(Debug, Clone, Default, Serialize)]
pub struct RegistryCounts {
    /// Total number of registered workers.
    pub all: usize,
    /// Workers in [`ConnectionState::Streaming`].
    pub live: usize,
    /// Workers in an error state (Disconnected, Reconnecting).
    pub error: usize,
    /// Workers in [`ConnectionState::Paused`].
    pub paused: usize,
}
