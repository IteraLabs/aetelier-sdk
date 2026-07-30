//! Multi-worker collection orchestration.
//!
//! [`DataWorker`] is the core ingestion primitive — a single-symbol,
//! single-exchange collection loop with reconnection, gap detection,
//! and stale-connection monitoring.  It publishes raw, un-normalised
//! events to topic-keyed broadcast channels — no synchronization, no
//! Parquet writes, no preprocessing.

/// Lean ingestion-only data worker (no preprocessing).
pub mod data_worker;
/// Per-topic ingestion gap detection.
pub mod gap_detector;
pub mod gap_ledger;
/// Shared ingestion engine (reconnection, classification, health).
pub mod ingestion_core;
/// Synchronised market data worker.
pub mod market_worker;
/// Pluggable output sinks for worker event delivery.
pub mod output;
/// Composable event pipeline between ingestion and workers.
pub mod pipeline;
/// Worker registry for fleet-level state aggregation and control.
pub mod registry;
/// Topic-keyed broadcast channels for raw event publishing.
pub mod topic_publisher;

pub use aetelier_types::WorkerId;
pub use data_worker::{DataWorker, DataWorkerReport};
pub use gap_detector::{GapDetector, GapDetectorSet, GapStats};
pub use ingestion_core::{IngestionCore, IngestionReport};
pub use market_worker::{MarketWorker, MarketWorkerReport};
pub use output::{
    BufferedSink, BufferedSinkFlushCallback, BufferedSinkFlushEvent, FlushReport,
    OutputSink, OutputSinkSet, SinkState, SinkStatus, SnapshotFlusher,
    TerminalEventCallback, TerminalSink, TerminalSinkRawEvent, build_sinks,
};
pub use pipeline::{EventPipeline, PassthroughPipeline, build_pipeline};
pub use registry::{
    RegistryCounts, WorkerChannels, WorkerCommand, WorkerHandle, WorkerRegistry,
    WorkerStatus,
};
pub use topic_publisher::{PublishError, TopicMessage, TopicPublisher, TopicRegistry};
