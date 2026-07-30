//! # aetelier-connect
//!
//! Exchange connectivity, workers, and synchronizers for the
//! **aetelier-sdk** trading engine.
//!
//! Provides WebSocket clients for supported exchanges, data ingestion
//! workers, and time-synchronization primitives.  Depends on
//! [`aetelier_types`] for the canonical data model.

#![allow(clippy::large_enum_variant)]
#![allow(clippy::too_many_arguments)]

/// Error types for WebSocket and exchange operations
pub mod errors;

/// Communication clients (WebSocket, HTTP, RPC)
pub mod clients;

/// Worker-specific configuration
pub mod config;

/// Mappings for exchange data sources
pub mod sources;

/// Exchange-abstraction framework: per-venue adapters behind a shared
/// transport, normalization, reconstruction, and registry.
pub mod framework;

/// Multi-source time synchronizer
pub mod synchronizers;

/// Multi-worker collection orchestration
pub mod workers;

// ──── Convenience re-exports

pub use clients::{
    connection_manager::{ConnectionManager, ConnectionManagerConfig},
    connection_state::ConnectionState,
    disconnect::{DisconnectReason, classify_close_frame, classify_tungstenite_error},
    reconnect::{
        CircuitState, ConnectionHealth, HealthMonitor, ReconnectAction, ReconnectPolicy,
    },
    wss::{WssClient, WssClientBuilder, WssDecoder},
};
pub use config::workers::ManifestMetadata;
pub use errors::{ConnectError, ExchangeError};
// The legacy raw-ingestion surface (superseded by the framework engine). These
// re-exports carry deprecated items forward for existing users of the raw path;
// the allow keeps the crate's own build clean while use sites warn.
#[allow(deprecated)]
pub use sources::{
    ExchangeEvent,
    binance::client::BinanceWssClient,
    bybit::{client::BybitWssClient, responses::BybitLiquidationData},
    coinbase::client::CoinbaseWssClient,
    gateio::client::GateioWssClient,
    kraken::client::KrakenWssClient,
    okx::client::OkxWssClient,
};
pub use synchronizers::{
    ClockMode, EventSynchronizer, MarketSynchronizer, ReferenceEventType,
};

pub use workers::{
    BufferedSink, BufferedSinkFlushCallback, BufferedSinkFlushEvent, DataWorker,
    DataWorkerReport, EventPipeline, FlushReport, GapDetector, GapDetectorSet, GapStats,
    IngestionCore, IngestionReport, MarketWorker, MarketWorkerReport, OutputSink,
    OutputSinkSet, PassthroughPipeline, PublishError, RegistryCounts, SinkState,
    SinkStatus, SnapshotFlusher, TerminalEventCallback, TerminalSink,
    TerminalSinkRawEvent, TopicMessage, TopicPublisher, TopicRegistry, WorkerChannels,
    WorkerCommand, WorkerHandle, WorkerId, WorkerRegistry, WorkerStatus, build_pipeline,
    build_sinks,
};

/// README code blocks compile as doc tests, so the README cannot drift from
/// the API. Invisible in rustdoc; exercised by `cargo test --doc`.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
pub struct ReadmeDoctests;
