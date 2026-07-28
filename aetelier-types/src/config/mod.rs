//! Configuration types for market data pipelines.
//!
//! Provides [`config::markets::market_config::MarketSnapshotConfig`] for configuring per-symbol processing
//! and [`config::markets::WorkerManifest`] for worker-level pipeline configuration.

/// Market configuration types.
pub mod markets;

pub use markets::MarketSnapshotConfig;
pub use markets::WorkerManifest;
