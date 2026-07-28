//! Worker-specific configuration types.
//!
//! This module provides lean, purpose-built configs for each worker type:
//!
//! - [`DataWorkerConfig`](crate::config::workers::data_worker_config::DataWorkerConfig) — raw ingestion, no synchronisation.
//! - [`MarketWorkerConfig`](crate::config::workers::market_worker_config::MarketWorkerConfig) — synchronised ingestion with grid alignment.
//!
//! Both share [`CommonWorkerFields`](crate::config::workers::common::CommonWorkerFields) for exchange, symbol, datatypes, and
//! operational tuning knobs.

pub mod common;
pub mod data_worker_config;
pub mod market_worker_config;

pub use common::{
    CommonWorkerFields, ManifestMetadata, OutputSinkConfig, ReconnectSection,
};
pub use data_worker_config::{DataWorkerConfig, DataWorkerManifest, SessionSection};
pub use market_worker_config::{MarketWorkerConfig, MarketWorkerManifest, SyncSection};
