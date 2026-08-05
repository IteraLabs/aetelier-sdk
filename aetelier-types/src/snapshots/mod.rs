//! Market snapshot types and aggregation.
//!
//! Provides [`MarketSnapshot`] for joining multi-source market data at
//! grid-aligned periods, and [`FlushResult`] for persistence tracking.

/// Aggregated market statistics module.
pub mod aggregate;
/// Market snapshot joining all data sources.
pub mod market;

pub use aggregate::MarketAggregate;
pub use market::MarketSnapshot;

/// Result of flushing synchronized market data to persistent storage.
#[derive(Debug, Clone, Default)]
pub struct FlushResult {
    /// Path to the written orderbook file, if persisted.
    pub orderbook_path: Option<std::path::PathBuf>,
    /// Path to the written trades file, if persisted.
    pub trades_path: Option<std::path::PathBuf>,
    /// Path to the written liquidations file, if persisted.
    pub liquidations_path: Option<std::path::PathBuf>,
    /// Path to the written funding rates file, if persisted.
    pub funding_path: Option<std::path::PathBuf>,
    /// Path to the written open interest file, if persisted.
    pub open_interest_path: Option<std::path::PathBuf>,
    pub funding_settlement_path: Option<std::path::PathBuf>,
    /// Count of snapshots flushed in this operation.
    pub snapshot_count: usize,
}
