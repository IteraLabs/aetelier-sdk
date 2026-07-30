//! Orderbook state persistence types
//!
//! Supports orderbook snapshots in CSV, JSON, and Parquet formats.

use serde::{Deserialize, Serialize};

use crate::orders::OrderSide;
use crate::trading_pair::TradingPair;

#[cfg(feature = "std")]
use std::path::Path;

/// Output format for persisting orderbook data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    /// CSV format.
    Csv,
    /// JSON format.
    Json,
    /// Parquet columnar format.
    Parquet,
}

impl OutputFormat {
    /// Infer format from a file path's extension (requires `std` feature).
    #[cfg(feature = "std")]
    pub fn from_path(path: &Path) -> Option<Self> {
        Self::from_extension(path.extension()?.to_str()?)
    }

    /// Infer format from a bare extension string.
    ///
    /// Works in both native and WASM environments.
    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext {
            "csv" => Some(Self::Csv),
            "json" => Some(Self::Json),
            "parquet" | "pq" => Some(Self::Parquet),
            _ => None,
        }
    }
}

/// A single price level record for serialization.
#[derive(Debug, Clone, Serialize)]
pub struct PriceLevelRecord {
    /// Timestamp in Unix microseconds.
    pub timestamp_us: u64,
    /// Canonical trading pair.
    pub pair: TradingPair,
    /// Book side (`"bid"` or `"ask"`).
    pub side: OrderSide,
    /// Level rank (0 = best, 1 = second best, etc.).
    pub level: usize,
    /// Price as a string to preserve precision.
    pub price: String,
    /// Size as a string to preserve precision.
    pub size: String,
}

/// Full orderbook snapshot for JSON serialization.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OrderbookSnapshot {
    /// Snapshot timestamp in Unix microseconds.
    pub timestamp_us: u64,
    /// Canonical trading pair.
    pub pair: TradingPair,
    /// Last update ID for sequence tracking.
    pub update_id: u64,
    /// Sequence number.
    pub sequence: u64,
    /// Count of deltas applied since last snapshot.
    pub delta_count: u64,
    /// Number of bid levels in snapshot.
    pub bid_depth: usize,
    /// Number of ask levels in snapshot.
    pub ask_depth: usize,
    /// Mid-price value as string, if computable.
    pub mid_price: Option<String>,
    /// Bid-ask spread as string.
    pub spread: Option<String>,
    /// Spread in basis points.
    pub spread_bps: Option<String>,
    /// Volume imbalance ratio.
    pub volume_imbalance: Option<String>,
    /// Total bid-side volume.
    pub total_bid_volume: String,
    /// Total ask-side volume.
    pub total_ask_volume: String,
    /// Bid levels as [price, size] pairs.
    pub bids: Vec<[String; 2]>,
    /// Ask levels as [price, size] pairs.
    pub asks: Vec<[String; 2]>,
}
