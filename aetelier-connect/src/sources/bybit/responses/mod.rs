//! Bybit v5 WebSocket response types.
//!
//! Each sub-module maps to one Bybit topic family and contains:
//!
//! - An **envelope struct** (`*Response`) that mirrors the full JSON
//!   message including `topic`, `type`, `ts`, and `data`.
//! - A **data struct** (`*Data`) that holds the inner payload the
//!   decoder surfaces as a [`BybitWssEvent`](super::events::BybitWssEvent)
//!   variant.
//!
//! Field names use `#[serde(rename = "...")]` to match the single-letter
//! keys Bybit sends on the wire while exposing descriptive Rust names.

pub mod liquidations;
pub mod orderbooks;
pub mod tickers;
pub mod trades;

pub use {
    liquidations::{BybitLiquidationData, BybitLiquidationResponse},
    orderbooks::{BybitOrderbookData, BybitOrderbookResponse},
    tickers::{BybitTickerData, BybitTickerResponse},
    trades::{BybitTradeData, BybitTradeResponse},
};
