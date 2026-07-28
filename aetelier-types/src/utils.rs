//! Utility functions for market data processing.
//!
//! Provides timestamp formatting, decimal conversion, and normalization
//! of exchange-specific side and symbol representations.

use chrono::{DateTime, SecondsFormat};
use rust_decimal::Decimal;

/// Format a UTC epoch microsecond timestamp as RFC 3339 with microsecond
/// precision (the platform timestamp standard).
pub fn format_ts(ts_us: u64) -> String {
    let dt = DateTime::from_timestamp_micros(ts_us as i64).unwrap_or_default();
    dt.to_rfc3339_opts(SecondsFormat::Micros, true)
}

/// Convert a Rust Decimal to f64, returning 0.0 on failure.
pub fn decimal_to_f64(d: Decimal) -> f64 {
    use std::str::FromStr;
    f64::from_str(&d.to_string()).unwrap_or(0.0)
}

/// Normalize a trade/liquidation side string to a [`crate::trades::TradeSide`] variant.
///
/// Handles exchange-specific formats:
/// - Bybit: `"Buy"` / `"Sell"`
/// - Coinbase: `"BUY"` / `"SELL"` (trades), `"bid"` / `"offer"` (L2)
/// - Kraken: `"buy"` / `"sell"` (already normalized)
///
/// Falls back to [`crate::trades::TradeSide::Buy`] for unrecognised input.
#[inline]
pub fn normalize_side(raw: &str) -> crate::trades::TradeSide {
    use crate::trades::TradeSide;
    match raw {
        "Buy" | "BUY" | "buy" | "bid" => TradeSide::Buy,
        "Sell" | "SELL" | "sell" | "offer" => TradeSide::Sell,
        other => other.parse::<TradeSide>().unwrap_or(TradeSide::Buy),
    }
}

/// Normalize a trading pair symbol to lowercase, separator-free format.
///
/// Handles exchange-specific formats:
/// - Bybit: `"BTCUSDT"` → `"btcusdt"`
/// - Coinbase: `"BTC-USD"` → `"btcusd"`
/// - Kraken: `"BTC/USD"` → `"btcusd"`
#[inline]
pub fn normalize_symbol(raw: &str) -> String {
    raw.replace(['-', '/', '_'], "").to_lowercase()
}
