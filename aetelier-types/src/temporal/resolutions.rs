//! Temporal validation utilities for timestamp sequences.
//!
//! Orderbook timestamps are in nanoseconds, trade timestamps in milliseconds,
//! and other datasets might be in microseconds or seconds.
//! This module provides a canonical representation (nanoseconds) and conversion
//! utilities to normalize heterogeneous timestamp sources.

use serde::{Deserialize, Serialize};

/// Nanoseconds per microsecond.
pub const NS_PER_US: u64 = 1_000;

/// Nanoseconds per millisecond.
pub const NS_PER_MS: u64 = 1_000_000;

/// Nanoseconds per second.
pub const NS_PER_S: u64 = 1_000_000_000;

/// Supported temporal resolutions for timestamp data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeResolution {
    /// Nanoseconds (1 ns = 1e-9 s).
    Nanoseconds,
    /// Microseconds (1 µs = 1e-6 s).
    Microseconds,
    /// Milliseconds (1 ms = 1e-3 s).
    Milliseconds,
    /// Seconds (1 s).
    Seconds,
}

impl TimeResolution {
    /// Returns the number of nanoseconds per unit of this resolution.
    pub fn nanos_per_unit(self) -> u64 {
        match self {
            Self::Nanoseconds => 1,
            Self::Microseconds => NS_PER_US,
            Self::Milliseconds => NS_PER_MS,
            Self::Seconds => NS_PER_S,
        }
    }
}

/// Convert a timestamp from the given resolution to nanoseconds.
///
/// # Examples
///
/// ```
/// use aetelier_types::temporal::{TimeResolution, to_nanos};
///
/// let ts_ms: u64 = 1_672_304_484_932;
/// let ts_us = to_nanos(ts_ms, TimeResolution::Milliseconds);
/// assert_eq!(ts_us, ts_ms * 1_000_000);
/// ```
pub fn to_nanos(ts: u64, resolution: TimeResolution) -> u64 {
    ts.saturating_mul(resolution.nanos_per_unit())
}

/// Convert a nanosecond timestamp to the target resolution as `f64`.
///
/// Returns a floating-point value to preserve sub-unit precision
/// (e.g. fractional microsecond or milliseconds).
///
/// # Examples
///
/// ```
/// use aetelier_types::temporal::{TimeResolution, from_nanos};
///
/// let ts_us: u64 = 1_500_000; // 1.5 ms
/// let ts_ms = from_nanos(ts_us, TimeResolution::Milliseconds);
/// assert!((ts_ms - 1.5).abs() < 1e-10);
/// ```
pub fn from_nanos(ts_us: u64, resolution: TimeResolution) -> f64 {
    ts_us as f64 / resolution.nanos_per_unit() as f64
}

/// Convert a microsecond timestamp to the target resolution as `f64`.
///
/// The microsecond counterpart of [`from_nanos`]: the platform standard is
/// epoch microseconds, so compute layers convert from µs, not ns.
///
/// # Examples
///
/// ```
/// use aetelier_types::temporal::{TimeResolution, from_micros};
///
/// let ts_us: u64 = 1_500; // 1.5 ms
/// let ts_ms = from_micros(ts_us, TimeResolution::Milliseconds);
/// assert!((ts_ms - 1.5).abs() < 1e-10);
/// ```
pub fn from_micros(ts_us: u64, resolution: TimeResolution) -> f64 {
    ts_us as f64 * NS_PER_US as f64 / resolution.nanos_per_unit() as f64
}
