//! The platform timestamp standard: **UTC epoch microseconds**.
//!
//! Every epoch timestamp in every aetelier component is a count of
//! microseconds since the Unix epoch (UTC — no local time anywhere), carried
//! as `u64` in serialized structs whose field and column names end in `_us`,
//! and as [`TimestampUs`] in APIs so the compiler rejects mixed units.
//!
//! Why microseconds: `i64` microseconds span ±292k years (nanoseconds die in
//! 2262); `TIMESTAMP(MICROS, UTC)` is the most portable Arrow/Parquet logical
//! type (Spark/DuckDB/ClickHouse `DateTime64(6)` are first-class); venue event
//! time is millisecond-grained and WAN jitter is far above 1 µs, so
//! microseconds are headroom without fake precision.
//!
//! Carve-outs:
//! 1. Intervals and timeouts are `std::time::Duration` — a duration is not a
//!    timestamp, and `Duration` is already unit-safe by type.
//! 2. A capture layer may hold nanoseconds internally (e.g. hardware
//!    timestamping, the transport's RTT ping payload); it converts to
//!    microseconds at every boundary, wire, and persisted surface.
//! 3. External wire contracts (venue JSON, proto) keep their declared units;
//!    conversion happens exactly at the mapping boundary.

use serde::{Deserialize, Serialize};

/// A UTC epoch timestamp in microseconds — the platform standard unit.
///
/// Transparent over `u64`: serializes as a plain integer, zero-cost `Copy`.
/// Use this type in APIs and math so mixed units are unrepresentable; struct
/// fields on serialized types carry raw `u64` with `_us`-suffixed names.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct TimestampUs(pub u64);

impl TimestampUs {
    /// The current wall-clock time (UTC epoch microseconds).
    ///
    /// On native targets this reads `std::time::SystemTime`; on WASM with the
    /// `wasm` feature it converts `js_sys::Date::now()` (milliseconds) up to
    /// microseconds; without that feature it returns 0.
    pub fn now() -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        {
            TimestampUs(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_micros() as u64)
                    .unwrap_or(0),
            )
        }
        #[cfg(all(target_arch = "wasm32", feature = "wasm"))]
        {
            TimestampUs((js_sys::Date::now() as u64).saturating_mul(1_000))
        }
        #[cfg(all(target_arch = "wasm32", not(feature = "wasm")))]
        {
            TimestampUs(0)
        }
    }

    /// Wrap a raw UTC epoch microsecond count.
    pub fn from_micros(us: u64) -> Self {
        TimestampUs(us)
    }

    /// Convert UTC epoch milliseconds to the standard unit.
    pub fn from_millis(ms: u64) -> Self {
        TimestampUs(ms.saturating_mul(1_000))
    }

    /// Convert UTC epoch nanoseconds to the standard unit (rounds down).
    pub fn from_nanos(ns: u64) -> Self {
        TimestampUs(ns / 1_000)
    }

    /// Convert UTC epoch seconds to the standard unit.
    pub fn from_secs(s: u64) -> Self {
        TimestampUs(s.saturating_mul(1_000_000))
    }

    /// The raw microsecond count.
    pub fn as_micros(self) -> u64 {
        self.0
    }

    /// The timestamp truncated to milliseconds.
    pub fn as_millis(self) -> u64 {
        self.0 / 1_000
    }

    /// Microseconds elapsed from `earlier` to `self` (0 when clock skew makes
    /// it negative).
    pub fn saturating_since(self, earlier: TimestampUs) -> u64 {
        self.0.saturating_sub(earlier.0)
    }
}

impl std::fmt::Display for TimestampUs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}us", self.0)
    }
}

impl From<u64> for TimestampUs {
    fn from(us: u64) -> Self {
        TimestampUs(us)
    }
}

impl From<TimestampUs> for u64 {
    fn from(ts: TimestampUs) -> Self {
        ts.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversions_are_exact() {
        assert_eq!(
            TimestampUs::from_millis(1_700_000_000_000).as_micros(),
            1_700_000_000_000_000
        );
        assert_eq!(TimestampUs::from_nanos(1_500).as_micros(), 1);
        assert_eq!(TimestampUs::from_secs(2).as_micros(), 2_000_000);
        assert_eq!(TimestampUs::from_millis(5).as_millis(), 5);
    }

    #[test]
    fn saturating_since_never_underflows() {
        let a = TimestampUs(100);
        let b = TimestampUs(300);
        assert_eq!(b.saturating_since(a), 200);
        assert_eq!(a.saturating_since(b), 0);
    }
}
