//! # aetelier-io
//!
//! Read and write Parquet, CSV, and JSON for **aetelier-sdk** market data types.
//!
//! Depends on [`aetelier_types`] for the canonical data model.
//! Parquet support requires the `parquet` cargo feature.
//!
//! ## Feature flags
//!
//! | Flag | Effect |
//! |------|--------|
//! | `parquet` | Enables Apache Parquet I/O (adds `arrow` + `parquet` deps). |
//! | `torch` | Enables `tch`-based tensor conversion in the `datasets` module. |
//! | `connect` | Enables the `FlushToParquet` extension trait for `MarketSynchronizer`. |

#![allow(clippy::large_enum_variant)]
#![allow(clippy::too_many_arguments)]

// ── Parquet / Arrow → PersistError bridge ──────────────────────────────
//
// `PersistError` lives in `aetelier_types` which has no parquet/arrow deps.
// The orphan rule prevents `impl From<ArrowError> for PersistError` here,
// so we provide crate-local conversion helpers used by all parquet modules.

#[cfg(feature = "parquet")]
pub(crate) mod parquet_err {
    use aetelier_types::errors::PersistError;

    #[inline]
    pub fn from_arrow(e: arrow::error::ArrowError) -> PersistError {
        PersistError::Parse(e.to_string())
    }

    #[inline]
    pub fn from_parquet(e: parquet::errors::ParquetError) -> PersistError {
        PersistError::Parse(e.to_string())
    }
}

/// Orderbook I/O (Parquet, CSV, JSON, terminal)
pub(crate) mod naming;
pub mod orderbooks;

/// Trade I/O (Parquet)
pub mod trades;

/// Funding rate I/O (Parquet)
pub mod funding;

/// Liquidation I/O (Parquet)
pub mod liquidations;

/// Open interest I/O (Parquet)
pub mod open_interest;

/// Snapshot aggregate I/O (Parquet)
pub mod snapshots;

/// FlushToParquet extension trait (requires `connect` + `parquet` features)
#[cfg(all(feature = "connect", feature = "parquet"))]
pub mod flush;

/// Batch rehydration of persisted trade parquets from venue REST
/// (requires `connect` + `parquet`).
#[cfg(all(feature = "connect", feature = "parquet"))]
pub mod rehydrate;

#[cfg(all(feature = "connect", feature = "parquet"))]
pub use flush::{FlushAggregateToParquet, FlushObSyncToParquet, FlushToParquet};

/// Concrete [`SnapshotFlusher`](aetelier_connect::workers::SnapshotFlusher) for Parquet (requires `connect` + `parquet` features)
#[cfg(all(feature = "connect", feature = "parquet"))]
pub mod sink;

#[cfg(all(feature = "connect", feature = "parquet"))]
pub use sink::ParquetSnapshotFlusher;

/// README code blocks compile as doc tests, so the README cannot drift from
/// the API. Invisible in rustdoc; exercised by `cargo test --doc`.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
pub struct ReadmeDoctests;
