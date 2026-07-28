//! # aetelier-sdk
//!
//! The front door to the aetelier market-data engine: a curated set of the
//! types and workers a data application actually reaches for, re-exported by
//! name at the crate root so a single `use` line gets you moving.
//!
//! ```
//! use aetelier_sdk::{Trade, TradeSide, TradingPair};
//!
//! // The canonical, exchange-agnostic trade — built with validated fields.
//! let trade = Trade::builder()
//!     .source_trade_ts_us(1_700_000_000_000_000)
//!     .pair(TradingPair::new("BTC", "USDT"))
//!     .side(TradeSide::Buy)
//!     .amount(0.5)
//!     .price(42_000.0)
//!     .exchange("binance".into())
//!     .id("t-1".into())
//!     .build()
//!     .expect("all fields set");
//!
//! assert_eq!(trade.pair.to_canonical(), "BTC/USDT");
//! ```
//!
//! ## The recommended entry point
//!
//! For live collection, drive a [`MarketWorker`] from a TOML manifest: it
//! connects to a venue, reconstructs order books and trades through the
//! framework's decode/normalize path, aligns them on a grid via a
//! [`MarketSynchronizer`], and (with the `parquet` feature) persists them.
//! This is the same path the `md_worker` binary runs, exposed here as a
//! library so you can embed it.
//!
//! ## What is re-exported
//!
//! - **Market data model** — [`Trade`], [`Orderbook`], [`Level`],
//!   [`Liquidation`], [`FundingRate`], [`OpenInterest`], [`MarketSnapshot`],
//!   [`TradingPair`], [`Exchange`], and their companions.
//! - **Framework runtime** — [`MarketWorker`], [`DataWorker`],
//!   [`MarketSynchronizer`], [`EventSynchronizer`], [`ManifestMetadata`].
//! - **Persistence** (feature `parquet`) — `FlushToParquet`,
//!   `ParquetSnapshotFlusher` (links resolve when the feature is enabled).
//! - **Telemetry** — [`TelemetryConfig`], [`init_telemetry`].
//! - **Errors** — the unified [`AetelierError`].
//!
//! Power users who need the full, uncurated surface can reach every workspace
//! crate through its re-export: [`aetelier_types`], [`aetelier_connect`],
//! [`aetelier_io`], [`aetelier_telemetry`].
//!
//! ## Feature flags
//!
//! | Flag | Effect |
//! |------|--------|
//! | `parquet` | Parquet read/write plus the `FlushToParquet` persistence surface. |
//! | `connect` | The `FlushToParquet` trait against live worker output. |
//! | `torch`   | `tch` tensor conversion in `aetelier_io` (experimental). |

pub mod error;
pub use error::AetelierError;

// ── Market data model (aetelier-types) ──────────────────────────────────────
pub use aetelier_types::{
    Exchange, FundingRate, Level, Liquidation, MarketAggregate, MarketSnapshot,
    MarketType, NormalizedDelta, OpenInterest, Order, OrderSide, OrderType, Orderbook,
    OrderbookDelta, SubscriptionStatus, TimestampUs, Trade, TradeSide, TradingPair,
};

// ── Framework runtime (aetelier-connect) ────────────────────────────────────
pub use aetelier_connect::config::workers::ManifestMetadata;
pub use aetelier_connect::synchronizers::{
    ClockMode, EventSynchronizer, MarketSynchronizer,
};
pub use aetelier_connect::workers::{
    DataWorker, DataWorkerReport, MarketWorker, MarketWorkerReport,
};

// ── Persistence (aetelier-io; requires the `parquet` feature) ────────────────
#[cfg(feature = "parquet")]
pub use aetelier_io::{
    FlushAggregateToParquet, FlushObSyncToParquet, FlushToParquet, ParquetSnapshotFlusher,
};

// ── Telemetry (aetelier-telemetry) ──────────────────────────────────────────
pub use aetelier_telemetry::{TelemetryConfig, TelemetryGuard, init_telemetry};

// ── Whole-crate re-exports for power users ──────────────────────────────────
pub use aetelier_connect;
pub use aetelier_io;
pub use aetelier_telemetry;
pub use aetelier_types;

/// README code blocks compile as doc tests, so the README cannot drift from
/// the API. Invisible in rustdoc; exercised by `cargo test --doc`.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
pub struct ReadmeDoctests;
