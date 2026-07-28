//! # aetelier-types
//!
//! Shared types, error taxonomy, configs, and serialization for the
//! **aetelier-sdk** trading engine.
//!
//! This is the schema crate — it defines the canonical data model
//! (orderbooks, trades, funding rates, liquidations, open interest,
//! snapshots) without any I/O, networking, or async dependencies.

#![allow(clippy::large_enum_variant)]
#![allow(clippy::too_many_arguments)]

/// Configuration for market data pipelines
pub mod config;

/// Result and error handling
pub mod errors;

/// Exchanges and properties
pub mod exchanges;

/// Funding rate data for perpetual futures
pub mod funding;

/// Orders-Price-Volume levels for Orderbooks
pub mod levels;

/// Liquidations of positions in CEX
pub mod liquidations;

/// Open interest tracking for derivatives
pub mod open_interest;

/// Single thread Orderbook structure
pub mod orderbooks;

/// Implementation of orders
pub mod orders;

/// Multi-source market snapshot aggregation
pub mod snapshots;

/// Synchronization and clock modes
pub mod synchronizers;

/// Configurations and experiments
pub mod templates;

/// Temporal data treatments
pub mod temporal;

/// The platform timestamp standard: UTC epoch microseconds.
pub mod time;
pub use time::TimestampUs;

/// Public Trades
pub mod trades;

/// Subscription lifecycle types
pub mod subscriptions;

/// Canonical trading pair representation
pub mod trading_pair;

/// General utilities
pub mod utils;

/// Worker identity types
pub mod workers;

// Convenience re-exports

/// Re-export of error types.
pub use errors::{
    ConfigError, LevelError, LoaderError, OrderError, OrderbookError, PersistError,
    TemporalError,
};
/// Re-export of Exchange type.
pub use exchanges::Exchange;
/// Re-export of MarketType type.
pub use exchanges::MarketType;
/// Re-export of FundingRate type.
pub use funding::FundingRate;
/// Re-export of Level type.
pub use levels::Level;
/// Re-export of Liquidation type.
pub use liquidations::Liquidation;
/// Re-export of OpenInterest type.
pub use open_interest::OpenInterest;
/// Re-export of orderbook types.
pub use orderbooks::{NormalizedDelta, Orderbook, OrderbookDelta};
/// Re-export of order types.
pub use orders::{Order, OrderSide, OrderType};
/// Re-export of snapshot types.
pub use snapshots::{FlushResult, MarketAggregate, MarketSnapshot};
/// Re-export of SubscriptionStatus type.
pub use subscriptions::SubscriptionStatus;
/// Re-export of Trade type.
pub use trades::Trade;
/// Re-export of TradeSide type.
pub use trades::TradeSide;
/// Re-export of TradingPair type.
pub use trading_pair::TradingPair;
/// Re-export of WorkerId type.
pub use workers::WorkerId;

/// README code blocks compile as doc tests, so the README cannot drift from
/// the API. Invisible in rustdoc; exercised by `cargo test --doc`.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
pub struct ReadmeDoctests;
