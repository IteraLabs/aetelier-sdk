//! Limit order book data structures and operations.
//!
//! Provides [`Orderbook`] for managing order book state, [`OrderbookDelta`]
//! for incremental updates, and persistence utilities for serialization.

/// Core orderbook data structure and operations.
pub mod core;
/// Incremental orderbook delta updates.
pub mod delta;
/// Orderbook persistence and serialization.
pub mod persist;
/// Orderbook loading targets and result types.
pub mod target;

pub use core::{Orderbook, decimal_to_f64, f64_to_decimal};
pub use delta::{L3Order, NormalizedDelta, OrderbookDelta};
pub use persist::{OrderbookSnapshot, OutputFormat, PriceLevelRecord};
pub use target::{
    OrderbookTarget, OrderbookTargetData, OrderbookUpdate, OrderbookUpdateType,
};
