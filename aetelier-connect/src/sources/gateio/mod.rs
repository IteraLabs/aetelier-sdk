//! Gate.io V4 public WebSocket source (spot).
//!
//! Layout matches the other venues. No auth. Order book =
//! `spot.order_book`: a full limited-depth snapshot at a fixed interval, so
//! no reconstruction. Trades = `spot.trades`. The incremental
//! `spot.order_book_update` channel is not used.

pub mod client;
pub mod decoder;
pub mod events;
pub mod responses;
pub mod tooling;
