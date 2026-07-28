//! OKX V5 public WebSocket source (spot).
//!
//! Layout matches the other venues: [`client`] (connection, subscription,
//! heartbeat), stateless [`decoder`], typed [`events`], raw [`responses`].
//!
//! No auth. Order book = `books5`: a full top-5 snapshot every 100 ms, so
//! no reconstruction. Trades = `trades`. The incremental `books` channel is
//! not used.

pub mod client;
pub mod decoder;
pub mod events;
pub mod responses;
pub mod tooling;
