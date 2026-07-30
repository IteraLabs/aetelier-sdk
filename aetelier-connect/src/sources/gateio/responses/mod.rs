//! Gate.io V4 WebSocket response types.
//!
//! Public data frames share a `{time, channel, event, result}` envelope.
//! Data frames carry `event == "update"`; subscription acks carry
//! `event == "subscribe"`/`"unsubscribe"`. Prices and sizes are delivered
//! as JSON **strings**.

pub mod orderbooks;
pub mod trades;

pub use orderbooks::{GateioLevel, GateioOrderbookData, GateioOrderbookResponse};
pub use trades::{GateioTradeData, GateioTradeResponse};
