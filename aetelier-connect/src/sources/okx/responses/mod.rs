//! OKX V5 WebSocket response types.
//!
//! Each public data frame carries an `arg` envelope (`{channel, instId}`)
//! plus a `data` array. Prices and sizes are delivered as JSON **strings**
//! to preserve precision.

pub mod orderbooks;
pub mod trades;

pub use orderbooks::{OkxArg, OkxLevel, OkxOrderbookData, OkxOrderbookResponse};
pub use trades::{OkxTradeData, OkxTradeResponse};
