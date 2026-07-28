//! Typed events emitted by [`GateioDecoder`](crate::sources::gateio::decoder::GateioDecoder).
//!
//! Each variant wraps the exchange-specific payload from
//! [`crate::sources::gateio::responses`].

use crate::sources::gateio::responses::{GateioOrderbookResponse, GateioTradeData};

/// Domain events produced by decoding Gate.io V4 public WebSocket frames.
#[derive(Debug, Clone)]
pub enum GateioWssEvent {
    /// Order-book snapshot from `spot.order_book` (full limited-depth book).
    OrderbookData(GateioOrderbookResponse),
    /// A single public trade from `spot.trades`.
    TradeData(GateioTradeData),
}
