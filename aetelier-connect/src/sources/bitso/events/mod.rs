//! Bitso WebSocket event types.
//!
//! The public `diff-orders` and `trades` streams are available without
//! authentication.

use crate::sources::bitso::responses::{BitsoDiffMessage, BitsoTradeMessage};

/// Events produced by [`BitsoDecoder`](crate::sources::bitso::decoder::BitsoDecoder) from the WebSocket feed.
#[derive(Debug, Clone)]
pub enum BitsoWssEvent {
    DiffOrders(BitsoDiffMessage),
    Trades(BitsoTradeMessage),
}
