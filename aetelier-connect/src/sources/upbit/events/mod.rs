//! Upbit WebSocket event types.

use crate::sources::upbit::responses::{UpbitOrderbook, UpbitTrade};

#[derive(Debug, Clone)]
pub enum UpbitWssEvent {
    Orderbook(UpbitOrderbook),
    Trade(UpbitTrade),
}
