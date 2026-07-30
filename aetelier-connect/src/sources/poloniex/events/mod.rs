//! Poloniex v2 WebSocket event types produced by the decoder from the
//! `book_lv2` and `trades` channels.

use crate::sources::poloniex::responses::{PoloniexBookFrame, PoloniexTradeFrame};

#[derive(Debug, Clone)]
pub enum PoloniexWssEvent {
    Book(PoloniexBookFrame),
    Trades(PoloniexTradeFrame),
}
