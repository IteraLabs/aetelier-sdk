//! Bitget V2 WebSocket event types.
//!
//! One variant per public data channel (`books`, `trade`), produced by
//! [`BitgetDecoder`](crate::sources::bitget::decoder::BitgetDecoder).

use crate::sources::bitget::responses::{BitgetBookFrame, BitgetTradeFrame};

#[derive(Debug, Clone)]
pub enum BitgetWssEvent {
    Book(BitgetBookFrame),
    Trade(BitgetTradeFrame),
}
