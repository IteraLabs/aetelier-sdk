//! Typed events emitted by [`OkxDecoder`](crate::sources::okx::decoder::OkxDecoder).
//!
//! Each variant wraps the exchange-specific payload from
//! [`crate::sources::okx::responses`].

use crate::sources::okx::responses::{OkxOrderbookResponse, OkxTradeData};

/// Domain events produced by decoding OKX V5 public WebSocket frames.
#[derive(Debug, Clone)]
pub enum OkxWssEvent {
    /// Order-book snapshot from the `books5` channel (full top-N book).
    OrderbookData(OkxOrderbookResponse),
    /// Public trades from the `trades` channel. A single push may batch
    /// multiple prints (`data` is an array), so the variant carries them all.
    TradeData(Vec<OkxTradeData>),
}
