//! Typed event variants emitted by [`BybitDecoder`](crate::sources::bybit::decoder::BybitDecoder).
//!
//! Each variant wraps the exchange-specific payload struct from
//! [`crate::sources::bybit::responses`].

use crate::sources::bybit::responses::*;

/// Domain events produced by decoding Bybit unified v5 WebSocket frames.
///
/// The decoder peels off the `topic`-based envelope and yields the inner
/// data payload.  Consumers pattern-match on these variants to route
/// events to the appropriate downstream processors.
#[derive(Debug, Clone)]
pub enum BybitWssEvent {
    /// Public trades from the `publicTrade.*` topic. A frame may batch multiple
    /// fills, so the variant carries them all (no head-drop).
    TradeData(Vec<BybitTradeData>),
    /// A single forced liquidation from the `allLiquidation.*` topic.
    LiquidationData(BybitLiquidationData),
    /// An orderbook snapshot or delta from the `orderbook.*` topic.
    ///
    /// Unlike the other variants this wraps the full response (including
    /// the topic, type, and timestamp envelope) because the consumer
    /// needs the `ty` field to distinguish snapshots from deltas.
    OrderbookData(BybitOrderbookResponse),
    /// Ticker / funding-rate snapshot from the `tickers.*` topic.
    TickerData(BybitTickerData),
}
