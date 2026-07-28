//! Coinbase WebSocket event types.
//!
//! For Coinbase Advanced Trade (spot), only orderbook and trade channels
//! are available. Liquidations, funding rates, and open interest require
//! Coinbase INTX (perpetual futures).

use crate::sources::coinbase::responses::*;

/// Events produced by [`CoinbaseDecoder`](crate::sources::coinbase::decoder::CoinbaseDecoder) from the Advanced Trade WSS feed.
#[derive(Debug, Clone)]
pub enum CoinbaseWssEvent {
    /// level2 channel — orderbook snapshot or incremental update.
    OrderbookData(CoinbaseOrderbookResponse),
    /// market_trades channel — public trade executions. A single frame may batch
    /// many trades (across one or more events), so the variant carries them all.
    TradeData(Vec<CoinbaseTradeData>),
    /// heartbeats channel — one frame per second carrying the server-global
    /// `heartbeat_counter` beside the connection-wide `sequence_num`. The
    /// level2 socket's continuity anchor: a missing counter slot the
    /// heartbeat delta cannot explain is a dropped data message.
    Heartbeat {
        sequence_num: u64,
        heartbeat_counter: u64,
    },
    /// Any other sequenced frame the socket carries (the `subscriptions`
    /// ack, unknown future channels). Emitted so the sequence tracker sees
    /// EVERY counter slot — a silently-swallowed ack would otherwise read as
    /// a one-message gap (live capture 2026-07-16: the two acks arrive as
    /// channel `subscriptions` with `sequence_num` 5 and 6).
    Control { sequence_num: u64 },
}
