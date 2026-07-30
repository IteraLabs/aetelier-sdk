//! Kraken WebSocket v2 event types.
//!
//! For Kraken spot, the public `book` and `trade` channels are available
//! without authentication.  Liquidations, funding rates, and open interest
//! are only available on Kraken Futures (separate endpoint + auth model).

use crate::sources::kraken::responses::*;

/// Events produced by [`KrakenDecoder`](crate::sources::kraken::decoder::KrakenDecoder) from the WebSocket v2 feed.
#[derive(Debug, Clone)]
pub enum KrakenWssEvent {
    /// `book` channel — orderbook snapshot or incremental update.
    OrderbookData(KrakenBookResponse),
    /// `trade` channel — public trade executions. Kraken v2 batches multiple
    /// trades per frame, so the variant carries them all.
    TradeData(Vec<KrakenTradeData>),
}
