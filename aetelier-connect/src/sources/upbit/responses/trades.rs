//! Upbit `trade` channel wire types.

use serde::Deserialize;

/// `{"type":"trade","code":"KRW-BTC","trade_price":…,"trade_volume":…,
/// "ask_bid":"BID","trade_timestamp":…,"sequential_id":…}`.
#[derive(Deserialize, Debug, Clone)]
pub struct UpbitTrade {
    pub code: String,
    pub trade_price: f64,
    pub trade_volume: f64,
    /// Taker side: `"BID"` = taker bought, `"ASK"` = taker sold.
    pub ask_bid: String,
    pub trade_timestamp: u64,
    pub sequential_id: u64,
}
