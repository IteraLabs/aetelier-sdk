use serde::Deserialize;

use super::orderbooks::OkxArg;

/// Envelope for the OKX `trades` channel.
///
/// ```json
/// {
///   "arg": { "channel": "trades", "instId": "BTC-USDT" },
///   "data": [{
///     "instId": "BTC-USDT", "tradeId": "216970876",
///     "px": "31684.5", "sz": "0.00001186", "side": "buy",
///     "ts": "1626531038288"
///   }]
/// }
/// ```
#[derive(Deserialize, Debug, Clone)]
pub struct OkxTradeResponse {
    /// Channel / instrument envelope.
    pub arg: OkxArg,
    /// One or more trade prints.
    pub data: Vec<OkxTradeData>,
}

/// A single public trade from the OKX `trades` channel.
///
/// All numeric values are delivered as strings. `side` is the **taker**
/// direction: `"buy"` = taker lifted the ask, `"sell"` = taker hit the bid.
#[derive(Deserialize, Debug, Clone)]
pub struct OkxTradeData {
    /// Instrument id (e.g. `"BTC-USDT"`).
    #[serde(rename = "instId")]
    pub inst_id: String,
    /// Exchange-assigned trade id (a string, even though numeric).
    #[serde(rename = "tradeId")]
    pub trade_id: String,
    /// Price as a string.
    pub px: String,
    /// Size (base-currency units) as a string.
    pub sz: String,
    /// Taker side: `"buy"` or `"sell"`.
    pub side: String,
    /// Exchange timestamp, Unix milliseconds as a string.
    pub ts: String,
}

impl OkxTradeData {
    /// Exchange timestamp parsed to Unix milliseconds.
    #[inline]
    pub fn ts_ms(&self) -> u64 {
        self.ts.parse().unwrap_or(0)
    }
}
