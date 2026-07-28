use serde::Deserialize;

/// Envelope for the Gate.io `spot.trades` channel.
///
/// Unlike most venues, `result` is a **single** trade object (not an
/// array).
///
/// ```json
/// {
///   "time": 1606292218, "time_ms": 1606292218231,
///   "channel": "spot.trades", "event": "update",
///   "result": {
///     "id": 309143071, "create_time": 1606292218,
///     "create_time_ms": "1606292218213.4578", "side": "sell",
///     "currency_pair": "BTC_USDT", "amount": "16.47", "price": "0.4705"
///   }
/// }
/// ```
#[derive(Deserialize, Debug, Clone)]
pub struct GateioTradeResponse {
    /// Server timestamp (Unix milliseconds); present on data frames.
    #[serde(default)]
    pub time_ms: Option<u64>,
    /// Channel name (`"spot.trades"`).
    pub channel: String,
    /// `"update"` for data frames.
    pub event: String,
    /// The single trade payload.
    pub result: GateioTradeData,
}

/// A single public trade from the Gate.io `spot.trades` channel.
///
/// `side` is the **taker** direction; only the taker side is published on
/// the public channel.
#[derive(Deserialize, Debug, Clone)]
pub struct GateioTradeData {
    /// Exchange-assigned trade id.
    pub id: u64,
    /// Trade time, Unix seconds.
    #[serde(default)]
    pub create_time: i64,
    /// Trade time, Unix milliseconds as a string with a fractional
    /// sub-millisecond part (e.g. `"1606292218213.4578"`).
    pub create_time_ms: String,
    /// Taker side: `"buy"` or `"sell"`.
    pub side: String,
    /// Currency pair (e.g. `"BTC_USDT"`).
    pub currency_pair: String,
    /// Trade size (base-currency units) as a string.
    pub amount: String,
    /// Trade price as a string.
    pub price: String,
}

impl GateioTradeData {
    /// Trade timestamp parsed to Unix milliseconds (the fractional
    /// sub-millisecond part is truncated).
    #[inline]
    pub fn ts_ms(&self) -> u64 {
        // "1606292218213.4578" -> 1606292218213
        if let Some(whole) = self.create_time_ms.split('.').next()
            && let Ok(ms) = whole.parse::<u64>()
        {
            return ms;
        }
        // Fallback: parse as float, or derive from create_time seconds.
        self.create_time_ms
            .parse::<f64>()
            .map(|f| f as u64)
            .unwrap_or((self.create_time.max(0) as u64) * 1000)
    }
}
