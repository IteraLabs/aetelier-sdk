use serde::Deserialize;

/// Top-level `trade` channel message from Kraken WebSocket v2.
///
/// ```json
/// {
///   "channel": "trade",
///   "type": "update",
///   "data": [{
///     "symbol": "BTC/USD",
///     "side": "buy",
///     "price": 23536.30,
///     "qty": 0.001,
///     "ord_type": "limit",
///     "trade_id": 12345,
///     "timestamp": "2023-02-09T20:19:35.396Z"
///   }]
/// }
/// ```
#[derive(Deserialize, Debug, Clone)]
pub struct KrakenTradeResponse {
    pub channel: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub data: Vec<KrakenTradeData>,
}

/// Individual trade from the `trade` channel.
///
/// Kraken sends `price` and `qty` as JSON numbers (floats), unlike
/// Bybit/Coinbase which use strings.  `side` is `"buy"` or `"sell"`.
#[derive(Deserialize, Debug, Clone)]
pub struct KrakenTradeData {
    pub symbol: String,
    /// `"buy"` or `"sell"` (taker side).
    pub side: String,
    /// Trade price as a float.
    pub price: f64,
    /// Trade quantity as a float.
    pub qty: f64,
    /// Order type: `"limit"` or `"market"`.
    #[serde(default)]
    pub ord_type: String,
    /// Unique trade sequence number per book.
    pub trade_id: u64,
    /// RFC 3339 timestamp of the trade.
    pub timestamp: String,
}

impl KrakenTradeData {
    /// Parse the RFC 3339 timestamp to UTC epoch microseconds.
    pub fn timestamp_us(&self) -> u64 {
        chrono::DateTime::parse_from_rfc3339(&self.timestamp)
            .map(|dt| dt.timestamp_micros() as u64)
            .unwrap_or(0)
    }
}
