use serde::Deserialize;

/// Top-level market_trades WebSocket message from Coinbase Advanced Trade.
///
/// ```json
/// {
///   "channel": "market_trades",
///   "timestamp": "2023-02-09T20:19:35.39625135Z",
///   "sequence_num": 0,
///   "events": [{ "type": "snapshot", "trades": [...] }]
/// }
/// ```
#[derive(Deserialize, Debug, Clone)]
pub struct CoinbaseTradeResponse {
    pub channel: String,
    pub timestamp: String,
    pub sequence_num: u64,
    pub events: Vec<CoinbaseTradeEvent>,
}

/// A single trade event (snapshot or update) within the response.
#[derive(Deserialize, Debug, Clone)]
pub struct CoinbaseTradeEvent {
    #[serde(rename = "type")]
    pub ty: String,
    pub trades: Vec<CoinbaseTradeData>,
}

/// Individual trade from the `market_trades` channel.
#[derive(Deserialize, Debug, Clone)]
pub struct CoinbaseTradeData {
    pub trade_id: String,
    pub product_id: String,
    /// Trade price as string (preserves precision).
    pub price: String,
    /// Trade size as string.
    pub size: String,
    /// Maker side: `"BUY"` or `"SELL"`.
    pub side: String,
    /// ISO 8601 timestamp of the trade.
    pub time: String,
}

impl CoinbaseTradeData {
    /// Parse the ISO 8601 timestamp to UTC epoch microseconds.
    pub fn timestamp_us(&self) -> u64 {
        chrono::DateTime::parse_from_rfc3339(&self.time)
            .map(|dt| dt.timestamp_micros() as u64)
            .unwrap_or(0)
    }
}
