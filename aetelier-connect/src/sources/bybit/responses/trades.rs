use serde::Deserialize;

/// Envelope for the `publicTrade.*` Bybit WebSocket topic.
///
/// ```json
/// {
///   "topic": "publicTrade.BTCUSDT",
///   "type": "snapshot",
///   "ts": 1672304484978,
///   "data": [{ "T": 1672304484978, "s": "BTCUSDT", ... }]
/// }
/// ```
#[derive(Deserialize, Debug, Clone)]
pub struct BybitTradeResponse {
    /// Topic string: `"publicTrade.{symbol}"`.
    pub topic: String,
    /// Message type (always `"snapshot"` for trades).
    #[serde(rename = "type")]
    pub ty: String,
    /// Server timestamp (Unix ms).
    pub ts: u64,
    /// One or more trade fills included in this frame.
    pub data: Vec<BybitTradeData>,
}

/// A single trade fill from the `publicTrade.*` topic.
///
/// All numeric values are delivered as strings to preserve full
/// precision.  Consumers should parse
/// with `Decimal` or `f64` as appropriate.
#[derive(Deserialize, Debug, Clone)]
pub struct BybitTradeData {
    /// Trade timestamp (Unix ms, exchange-reported).
    #[serde(rename = "T")]
    pub trade_ts: u64,
    /// Trading pair symbol (e.g. `"BTCUSDT"`).
    #[serde(rename = "s")]
    pub symbol: String,
    /// Taker side: `"Buy"` or `"Sell"`.
    #[serde(rename = "S")]
    pub side: String,
    /// Trade quantity as a string.
    #[serde(rename = "v")]
    pub amount: String,
    /// Trade price as a string.
    #[serde(rename = "p")]
    pub price: String,
    /// Tick direction: `"PlusTick"`, `"ZeroPlusTick"`, etc. Absent on spot
    /// `publicTrade` frames (Bybit only emits `L` on derivatives), so default
    /// to `None` rather than failing the whole trade decode.
    #[serde(rename = "L", default)]
    pub direction: Option<String>,
    /// Exchange-assigned trade ID.
    #[serde(rename = "i")]
    pub trade_id: String,
    /// `true` if this trade was a block trade.
    #[serde(rename = "BT")]
    pub block_trade: bool,
    /// `true` if this trade was a Retail Price Improvement (RPI) trade.
    #[serde(rename = "RPI")]
    pub rpi_trade: bool,
    /// Monotonic sequence number for gap detection.
    #[serde(rename = "seq")]
    pub sequence: u64,
}
