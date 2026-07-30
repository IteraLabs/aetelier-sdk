use serde::Deserialize;

/// Individual trade from the `<symbol>@trade` stream.
///
/// ```json
/// {
///   "e": "trade",
///   "E": 1672304484978,
///   "s": "BTCUSDT",
///   "t": 12345,
///   "p": "21921.73",
///   "q": "0.063",
///   "T": 1672304484975,
///   "m": true
/// }
/// ```
#[derive(Deserialize, Debug, Clone)]
pub struct BinanceTradeData {
    /// Event type — always `"trade"`.
    #[serde(rename = "e")]
    pub event_type: String,

    /// Event time (Unix ms).
    #[serde(rename = "E")]
    pub event_time: u64,

    /// Symbol (e.g. `"BTCUSDT"`).
    #[serde(rename = "s")]
    pub symbol: String,

    /// Trade ID.
    #[serde(rename = "t")]
    pub trade_id: u64,

    /// Price (string to preserve precision).
    #[serde(rename = "p")]
    pub price: String,

    /// Quantity (string to preserve precision).
    #[serde(rename = "q")]
    pub quantity: String,

    /// Trade time (Unix ms).
    #[serde(rename = "T")]
    pub trade_time: u64,

    /// `true` if the buyer is the market maker (i.e. the trade was a sell).
    #[serde(rename = "m")]
    pub is_buyer_maker: bool,
}

impl BinanceTradeData {
    /// Taker side: if `is_buyer_maker` then taker sold, else taker bought.
    pub fn taker_side(&self) -> &'static str {
        if self.is_buyer_maker { "sell" } else { "buy" }
    }
}
