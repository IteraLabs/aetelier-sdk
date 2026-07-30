//! KuCoin `/market/match` public trade payload types.

use serde::Deserialize;

/// `/market/match` trade payload.
#[derive(Deserialize, Debug, Clone)]
pub struct KucoinMatchData {
    pub symbol: String,
    /// Taker side: `"buy"` / `"sell"`.
    pub side: String,
    pub price: String,
    pub size: String,
    /// Match time in **nanoseconds** (string).
    pub time: String,
    #[serde(rename = "tradeId")]
    pub trade_id: String,
}
