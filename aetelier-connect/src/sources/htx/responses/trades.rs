//! HTX trade wire types for the `market.<sym>.trade.detail` channel.

use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct HtxTradeDetail {
    pub ts: u64,
    #[serde(rename = "tradeId")]
    pub trade_id: u64,
    pub price: f64,
    pub amount: f64,
    /// Taker side: `"buy"` / `"sell"`.
    pub direction: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct HtxTradeTick {
    #[serde(default)]
    pub data: Vec<HtxTradeDetail>,
}

/// Trade push: `{"ch":"market.btcusdt.trade.detail","tick":{"data":[…]}}`.
#[derive(Deserialize, Debug, Clone)]
pub struct HtxTradeFrame {
    pub ch: String,
    pub tick: HtxTradeTick,
}
