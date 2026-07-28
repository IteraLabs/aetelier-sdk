//! Poloniex v2 `trades` channel wire types.

use serde::Deserialize;

/// One trade print. `quantity` is the base amount; `takerSide` is the side.
#[derive(Deserialize, Debug, Clone)]
pub struct PoloniexTradeData {
    pub symbol: String,
    pub id: String,
    pub price: String,
    pub quantity: String,
    #[serde(rename = "takerSide", default)]
    pub taker_side: String,
    pub ts: u64,
    /// True trade-match time (ms). Preferred over `ts` (server push time)
    /// so trade latency reflects exchange generation, not relay, time.
    #[serde(rename = "createTime", default)]
    pub create_time: u64,
}

/// `trades` push: `{"channel":"trades","data":[…]}`.
#[derive(Deserialize, Debug, Clone)]
pub struct PoloniexTradeFrame {
    #[serde(default)]
    pub data: Vec<PoloniexTradeData>,
}
