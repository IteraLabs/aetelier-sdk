//! Bitso `trades` wire payloads.

use serde::Deserialize;

/// One trade. `t`: 0 = taker buy, 1 = taker sell.
#[derive(Deserialize, Debug, Clone)]
pub struct BitsoTrade {
    /// Trade id.
    pub i: u64,
    /// Amount, a decimal string.
    pub a: String,
    /// Rate (price), a decimal string.
    pub r: String,
    /// Taker side: 0 = buy, 1 = sell.
    pub t: u8,
    /// Execution timestamp in ms — the WSS `trades` payload carries it as
    /// `x` (NOT `d`, which is the diff-orders field). `epoch_to_us` scales
    /// it to the platform µs standard.
    #[serde(default)]
    pub x: u64,
}

#[derive(Deserialize, Debug, Clone)]
pub struct BitsoTradeMessage {
    pub book: String,
    pub payload: Vec<BitsoTrade>,
}
