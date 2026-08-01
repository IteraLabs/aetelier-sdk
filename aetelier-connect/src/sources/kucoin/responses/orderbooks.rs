//! KuCoin `/market/level2` incremental orderbook payload types.

use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct KucoinChange(pub String, pub String, pub String);

#[derive(Deserialize, Debug, Clone, Default)]
pub struct KucoinChanges {
    #[serde(default)]
    pub asks: Vec<KucoinChange>,
    #[serde(default)]
    pub bids: Vec<KucoinChange>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct KucoinL2Data {
    #[serde(rename = "sequenceStart")]
    pub sequence_start: u64,
    #[serde(rename = "sequenceEnd")]
    pub sequence_end: u64,
    pub symbol: String,
    pub changes: KucoinChanges,
    #[serde(default)]
    pub time: u64,
}
