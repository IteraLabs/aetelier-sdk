use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct HyperliquidLevel {
    pub px: String,
    pub sz: String,
    pub n: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HyperliquidBook {
    pub coin: String,
    pub time: u64,
    pub levels: Vec<Vec<HyperliquidLevel>>,
}
