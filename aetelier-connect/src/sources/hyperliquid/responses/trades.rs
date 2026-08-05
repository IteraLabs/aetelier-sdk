use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct HyperliquidTrade {
    pub coin: String,
    pub side: String,
    pub px: String,
    pub sz: String,
    pub time: u64,
    pub tid: u64,
    #[serde(default)]
    pub users: Vec<String>,
    #[serde(default)]
    pub hash: String,
}
