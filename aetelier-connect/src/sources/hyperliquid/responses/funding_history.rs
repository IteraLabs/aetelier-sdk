use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HyperliquidFundingHistoryRow {
    pub coin: String,
    pub funding_rate: String,
    #[serde(default)]
    pub premium: Option<String>,
    pub time: u64,
}
