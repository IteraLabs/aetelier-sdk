use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct HyperliquidAssetCtxMsg {
    pub coin: String,
    pub ctx: HyperliquidAssetCtx,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HyperliquidAssetCtx {
    #[serde(default)]
    pub funding: Option<String>,
    #[serde(default)]
    pub open_interest: Option<String>,
    #[serde(default)]
    pub mark_px: Option<String>,
    #[serde(default)]
    pub oracle_px: Option<String>,
    #[serde(default)]
    pub mid_px: Option<String>,
    #[serde(default)]
    pub premium: Option<String>,
    #[serde(default)]
    pub impact_pxs: Option<Vec<String>>,
    #[serde(default)]
    pub prev_day_px: Option<String>,
    #[serde(default)]
    pub day_ntl_vlm: Option<String>,
    #[serde(default)]
    pub day_base_vlm: Option<String>,
}
