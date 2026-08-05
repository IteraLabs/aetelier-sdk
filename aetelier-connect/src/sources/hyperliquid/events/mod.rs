use crate::sources::hyperliquid::responses::{
    HyperliquidAssetCtxMsg, HyperliquidBook, HyperliquidTrade,
};

#[derive(Debug, Clone)]
pub enum HyperliquidWssEvent {
    Book(HyperliquidBook),
    Trades(Vec<HyperliquidTrade>),
    AssetCtx(HyperliquidAssetCtxMsg),
}
