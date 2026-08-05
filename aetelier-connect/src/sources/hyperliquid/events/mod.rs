use crate::sources::hyperliquid::responses::{HyperliquidBook, HyperliquidTrade};

#[derive(Debug, Clone)]
pub enum HyperliquidWssEvent {
    Book(HyperliquidBook),
    Trades(Vec<HyperliquidTrade>),
}
