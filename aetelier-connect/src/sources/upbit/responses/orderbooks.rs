//! Upbit `orderbook` channel wire types.

use serde::Deserialize;

/// One `orderbook_units` row (both sides at a depth rank).
#[derive(Deserialize, Debug, Clone)]
pub struct UpbitBookUnit {
    pub ask_price: f64,
    pub bid_price: f64,
    pub ask_size: f64,
    pub bid_size: f64,
}

/// `{"type":"orderbook","code":"KRW-BTC","timestamp":…,"orderbook_units":[…]}`
/// — a full top-N book every frame.
#[derive(Deserialize, Debug, Clone)]
pub struct UpbitOrderbook {
    pub code: String,
    pub timestamp: u64,
    pub orderbook_units: Vec<UpbitBookUnit>,
}
