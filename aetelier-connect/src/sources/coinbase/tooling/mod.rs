//! Coinbase subscription helpers.
//!
//! Coinbase Advanced Trade uses a different subscription model from Bybit:
//! channels and product_ids are specified separately in the subscribe message.
//! This module helps build the channel list from a [`MarketSnapshotConfig`](aetelier_types::config::MarketSnapshotConfig).

/// Build the list of Coinbase channels to subscribe to based on enabled data types.
///
/// For Coinbase spot (Advanced Trade):
/// - Orderbooks → `"level2"`
/// - Trades → `"market_trades"`
///
/// Liquidations, funding rates, and open interest are **not** available
/// on spot — they require Coinbase INTX (perpetual futures).
pub fn channels_for_config(
    collect_orderbooks: bool,
    collect_trades: bool,
) -> Vec<String> {
    let mut channels = Vec::new();
    if collect_orderbooks {
        channels.push("level2".to_string());
    }
    if collect_trades {
        channels.push("market_trades".to_string());
    }
    channels
}
