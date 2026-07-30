//! Kraken subscription helpers.
//!
//! Kraken WebSocket v2 uses `{"method": "subscribe", "params": {"channel": ..., "symbol": [...]}}`
//! for subscription.  This module helps build the channel list from config flags.

/// Build the list of Kraken channels to subscribe to based on enabled data types.
///
/// For Kraken spot (WebSocket v2):
/// - Orderbooks → `"book"`
/// - Trades → `"trade"`
///
/// Liquidations, funding rates, and open interest are **not** available
/// on spot — they require Kraken Futures (separate endpoint + auth).
pub fn channels_for_config(
    collect_orderbooks: bool,
    collect_trades: bool,
) -> Vec<String> {
    let mut channels = Vec::new();
    if collect_orderbooks {
        channels.push("book".to_string());
    }
    if collect_trades {
        channels.push("trade".to_string());
    }
    channels
}
