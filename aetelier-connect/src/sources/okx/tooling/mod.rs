//! OKX subscription helpers.
//!
//! OKX subscribes by sending `{"op":"subscribe","args":[{"channel","instId"}]}`.
//! This module maps the enabled data-type flags to OKX channel names.

/// Build the list of OKX channels to subscribe to based on enabled feeds.
///
/// - Orderbooks → `"books5"` (full top-5 snapshot every 100 ms, no auth,
///   no checksum, no reconstruction).
/// - Trades → `"trades"`.
///
/// Liquidations, funding rates, and open interest are not wired for OKX
/// spot here.
pub fn channels_for_config(
    collect_orderbooks: bool,
    collect_trades: bool,
) -> Vec<String> {
    let mut channels = Vec::new();
    if collect_orderbooks {
        channels.push("books5".to_string());
    }
    if collect_trades {
        channels.push("trades".to_string());
    }
    channels
}
