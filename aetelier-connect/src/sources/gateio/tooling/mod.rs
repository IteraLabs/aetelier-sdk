//! Gate.io subscription helpers.
//!
//! Gateio subscribes by sending one frame per channel:
//! `{"time", "channel", "event":"subscribe", "payload":[...]}`. This module
//! maps the enabled data-type flags to channel names and snaps the
//! configured depth to a value the `spot.order_book` channel accepts.

/// Build the list of Gateio channels to subscribe to based on enabled feeds.
///
/// - Orderbooks → `"spot.order_book"` (full limited-depth snapshot).
/// - Trades → `"spot.trades"`.
pub fn channels_for_config(
    collect_orderbooks: bool,
    collect_trades: bool,
) -> Vec<String> {
    let mut channels = Vec::new();
    if collect_orderbooks {
        channels.push("spot.order_book".to_string());
    }
    if collect_trades {
        channels.push("spot.trades".to_string());
    }
    channels
}

/// Snap a requested depth to the nearest level the `spot.order_book`
/// channel accepts (`1, 5, 10, 20, 50, 100`), rounding **up**.
pub fn snap_level(depth: usize) -> usize {
    const ALLOWED: [usize; 6] = [1, 5, 10, 20, 50, 100];
    for &a in ALLOWED.iter() {
        if depth <= a {
            return a;
        }
    }
    100
}
