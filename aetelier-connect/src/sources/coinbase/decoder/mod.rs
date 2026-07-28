//! Coinbase Advanced Trade WSS decoder.
//!
//! Routes incoming JSON messages by the `"channel"` field to produce
//! [`CoinbaseWssEvent`] variants.

use crate::{
    clients::wss::WssDecoder,
    errors::ExchangeError,
    sources::coinbase::{
        events::CoinbaseWssEvent,
        responses::{
            heartbeats::CoinbaseHeartbeatResponse, orderbooks::CoinbaseOrderbookResponse,
            trades::CoinbaseTradeResponse,
        },
    },
};
use serde_json::Value;

/// [`WssDecoder`] implementation for the Coinbase Advanced Trade WebSocket API.
///
/// `CoinbaseDecoder` is a zero-sized type — all state lives in the
/// frame payload itself.  Decoding proceeds as follows:
///
/// 1. Parse the raw text into a [`serde_json::Value`].
/// 2. Check for `"type": "subscriptions"` or `"error"` control messages
///    (subscription acks / errors) — consumed as `Ok(None)`.
/// 3. Dispatch on the `"channel"` field:
///    - `"l2_data"` → [`CoinbaseWssEvent::OrderbookData`]
///    - `"market_trades"` → [`CoinbaseWssEvent::TradeData`]
///    - `"heartbeats"` → [`CoinbaseWssEvent::Heartbeat`]
///    - anything else carrying a `sequence_num` (the `subscriptions` ack,
///      unknown channels) → [`CoinbaseWssEvent::Control`] — the sequence
///      tracker must observe every counter slot
///    - unsequenced frames → `Ok(None)`
///
/// Parse failures on data frames are logged at `WARN` and degrade to
/// [`CoinbaseWssEvent::Control`] (the slot is accounted, the payload is not)
/// to avoid tearing down the WebSocket pump.
pub struct CoinbaseDecoder;

impl WssDecoder for CoinbaseDecoder {
    type Event = CoinbaseWssEvent;

    fn decode(text: &str) -> Result<Option<Self::Event>, Box<ExchangeError>> {
        let json: Value =
            serde_json::from_str(text).map_err(|e| Box::new(ExchangeError::from(e)))?;

        // ── Subscription confirmation / error ──────────────────────────
        // Coinbase returns `{"type": "subscriptions", "channels": [...]}` on
        // success and `{"type": "error", "message": "..."}` on failure.
        if let Some(ty) = json.get("type") {
            let ty_str = ty.as_str().unwrap_or("");
            match ty_str {
                "subscriptions" => {
                    let channels = json
                        .get("channels")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|c| c.get("name").and_then(|n| n.as_str()))
                                .collect::<Vec<_>>()
                                .join(", ")
                        })
                        .unwrap_or_default();
                    tracing::info!(
                        channels = channels.as_str(),
                        "coinbase.subscription_confirmed"
                    );
                    return Ok(None);
                }
                "error" => {
                    let message = json
                        .get("message")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown error");
                    tracing::error!(
                        message = message,
                        raw = text,
                        "coinbase.subscription_failed"
                    );
                    return Ok(None);
                }
                _ => {} // fall through to channel dispatch
            }
        }

        // Data messages carry a `"channel"` field
        let channel = json.get("channel").and_then(|c| c.as_str()).unwrap_or("");
        // Every Advanced Trade frame carries the connection-wide counter. The
        // sequence tracker must see EVERY slot, so any consumed-but-sequenced
        // frame (ack, unparseable payload, unknown channel) degrades to
        // `Control { sequence_num }` rather than vanishing — a swallowed slot
        // would read downstream as a one-message gap.
        let seq = json.get("sequence_num").and_then(|s| s.as_u64());
        let sequenced_control = |ch: &str| match seq {
            Some(sequence_num) => {
                tracing::debug!(channel = ch, sequence_num, "coinbase.control_frame");
                Ok(Some(CoinbaseWssEvent::Control { sequence_num }))
            }
            None => {
                tracing::debug!(channel = ch, "coinbase.ignoring_unsequenced_frame");
                Ok(None)
            }
        };

        match channel {
            // level2 / l2_data channel — orderbook snapshots and updates
            "l2_data" => match serde_json::from_str::<CoinbaseOrderbookResponse>(text) {
                Ok(resp) => Ok(Some(CoinbaseWssEvent::OrderbookData(resp))),
                Err(e) => {
                    tracing::warn!("Failed to parse Coinbase level2 data: {}", e);
                    sequenced_control("l2_data")
                }
            },

            // market_trades channel
            "market_trades" => {
                match serde_json::from_str::<CoinbaseTradeResponse>(text) {
                    Ok(resp) => {
                        // Flatten: a frame may batch multiple trades across
                        // multiple events — collect them all (no head-drop).
                        let trades: Vec<_> = resp
                            .events
                            .into_iter()
                            .flat_map(|event| event.trades)
                            .collect();
                        if trades.is_empty() {
                            sequenced_control("market_trades")
                        } else {
                            Ok(Some(CoinbaseWssEvent::TradeData(trades)))
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to parse Coinbase trade data: {}", e);
                        sequenced_control("market_trades")
                    }
                }
            }

            // heartbeats channel — the level2 socket's contiguity anchor.
            "heartbeats" => {
                match serde_json::from_str::<CoinbaseHeartbeatResponse>(text) {
                    Ok(resp) => match resp.events.first() {
                        Some(ev) => Ok(Some(CoinbaseWssEvent::Heartbeat {
                            sequence_num: resp.sequence_num,
                            heartbeat_counter: ev.heartbeat_counter,
                        })),
                        None => sequenced_control("heartbeats"),
                    },
                    Err(e) => {
                        tracing::warn!("Failed to parse Coinbase heartbeat: {}", e);
                        sequenced_control("heartbeats")
                    }
                }
            }

            // Subscription acks (channel "subscriptions" carries sequence_num
            // 5/6 on a fresh socket — live capture 2026-07-16) and unknown
            // channels: surface the slot, drop the payload.
            other => sequenced_control(other),
        }
    }
}
