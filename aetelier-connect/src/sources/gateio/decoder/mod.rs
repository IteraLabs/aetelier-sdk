//! Gate.io V4 public WebSocket decoder.
//!
//! Routes incoming frames to [`GateioWssEvent`] variants. Subscription acks
//! (`event != "update"`) and `spot.pong` heartbeat replies are silently
//! consumed and produce `Ok(None)`.
//!
//! # Dispatch
//!
//! Data frames carry `event == "update"` and a `channel` field:
//!
//! | Channel            | Event variant                   |
//! |--------------------|---------------------------------|
//! | `spot.order_book`  | [`GateioWssEvent::OrderbookData`] |
//! | `spot.trades`      | [`GateioWssEvent::TradeData`]      |

use crate::{
    clients::wss::WssDecoder,
    errors::ExchangeError,
    sources::gateio::events::GateioWssEvent,
    sources::gateio::responses::{GateioOrderbookResponse, GateioTradeResponse},
};
use serde_json::Value;

/// [`WssDecoder`] implementation for the Gate.io V4 public WebSocket API.
///
/// Zero-sized; all routing is determined by frame content.
pub struct GateioDecoder;

impl WssDecoder for GateioDecoder {
    type Event = GateioWssEvent;

    fn decode(text: &str) -> Result<Option<Self::Event>, Box<ExchangeError>> {
        let json: Value =
            serde_json::from_str(text).map_err(|e| Box::new(ExchangeError::from(e)))?;

        let event = json.get("event").and_then(|v| v.as_str());

        // ── Control frames (acks, pong, anything that is not data) ──────
        if event != Some("update") {
            match event {
                Some("subscribe") => tracing::info!("gateio.subscription_confirmed"),
                Some("unsubscribe") => tracing::info!("gateio.unsubscribe_confirmed"),
                _ => {
                    // spot.pong, errors, empty-event frames, etc.
                    if let Some(err) = json.get("error")
                        && !err.is_null()
                    {
                        tracing::error!(raw = text, "gateio.frame_error");
                    }
                }
            }
            return Ok(None);
        }

        // ── Data frames: dispatch on channel ────────────────────────────
        let channel = json.get("channel").and_then(|c| c.as_str());

        match channel {
            Some("spot.order_book") => {
                match serde_json::from_str::<GateioOrderbookResponse>(text) {
                    Ok(resp) => Ok(Some(GateioWssEvent::OrderbookData(resp))),
                    Err(e) => {
                        tracing::warn!("Failed to parse Gateio order book: {}", e);
                        Ok(None)
                    }
                }
            }
            Some("spot.trades") => {
                match serde_json::from_str::<GateioTradeResponse>(text) {
                    Ok(resp) => Ok(Some(GateioWssEvent::TradeData(resp.result))),
                    Err(e) => {
                        tracing::warn!("Failed to parse Gateio trade: {}", e);
                        Ok(None)
                    }
                }
            }
            _ => Ok(None),
        }
    }
}
