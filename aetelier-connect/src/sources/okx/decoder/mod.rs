//! OKX V5 public WebSocket decoder.
//!
//! Routes incoming frames to [`OkxWssEvent`] variants. Control frames
//! (subscription acks/errors and the literal `"pong"` heartbeat reply) are
//! silently consumed and produce `Ok(None)`.
//!
//! # Dispatch
//!
//! Data frames carry an `arg.channel` field:
//!
//! | Channel prefix      | Event variant                  |
//! |---------------------|--------------------------------|
//! | `books*` / `bbo-tbt`| [`OkxWssEvent::OrderbookData`] |
//! | `trades`            | [`OkxWssEvent::TradeData`]      |
//!
//! Frames carrying an `event` field (`subscribe` ack / `error`) are control
//! frames and yield `Ok(None)`.

use crate::{
    clients::wss::WssDecoder,
    errors::ExchangeError,
    sources::okx::events::OkxWssEvent,
    sources::okx::responses::{OkxOrderbookResponse, OkxTradeResponse},
};
use serde_json::Value;

/// [`WssDecoder`] implementation for the OKX V5 public WebSocket API.
///
/// Zero-sized; all routing is determined by frame content.
pub struct OkxDecoder;

impl WssDecoder for OkxDecoder {
    type Event = OkxWssEvent;

    fn decode(text: &str) -> Result<Option<Self::Event>, Box<ExchangeError>> {
        // ── Heartbeat reply ─────────────────────────────────────────────
        // OKX answers the client's literal text "ping" with literal "pong".
        if text == "pong" {
            return Ok(None);
        }

        let json: Value =
            serde_json::from_str(text).map_err(|e| Box::new(ExchangeError::from(e)))?;

        // ── Control frames (subscription ack / error) ───────────────────
        if let Some(event) = json.get("event").and_then(|v| v.as_str()) {
            match event {
                "error" => tracing::error!(raw = text, "okx.subscription_error"),
                "subscribe" => tracing::info!("okx.subscription_confirmed"),
                "unsubscribe" => tracing::info!("okx.unsubscribe_confirmed"),
                other => tracing::debug!(event = other, "okx.control_frame"),
            }
            return Ok(None);
        }

        // ── Data frames: dispatch on arg.channel ────────────────────────
        let channel = json
            .get("arg")
            .and_then(|a| a.get("channel"))
            .and_then(|c| c.as_str());

        match channel {
            Some(ch) if ch.starts_with("books") || ch == "bbo-tbt" => {
                match serde_json::from_str::<OkxOrderbookResponse>(text) {
                    Ok(resp) => Ok(Some(OkxWssEvent::OrderbookData(resp))),
                    Err(e) => {
                        tracing::warn!("Failed to parse OKX order book: {}", e);
                        Ok(None)
                    }
                }
            }
            Some(ch) if ch.starts_with("trades") => {
                match serde_json::from_str::<OkxTradeResponse>(text) {
                    Ok(resp) => {
                        // A push may batch multiple prints — carry them all
                        // (no head-drop).
                        if resp.data.is_empty() {
                            Ok(None)
                        } else {
                            Ok(Some(OkxWssEvent::TradeData(resp.data)))
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to parse OKX trade: {}", e);
                        Ok(None)
                    }
                }
            }
            _ => Ok(None),
        }
    }
}
