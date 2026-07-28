//! Kraken WebSocket v2 decoder.
//!
//! Routes incoming JSON messages by the `"channel"` field to produce
//! [`KrakenWssEvent`] variants.  Heartbeats and subscription
//! confirmations are silently consumed.

use crate::{
    clients::wss::WssDecoder,
    errors::ExchangeError,
    sources::kraken::{
        events::KrakenWssEvent,
        responses::{orderbooks::KrakenBookResponse, trades::KrakenTradeResponse},
    },
};
use serde_json::Value;

/// [`WssDecoder`] implementation for the Kraken WebSocket v2 API.
///
/// `KrakenDecoder` is a zero-sized type — all state lives in the
/// frame payload itself.  Decoding proceeds as follows:
///
/// 1. Parse the raw text into a [`serde_json::Value`].
/// 2. Check for `"method"` field (subscription confirmation / error) —
///    consumed as `Ok(None)`.
/// 3. Dispatch on the `"channel"` field:
///    - `"book"` → [`KrakenWssEvent::OrderbookData`]
///    - `"trade"` → [`KrakenWssEvent::TradeData`]
///    - `"heartbeat"`, `"status"`, or unknown → `Ok(None)`
///
/// Parse failures on data frames are logged at `WARN` and swallowed as
/// `Ok(None)` to avoid tearing down the WebSocket pump.
pub struct KrakenDecoder;

impl WssDecoder for KrakenDecoder {
    type Event = KrakenWssEvent;

    fn decode(text: &str) -> Result<Option<Self::Event>, Box<ExchangeError>> {
        let json: Value =
            serde_json::from_str(text).map_err(|e| Box::new(ExchangeError::from(e)))?;

        // ── Subscription confirmations / errors ──────────────────────
        // Kraken v2 responses carry `"method": "subscribe"` with
        // `"success": bool` and an optional `"error"` string.
        if let Some(method) = json.get("method").and_then(|v| v.as_str()) {
            let success = json
                .get("success")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);

            // Extract the channel from the nested result object.
            let channel = json
                .get("result")
                .and_then(|r| r.get("channel"))
                .and_then(|c| c.as_str())
                .unwrap_or("unknown");

            if success {
                tracing::info!(
                    method = method,
                    channel = channel,
                    "kraken.subscription_confirmed"
                );
            } else {
                let error = json
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown error");
                tracing::error!(
                    method = method,
                    channel = channel,
                    error = error,
                    raw = text,
                    "kraken.subscription_failed"
                );
            }
            return Ok(None);
        }

        // ── Heartbeat messages ───────────────────────────────────────
        // Kraken sends `{"channel": "heartbeat"}` roughly every second.
        let channel = json.get("channel").and_then(|c| c.as_str()).unwrap_or("");

        match channel {
            "book" => match serde_json::from_str::<KrakenBookResponse>(text) {
                Ok(resp) => Ok(Some(KrakenWssEvent::OrderbookData(resp))),
                Err(e) => {
                    tracing::warn!("Failed to parse Kraken book data: {}", e);
                    Ok(None)
                }
            },

            "trade" => {
                match serde_json::from_str::<KrakenTradeResponse>(text) {
                    Ok(resp) => {
                        // Kraken v2 batches multiple trades per frame — carry
                        // them all (no head-drop).
                        if resp.data.is_empty() {
                            Ok(None)
                        } else {
                            Ok(Some(KrakenWssEvent::TradeData(resp.data)))
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to parse Kraken trade data: {}", e);
                        Ok(None)
                    }
                }
            }

            // heartbeat, status, or unknown — silently ignore
            _ => {
                tracing::debug!("Kraken: ignoring channel={}", channel);
                Ok(None)
            }
        }
    }
}
