//! Bybit WebSocket v5 decoder.
//!
//! Routes incoming JSON frames from the Bybit unified v5 stream to
//! [`BybitWssEvent`] variants.  Control frames (subscription acks,
//! `pong` heartbeats) are silently consumed and produce `Ok(None)`.
//!
//! # Topic dispatch
//!
//! Bybit data messages carry a `"topic"` field whose prefix selects the
//! channel:
//!
//! | Prefix              | Event variant                       |
//! |---------------------|-------------------------------------|
//! | `allLiquidation.*`  | [`BybitWssEvent::LiquidationData`]  |
//! | `publicTrade.*`     | [`BybitWssEvent::TradeData`]         |
//! | `orderbook.*`       | [`BybitWssEvent::OrderbookData`]     |
//! | `tickers*`          | [`BybitWssEvent::TickerData`]        |

use crate::{
    clients::wss::WssDecoder,
    errors::ExchangeError,
    sources::bybit::events::BybitWssEvent,
    sources::bybit::responses::{
        liquidations::BybitLiquidationResponse, orderbooks::BybitOrderbookResponse,
        tickers::BybitTickerResponse, trades::BybitTradeResponse,
    },
};
use serde_json::Value;

/// [`WssDecoder`] implementation for the Bybit unified v5 WebSocket API.
///
/// `BybitDecoder` is a zero-sized type — all state lives in the frame
/// payload itself.  Decoding is a two-stage process:
///
/// 1. Parse the raw text into a [`serde_json::Value`] to inspect the
///    top-level control fields (`success`, `op`, `topic`).
/// 2. Dispatch to a strongly-typed [`serde_json::from_str`] call for the
///    matched topic prefix.
///
/// Parse failures on data frames are logged at `WARN` and swallowed as
/// `Ok(None)` so that a single malformed message does not tear down the
/// entire WebSocket pump.
pub struct BybitDecoder;

impl WssDecoder for BybitDecoder {
    type Event = BybitWssEvent;

    fn decode(text: &str) -> Result<Option<Self::Event>, Box<ExchangeError>> {
        let json: Value =
            serde_json::from_str(text).map_err(|e| Box::new(ExchangeError::from(e)))?;

        // ── Subscription confirmation / error ──────────────────────────
        // Bybit returns `{"success": true/false, "op": "subscribe", "ret_msg": "..."}`.
        if let Some(op) = json.get("op") {
            let op_str = op.as_str().unwrap_or("");
            match op_str {
                "pong" => return Ok(None),
                "subscribe" => {
                    let success = json
                        .get("success")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    let ret_msg =
                        json.get("ret_msg").and_then(|v| v.as_str()).unwrap_or("");
                    let conn_id = json
                        .get("conn_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown");

                    if success {
                        tracing::info!(
                            conn_id = conn_id,
                            ret_msg = ret_msg,
                            "bybit.subscription_confirmed"
                        );
                    } else {
                        tracing::error!(
                            conn_id = conn_id,
                            ret_msg = ret_msg,
                            raw = text,
                            "bybit.subscription_failed"
                        );
                    }
                    return Ok(None);
                }
                _ => {
                    // Other op types (e.g. "auth") — log and skip.
                    tracing::debug!(op = op_str, "bybit.control_frame");
                    return Ok(None);
                }
            }
        }

        // Standalone success field without op (legacy format).
        if let Some(success) = json.get("success")
            && success.as_bool() == Some(true)
        {
            tracing::info!("bybit.subscription_confirmed_legacy");
            return Ok(None);
        }

        // Check if this has a topic field
        if let Some(topic) = json.get("topic") {
            if let Some(topic_str) = topic.as_str() {
                // Liquidation Data
                if topic_str.starts_with("allLiquidation.") {
                    // Try to parse as liquidation response
                    match serde_json::from_str::<BybitLiquidationResponse>(text) {
                        Ok(env) => {
                            // Return the first liquidation data item if available
                            if let Some(liq_data) = env.data.into_iter().next() {
                                Ok(Some(Self::Event::LiquidationData(liq_data)))
                            } else {
                                Ok(None)
                            }
                        }
                        Err(e) => {
                            tracing::warn!("Failed to parse liquidation data: {}", e);
                            Ok(None)
                        }
                    }
                // Public Trades Data
                } else if topic_str.starts_with("publicTrade.") {
                    // Try to parse
                    match serde_json::from_str::<BybitTradeResponse>(text) {
                        // A `publicTrade` frame batches multiple fills — carry
                        // them all (no head-drop), mirroring OKX/Coinbase/Kraken.
                        Ok(env) => Ok(Some(Self::Event::TradeData(env.data))),
                        Err(e) => {
                            tracing::warn!("Failed to parse public trade data: {}", e);
                            Ok(None)
                        }
                    }
                // Orderbooks Data
                } else if topic_str.starts_with("orderbook.") {
                    // Try to parse
                    match serde_json::from_str::<BybitOrderbookResponse>(text) {
                        Ok(env) => {
                            if let Some(orderbook_data) = Some(env) {
                                Ok(Some(Self::Event::OrderbookData(orderbook_data)))
                            } else {
                                Ok(None)
                            }
                        }
                        Err(e) => {
                            tracing::warn!("Failed to parse order book data: {}", e);
                            Ok(None)
                        }
                    }
                // Funding Rates Data
                } else if topic_str.starts_with("tickers") {
                    match serde_json::from_str::<BybitTickerResponse>(text) {
                        Ok(env) => Ok(Some(Self::Event::TickerData(env.data))),
                        Err(e) => {
                            tracing::warn!("Failed to parse ticker data: {}", e);
                            Ok(None)
                        }
                    }
                } else {
                    Ok(None) // Ignore other topics
                }
            } else {
                Ok(None)
            }
        } else {
            // Log unknown message types for debugging
            tracing::debug!("Received unknown message type: {}", text);
            Ok(None)
        }
    }
}
