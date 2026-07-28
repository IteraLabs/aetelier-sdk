use crate::clients::wss::WssDecoder;
use crate::errors::ExchangeError;

use super::events::BinanceWssEvent;
use super::responses::{orderbooks::BinanceDepthUpdate, trades::BinanceTradeData};

/// Stateless decoder for Binance spot WebSocket frames.
///
/// Dispatches on the `"e"` (event type) field:
/// - `"depthUpdate"` → [`BinanceWssEvent::DepthUpdate`]
/// - `"trade"` → [`BinanceWssEvent::TradeData`]
/// - everything else (subscription acks, pongs) → `Ok(None)`
pub struct BinanceDecoder;

impl WssDecoder for BinanceDecoder {
    type Event = BinanceWssEvent;

    fn decode(text: &str) -> Result<Option<Self::Event>, Box<ExchangeError>> {
        // Subscription ack: {"result":null,"id":1}
        let v: serde_json::Value = serde_json::from_str(text)
            .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;

        let Some(event_type) = v.get("e").and_then(|e| e.as_str()) else {
            // Control frame (subscription ack, error, etc.) — skip.
            return Ok(None);
        };

        match event_type {
            "depthUpdate" => {
                let update: BinanceDepthUpdate = serde_json::from_value(v)
                    .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;
                Ok(Some(BinanceWssEvent::DepthUpdate(update)))
            }
            "trade" => {
                let trade: BinanceTradeData = serde_json::from_value(v)
                    .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;
                Ok(Some(BinanceWssEvent::TradeData(trade)))
            }
            _ => {
                tracing::trace!(
                    event_type = event_type,
                    "binance_decoder.unknown_event_type"
                );
                Ok(None)
            }
        }
    }
}
