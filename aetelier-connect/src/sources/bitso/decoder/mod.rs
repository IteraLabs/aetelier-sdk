//! Bitso WebSocket decoder.
//!
//! Routes incoming JSON messages by the `"type"` field to produce
//! [`BitsoWssEvent`] variants.

use crate::clients::wss::WssDecoder;
use crate::errors::ExchangeError;
use crate::sources::bitso::events::BitsoWssEvent;

/// Dispatches on `"type"`; keep-alives (`{"type":"ka"}`) and subscription
/// acks (`{"action":"subscribe","response":"ok"}`) yield `Ok(None)`.
pub struct BitsoDecoder;

impl WssDecoder for BitsoDecoder {
    type Event = BitsoWssEvent;

    fn decode(text: &str) -> Result<Option<Self::Event>, Box<ExchangeError>> {
        let v: serde_json::Value = serde_json::from_str(text)
            .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;
        match v.get("type").and_then(|t| t.as_str()) {
            Some("diff-orders") => {
                // A subscription ack also carries type:"diff-orders" but no
                // payload array — guard on payload presence.
                if v.get("payload").map(|p| p.is_array()) != Some(true) {
                    return Ok(None);
                }
                let m = serde_json::from_value(v)
                    .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;
                Ok(Some(BitsoWssEvent::DiffOrders(m)))
            }
            Some("trades") => {
                if v.get("payload").map(|p| p.is_array()) != Some(true) {
                    return Ok(None);
                }
                let m = serde_json::from_value(v)
                    .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;
                Ok(Some(BitsoWssEvent::Trades(m)))
            }
            _ => Ok(None),
        }
    }
}
