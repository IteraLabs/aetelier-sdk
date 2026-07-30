//! Bitget V2 WebSocket decoder.
//!
//! Routes incoming frames by `arg.channel` to produce [`BitgetWssEvent`]
//! variants; control frames and the literal `"pong"` reply are swallowed.

use crate::clients::wss::WssDecoder;
use crate::errors::ExchangeError;
use crate::sources::bitget::events::BitgetWssEvent;

/// Dispatches on `arg.channel`. Control frames — subscribe/error events
/// (`{"event":"subscribe"|"error",…}`) and the literal `"pong"` keep-alive
/// reply — yield `Ok(None)`.
pub struct BitgetDecoder;

impl WssDecoder for BitgetDecoder {
    type Event = BitgetWssEvent;

    fn decode(text: &str) -> Result<Option<Self::Event>, Box<ExchangeError>> {
        // The keep-alive reply is the bare string "pong", not JSON.
        if text == "pong" {
            return Ok(None);
        }
        let v: serde_json::Value = serde_json::from_str(text)
            .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;

        // Subscribe acks / errors carry an "event" field, no data dispatch.
        if v.get("event").is_some() {
            return Ok(None);
        }

        match v
            .get("arg")
            .and_then(|a| a.get("channel"))
            .and_then(|c| c.as_str())
        {
            // `books`, `books1`, `books5`, `books15` all reconstruct the same.
            Some(ch) if ch.starts_with("books") => {
                let f = serde_json::from_value(v)
                    .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;
                Ok(Some(BitgetWssEvent::Book(f)))
            }
            Some("trade") => {
                let f = serde_json::from_value(v)
                    .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;
                Ok(Some(BitgetWssEvent::Trade(f)))
            }
            _ => Ok(None),
        }
    }
}
