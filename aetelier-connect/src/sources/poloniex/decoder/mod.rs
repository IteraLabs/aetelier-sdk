//! Poloniex v2 WebSocket decoder: routes incoming JSON messages by the
//! `"channel"` field to produce [`PoloniexWssEvent`] variants.

use crate::clients::wss::WssDecoder;
use crate::errors::ExchangeError;
use crate::sources::poloniex::events::PoloniexWssEvent;

/// Dispatches on `"channel"`. Subscribe acks (`{"event":"subscribe",…}`),
/// the server pong (`{"event":"pong"}`), and the connection heartbeat
/// (`{"channel":"heartbeat"}`) all yield `Ok(None)`.
pub struct PoloniexDecoder;

impl WssDecoder for PoloniexDecoder {
    type Event = PoloniexWssEvent;

    fn decode(text: &str) -> Result<Option<Self::Event>, Box<ExchangeError>> {
        let v: serde_json::Value = serde_json::from_str(text)
            .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;

        // Control frames: subscribe/unsubscribe acks + the pong reply.
        if v.get("event").is_some() {
            return Ok(None);
        }

        match v.get("channel").and_then(|c| c.as_str()) {
            Some("book_lv2") => {
                let f = serde_json::from_value(v)
                    .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;
                Ok(Some(PoloniexWssEvent::Book(f)))
            }
            Some("trades") => {
                let f = serde_json::from_value(v)
                    .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;
                Ok(Some(PoloniexWssEvent::Trades(f)))
            }
            // "heartbeat" and any other channel are non-data.
            _ => Ok(None),
        }
    }
}
