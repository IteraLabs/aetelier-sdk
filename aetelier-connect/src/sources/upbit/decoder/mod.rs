//! Upbit WebSocket decoder.

use crate::clients::wss::WssDecoder;
use crate::errors::ExchangeError;
use crate::sources::upbit::events::UpbitWssEvent;

/// Dispatches on the `"type"` field; status frames (`{"status":"UP"}`) and
/// the `"PONG"` keep-alive reply yield `Ok(None)`.
pub struct UpbitDecoder;

impl WssDecoder for UpbitDecoder {
    type Event = UpbitWssEvent;

    fn decode(text: &str) -> Result<Option<Self::Event>, Box<ExchangeError>> {
        let v: serde_json::Value = serde_json::from_str(text)
            .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;
        match v.get("type").and_then(|t| t.as_str()) {
            Some("orderbook") => {
                let ob = serde_json::from_value(v)
                    .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;
                Ok(Some(UpbitWssEvent::Orderbook(ob)))
            }
            Some("trade") => {
                let tr = serde_json::from_value(v)
                    .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;
                Ok(Some(UpbitWssEvent::Trade(tr)))
            }
            _ => Ok(None),
        }
    }
}
