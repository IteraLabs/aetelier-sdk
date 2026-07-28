//! KuCoin WSS decoder.
//!
//! Routes data `message` frames by their `topic` prefix into
//! [`KucoinWssEvent`] variants; lifecycle frames are silently consumed.

use crate::clients::wss::WssDecoder;
use crate::errors::ExchangeError;
use crate::sources::kucoin::events::KucoinWssEvent;

/// Dispatches data `message` frames on the `topic` prefix; lifecycle frames
/// (`welcome`/`ack`/`pong`) yield `Ok(None)`.
pub struct KucoinDecoder;

impl WssDecoder for KucoinDecoder {
    type Event = KucoinWssEvent;

    fn decode(text: &str) -> Result<Option<Self::Event>, Box<ExchangeError>> {
        let v: serde_json::Value = serde_json::from_str(text)
            .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;
        if v.get("type").and_then(|t| t.as_str()) != Some("message") {
            return Ok(None);
        }
        let topic = v.get("topic").and_then(|t| t.as_str()).unwrap_or_default();
        let data = match v.get("data") {
            Some(d) => d.clone(),
            None => return Ok(None),
        };
        if topic.starts_with("/market/level2") {
            let d = serde_json::from_value(data)
                .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;
            Ok(Some(KucoinWssEvent::Level2(d)))
        } else if topic.starts_with("/market/match") {
            let d = serde_json::from_value(data)
                .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;
            Ok(Some(KucoinWssEvent::Match(d)))
        } else {
            Ok(None)
        }
    }
}
