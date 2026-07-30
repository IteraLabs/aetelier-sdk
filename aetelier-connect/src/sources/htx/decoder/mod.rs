//! HTX WebSocket decoder.
//!
//! Routes already-inflated text frames by `rep` / `ch` to produce
//! [`HtxWssEvent`] variants.

use crate::clients::wss::WssDecoder;
use crate::errors::ExchangeError;
use crate::sources::htx::events::HtxWssEvent;

/// Receives **already-inflated** text (the transport gunzips first).
/// Dispatches on `rep` (REQ reply) / `ch` (channel suffix); subscribe acks
/// (`{"status":"ok","subbed":…}`) yield `Ok(None)`. The `{"ping":…}` control
/// frame never reaches here — `on_inbound_control` replies first.
pub struct HtxDecoder;

impl WssDecoder for HtxDecoder {
    type Event = HtxWssEvent;

    fn decode(text: &str) -> Result<Option<Self::Event>, Box<ExchangeError>> {
        let v: serde_json::Value = serde_json::from_str(text)
            .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;

        if let Some(rep) = v.get("rep").and_then(|r| r.as_str()) {
            if rep.contains(".mbp.") {
                let s = serde_json::from_value(v)
                    .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;
                return Ok(Some(HtxWssEvent::MbpSnapshot(s)));
            }
            return Ok(None);
        }

        match v.get("ch").and_then(|c| c.as_str()) {
            Some(ch) if ch.contains(".mbp.") => {
                let u = serde_json::from_value(v)
                    .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;
                Ok(Some(HtxWssEvent::MbpUpdate(u)))
            }
            Some(ch) if ch.ends_with(".trade.detail") => {
                let t = serde_json::from_value(v)
                    .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;
                Ok(Some(HtxWssEvent::Trade(t)))
            }
            _ => Ok(None),
        }
    }
}
