use crate::clients::wss::WssDecoder;
use crate::errors::ExchangeError;
use crate::sources::hyperliquid::events::HyperliquidWssEvent;

pub struct HyperliquidDecoder;

impl WssDecoder for HyperliquidDecoder {
    type Event = HyperliquidWssEvent;

    fn decode(text: &str) -> Result<Option<Self::Event>, Box<ExchangeError>> {
        let mut v: serde_json::Value = serde_json::from_str(text)
            .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;
        match v.get("channel").and_then(|c| c.as_str()) {
            Some("l2Book") => {
                let data = v
                    .get_mut("data")
                    .map(serde_json::Value::take)
                    .unwrap_or(serde_json::Value::Null);
                let book = serde_json::from_value(data)
                    .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;
                Ok(Some(HyperliquidWssEvent::Book(book)))
            }
            Some("trades") => {
                let data = v
                    .get_mut("data")
                    .map(serde_json::Value::take)
                    .unwrap_or(serde_json::Value::Null);
                let trades = serde_json::from_value(data)
                    .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;
                Ok(Some(HyperliquidWssEvent::Trades(trades)))
            }
            Some("activeAssetCtx") => {
                let data = v
                    .get_mut("data")
                    .map(serde_json::Value::take)
                    .unwrap_or(serde_json::Value::Null);
                let msg = serde_json::from_value(data)
                    .map_err(|e| Box::new(ExchangeError::JsonError(e)))?;
                Ok(Some(HyperliquidWssEvent::AssetCtx(msg)))
            }
            _ => Ok(None),
        }
    }
}
