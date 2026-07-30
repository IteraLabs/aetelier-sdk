use aetelier_types::orderbooks::delta::NormalizedDelta;
use serde::Deserialize;

/// Top-level level2 WebSocket message from Coinbase Advanced Trade.
///
/// ```json
/// {
///   "channel": "l2_data",
///   "timestamp": "2023-02-09T20:32:50.714964855Z",
///   "sequence_num": 0,
///   "events": [{ "type": "snapshot", "product_id": "BTC-USD", "updates": [...] }]
/// }
/// ```
#[derive(Deserialize, Debug, Clone)]
pub struct CoinbaseOrderbookResponse {
    pub channel: String,
    pub timestamp: String,
    pub sequence_num: u64,
    pub events: Vec<CoinbaseL2Event>,
}

/// A single level2 event (snapshot or update) within the response.
#[derive(Deserialize, Debug, Clone)]
pub struct CoinbaseL2Event {
    /// `"snapshot"` or `"update"`.
    #[serde(rename = "type")]
    pub ty: String,
    pub product_id: String,
    pub updates: Vec<CoinbaseL2Update>,
}

/// Individual price-level update in a level2 message.
///
/// `new_quantity` of `"0"` means remove the level.
#[derive(Deserialize, Debug, Clone)]
pub struct CoinbaseL2Update {
    pub side: String,
    pub event_time: String,
    pub price_level: String,
    pub new_quantity: String,
}

impl CoinbaseOrderbookResponse {
    /// Parse the envelope ISO 8601 `timestamp` to UTC epoch microseconds (0 if it
    /// fails to parse).
    pub fn timestamp_us(&self) -> u64 {
        chrono::DateTime::parse_from_rfc3339(&self.timestamp)
            .map(|dt| dt.timestamp_micros() as u64)
            .unwrap_or(0)
    }

    /// Convert to exchange-agnostic [`NormalizedDelta`] for the first event.
    ///
    /// Coinbase level2 messages may contain multiple events; this converts
    /// the first one. For multi-product subscriptions on a single connection,
    /// the caller should iterate `events` directly.
    pub fn to_normalized(&self) -> Option<NormalizedDelta> {
        let event = self.events.first()?;

        let mut bids = Vec::new();
        let mut asks = Vec::new();

        for update in &event.updates {
            let pair = (update.price_level.clone(), update.new_quantity.clone());
            match update.side.as_str() {
                "bid" => bids.push(pair),
                "offer" | "ask" => asks.push(pair),
                _ => {}
            }
        }

        Some(NormalizedDelta {
            symbol: event.product_id.clone(),
            bids,
            asks,
            update_id: self.sequence_num,
            sequence: self.sequence_num,
            source_orderbook_ts_us: self.timestamp_us(),
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            checksum: None,
            orders: Vec::new(),
            is_snapshot: event.ty == "snapshot",
        })
    }
}
