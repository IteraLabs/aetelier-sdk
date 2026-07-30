use aetelier_types::orderbooks::delta::NormalizedDelta;
use serde::{Deserialize, Deserializer};

/// Capture a JSON number's exact wire token as a string, preserving the
/// precision (trailing zeros) the order-book checksum depends on. `f64` would
/// drop them (`0.01300000` → `0.013`).
fn de_num_str<'de, D: Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    let raw: Box<serde_json::value::RawValue> = Deserialize::deserialize(d)?;
    Ok(raw.get().to_string())
}

/// Top-level `book` channel message from Kraken WebSocket v2.
///
/// ```json
/// {
///   "channel": "book",
///   "type": "snapshot",
///   "data": [{
///     "symbol": "BTC/USD",
///     "bids": [{"price": 21921.73, "qty": 0.063}],
///     "asks": [{"price": 21922.00, "qty": 0.500}],
///     "checksum": 2439117997,
///     "timestamp": "2023-09-26T16:49:20.962586Z"
///   }]
/// }
/// ```
#[derive(Deserialize, Debug, Clone)]
pub struct KrakenBookResponse {
    pub channel: String,
    /// `"snapshot"` or `"update"`.
    #[serde(rename = "type")]
    pub ty: String,
    pub data: Vec<KrakenBookData>,
}

/// A single book snapshot/update payload within the `data` array.
#[derive(Deserialize, Debug, Clone)]
pub struct KrakenBookData {
    pub symbol: String,
    pub bids: Vec<KrakenPriceLevel>,
    pub asks: Vec<KrakenPriceLevel>,
    /// CRC32 checksum of top-10 bids/asks for integrity validation.
    #[serde(default)]
    pub checksum: u64,
    pub timestamp: String,
}

/// Individual price level in a Kraken book message.
///
/// Kraken sends `price` and `qty` as JSON numbers; they are kept as their exact
/// wire-token strings so the checksum's precision is preserved.
#[derive(Deserialize, Debug, Clone)]
pub struct KrakenPriceLevel {
    #[serde(deserialize_with = "de_num_str")]
    pub price: String,
    #[serde(deserialize_with = "de_num_str")]
    pub qty: String,
}

impl KrakenBookResponse {
    /// Convert to exchange-agnostic [`NormalizedDelta`] for the first data entry.
    ///
    /// Kraken book messages typically contain a single data entry per symbol.
    pub fn to_normalized(&self) -> Option<NormalizedDelta> {
        let data = self.data.first()?;

        let bids: Vec<(String, String)> = data
            .bids
            .iter()
            .map(|l| (l.price.clone(), l.qty.clone()))
            .collect();

        let asks: Vec<(String, String)> = data
            .asks
            .iter()
            .map(|l| (l.price.clone(), l.qty.clone()))
            .collect();

        Some(NormalizedDelta {
            symbol: data.symbol.clone(),
            bids,
            asks,
            update_id: data.checksum,
            sequence: data.checksum,
            // Kraken book entries carry an ISO 8601 `timestamp`.
            source_orderbook_ts_us: chrono::DateTime::parse_from_rfc3339(&data.timestamp)
                .map(|dt| dt.timestamp_micros() as u64)
                .unwrap_or(0),
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            checksum: Some(data.checksum as i64),
            orders: Vec::new(),
            is_snapshot: self.ty == "snapshot",
        })
    }
}
