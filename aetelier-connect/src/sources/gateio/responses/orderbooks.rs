use serde::Deserialize;

/// Gate.io `spot.order_book` WebSocket response (full limited-depth
/// snapshot pushed at a fixed interval).
///
/// ```json
/// {
///   "time": 1606295412, "time_ms": 1606295412213,
///   "channel": "spot.order_book", "event": "update",
///   "result": {
///     "t": 1606295412123, "lastUpdateId": 48791820, "s": "BTC_USDT",
///     "bids": [["19079.55","0.0195"]], "asks": [["19080.24","0.1638"]]
///   }
/// }
/// ```
#[derive(Deserialize, Debug, Clone)]
pub struct GateioOrderbookResponse {
    /// Server timestamp (Unix milliseconds); present on data frames.
    #[serde(default)]
    pub time_ms: Option<u64>,
    /// Channel name (`"spot.order_book"`).
    pub channel: String,
    /// `"update"` for data frames.
    pub event: String,
    /// Order-book payload.
    pub result: GateioOrderbookData,
}

/// The `result` payload of a `spot.order_book` frame.
#[derive(Deserialize, Debug, Clone)]
pub struct GateioOrderbookData {
    /// Book timestamp, Unix milliseconds.
    #[serde(rename = "t", default)]
    pub ts_ms: u64,
    /// Monotonic book id (shared id-space with the diff stream / REST).
    #[serde(rename = "lastUpdateId", default)]
    pub last_update_id: i64,
    /// Currency pair (e.g. `"BTC_USDT"`).
    #[serde(rename = "s")]
    pub symbol: String,
    /// Bid levels (`[price, amount]` strings).
    #[serde(default)]
    pub bids: Vec<GateioLevel>,
    /// Ask levels (`[price, amount]` strings).
    #[serde(default)]
    pub asks: Vec<GateioLevel>,
}

/// A Gate.io price level: a 2-element array of strings `[price, amount]`.
#[derive(Deserialize, Debug, Clone, PartialEq)]
pub struct GateioLevel(pub String, pub String);

impl GateioLevel {
    /// Price as `f64` (`0.0` on parse failure).
    #[inline]
    pub fn price(&self) -> f64 {
        self.0.parse().unwrap_or(0.0)
    }

    /// Size/amount as `f64` (`0.0` on parse failure).
    #[inline]
    pub fn size(&self) -> f64 {
        self.1.parse().unwrap_or(0.0)
    }

    /// Raw price string (zero-copy).
    #[inline]
    pub fn price_str(&self) -> &str {
        &self.0
    }

    /// Raw size string (zero-copy).
    #[inline]
    pub fn size_str(&self) -> &str {
        &self.1
    }
}
