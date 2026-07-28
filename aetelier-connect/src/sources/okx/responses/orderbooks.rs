use serde::Deserialize;

/// The `arg` envelope shared by every OKX data frame.
///
/// ```json
/// "arg": { "channel": "books5", "instId": "BTC-USDT" }
/// ```
#[derive(Deserialize, Debug, Clone)]
pub struct OkxArg {
    /// Channel name (e.g. `"books5"`, `"books"`, `"trades"`).
    pub channel: String,
    /// Instrument id (e.g. `"BTC-USDT"`).
    #[serde(rename = "instId")]
    pub inst_id: String,
}

/// OKX order-book WebSocket response (`books5` / `books` / `bbo-tbt`).
///
/// Example (`books5`, a full top-5 snapshot pushed every 100 ms):
/// ```json
/// {
///   "arg": { "channel": "books5", "instId": "BTC-USDT" },
///   "data": [{
///     "asks": [["31685.1","0.0001","0","1"]],
///     "bids": [["31684.9","0.01","0","1"]],
///     "instId": "BTC-USDT", "ts": "1626537446491", "seqId": 1234567
///   }]
/// }
/// ```
///
/// On the incremental `books` channel an `action` field (`"snapshot"` /
/// `"update"`) is present; `books5` and `bbo-tbt` omit it (every frame is a
/// full snapshot).
#[derive(Deserialize, Debug, Clone)]
pub struct OkxOrderbookResponse {
    /// Channel / instrument envelope.
    pub arg: OkxArg,
    /// `"snapshot"` or `"update"` on the `books` channel; absent on
    /// `books5` / `bbo-tbt`.
    #[serde(default)]
    pub action: Option<String>,
    /// Order-book payload (single element for `books5`).
    pub data: Vec<OkxOrderbookData>,
}

/// A single OKX order-book payload.
#[derive(Deserialize, Debug, Clone)]
pub struct OkxOrderbookData {
    /// Ask levels (ascending by price). `[price, size, "0", numOrders]`.
    #[serde(default)]
    pub asks: Vec<OkxLevel>,
    /// Bid levels (descending by price). `[price, size, "0", numOrders]`.
    #[serde(default)]
    pub bids: Vec<OkxLevel>,
    /// Exchange timestamp, Unix milliseconds as a string.
    pub ts: String,
    /// Current sequence id (a JSON number). `books5` carries one too, but
    /// it does not require gap-tracking.
    #[serde(rename = "seqId", default)]
    pub seq_id: i64,
    /// Previous sequence id — only on the incremental `books` channel
    /// (`-1` on the first snapshot).
    #[serde(rename = "prevSeqId", default)]
    pub prev_seq_id: Option<i64>,
    /// CRC32 checksum — only on the `books` channel (being deprecated by
    /// OKX; do not rely on it).
    #[serde(default)]
    pub checksum: Option<i64>,
}

impl OkxOrderbookData {
    /// Exchange timestamp parsed to Unix milliseconds.
    #[inline]
    pub fn ts_ms(&self) -> u64 {
        self.ts.parse().unwrap_or(0)
    }
}

/// An OKX price level: a 4-element array of strings
/// `[price, size, deprecated, numOrders]`.
///
/// Index 2 is a deprecated "number of liquidated orders" field (always
/// `"0"` for spot); index 3 is the order count at the level.
#[derive(Deserialize, Debug, Clone, PartialEq)]
pub struct OkxLevel(pub String, pub String, pub String, pub String);

impl OkxLevel {
    /// Price as `f64` (`0.0` on parse failure).
    #[inline]
    pub fn price(&self) -> f64 {
        self.0.parse().unwrap_or(0.0)
    }

    /// Size as `f64` (`0.0` on parse failure).
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

impl OkxOrderbookResponse {
    /// `true` if this is a full snapshot frame.
    ///
    /// `books5` frames have no `action` and are always full snapshots, so
    /// the absence of `action` is treated as a snapshot.
    #[inline]
    pub fn is_snapshot(&self) -> bool {
        matches!(self.action.as_deref(), Some("snapshot") | None)
    }

    /// Symbol from the envelope (e.g. `"BTC-USDT"`).
    #[inline]
    pub fn symbol(&self) -> &str {
        &self.arg.inst_id
    }
}
