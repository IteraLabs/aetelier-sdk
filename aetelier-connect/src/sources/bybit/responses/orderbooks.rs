use aetelier_types::orderbooks::delta::NormalizedDelta;
use aetelier_types::orderbooks::target::OrderbookUpdateType;
use serde::Deserialize;

/// Bybit-specific type alias for backwards compatibility
pub type BybitOrderbookType = OrderbookUpdateType;

/// Bybit orderbook WebSocket response
///
/// Example message:
/// ```json
/// {
///   "topic": "orderbook.50.BTCUSDT",
///   "type": "snapshot",
///   "ts": 1672304484978,
///   "data": { "s": "BTCUSDT", "b": [...], "a": [...], "u": 18521288, "seq": 7961638724 },
///   "cts": 1672304484998
/// }
/// ```
#[derive(Deserialize, Debug, Clone)]
pub struct BybitOrderbookResponse {
    /// Topic string: "orderbook.{depth}.{symbol}"
    pub topic: String,
    /// Message type: "snapshot" or "delta"
    #[serde(rename = "type")]
    pub ty: String,
    /// Orderbook timestamp (exchange time in milliseconds)
    #[serde(rename = "ts")]
    pub orderbook_ts_ms: u64,
    /// Orderbook data payload (single object)
    pub data: BybitOrderbookData,
    /// Cross timestamp (optional, for latency measurement)
    #[serde(default)]
    pub cts: Option<u64>,
}

/// Bybit orderbook data
#[derive(Deserialize, Debug, Clone)]
pub struct BybitOrderbookData {
    /// Symbol
    #[serde(rename = "s")]
    pub symbol: String,
    /// Bids: [[price, size], ...] sorted by price descending
    #[serde(rename = "b")]
    pub bids: Vec<BybitPriceLevel>,
    /// Asks: [[price, size], ...] sorted by price ascending
    #[serde(rename = "a")]
    pub asks: Vec<BybitPriceLevel>,
    /// Update ID (used for sequencing deltas)
    #[serde(rename = "u")]
    pub update_id: u64,
    /// Cross sequence (global sequence number)
    #[serde(rename = "seq")]
    pub sequence: u64,
}

/// Price level as tuple: ["price", "size"]
///
/// Bybit sends price levels as two-element arrays of strings:
/// `["16493.50", "0.006"]`
#[derive(Deserialize, Debug, Clone, PartialEq)]
pub struct BybitPriceLevel(pub String, pub String);

impl BybitPriceLevel {
    /// Create a new price level
    #[inline]
    pub fn new(price: impl Into<String>, size: impl Into<String>) -> Self {
        Self(price.into(), size.into())
    }

    /// Get price as f64
    #[inline]
    pub fn price(&self) -> f64 {
        self.0.parse().unwrap_or(0.0)
    }

    /// Get size as f64
    #[inline]
    pub fn size(&self) -> f64 {
        self.1.parse().unwrap_or(0.0)
    }

    /// Get raw price string (zero-copy)
    #[inline]
    pub fn price_str(&self) -> &str {
        &self.0
    }

    /// Get raw size string (zero-copy)
    #[inline]
    pub fn size_str(&self) -> &str {
        &self.1
    }

    /// Check if this level is a deletion (size == "0")
    #[inline]
    pub fn is_deletion(&self) -> bool {
        self.1 == "0" || self.1 == "0.0" || self.1 == "0.00"
    }
}

impl BybitOrderbookData {
    /// Parse the update type from a type string
    #[inline]
    pub fn update_type(ty: &str) -> OrderbookUpdateType {
        match ty {
            "snapshot" => OrderbookUpdateType::Snapshot,
            _ => OrderbookUpdateType::Delta,
        }
    }

    /// Get the best bid (highest price)
    #[inline]
    pub fn best_bid(&self) -> Option<&BybitPriceLevel> {
        self.bids.first()
    }

    /// Get the best ask (lowest price)
    #[inline]
    pub fn best_ask(&self) -> Option<&BybitPriceLevel> {
        self.asks.first()
    }

    /// Calculate mid price
    pub fn mid_price(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some((bid.price() + ask.price()) / 2.0),
            _ => None,
        }
    }

    /// Calculate spread
    pub fn spread(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some(ask.price() - bid.price()),
            _ => None,
        }
    }

    /// Calculate spread in basis points
    pub fn spread_bps(&self) -> Option<f64> {
        let spread = self.spread()?;
        let mid = self.mid_price()?;
        if mid > 0.0 {
            Some((spread / mid) * 10_000.0)
        } else {
            None
        }
    }

    /// Total bid volume
    pub fn total_bid_volume(&self) -> f64 {
        self.bids.iter().map(|l| l.size()).sum()
    }

    /// Total ask volume
    pub fn total_ask_volume(&self) -> f64 {
        self.asks.iter().map(|l| l.size()).sum()
    }

    /// Volume imbalance ∈ [-1, 1]
    pub fn volume_imbalance(&self) -> Option<f64> {
        let bid = self.total_bid_volume();
        let ask = self.total_ask_volume();
        let total = bid + ask;
        if total > 0.0 {
            Some((bid - ask) / total)
        } else {
            None
        }
    }
}

impl BybitOrderbookResponse {
    /// Parse the message type
    #[inline]
    pub fn message_type(&self) -> OrderbookUpdateType {
        if self.ty == "snapshot" {
            OrderbookUpdateType::Snapshot
        } else {
            OrderbookUpdateType::Delta
        }
    }

    /// Check if this is a snapshot
    #[inline]
    pub fn is_snapshot(&self) -> bool {
        self.ty == "snapshot"
    }

    /// Check if this is a delta
    #[inline]
    pub fn is_delta(&self) -> bool {
        self.ty == "delta"
    }

    /// Extract symbol from topic: "orderbook.{depth}.{symbol}"
    #[inline]
    pub fn symbol(&self) -> Option<&str> {
        self.topic.split('.').nth(2)
    }

    /// Extract depth from topic
    #[inline]
    pub fn depth(&self) -> Option<u32> {
        self.topic.split('.').nth(1)?.parse().ok()
    }

    /// Shortcut to update_id
    #[inline]
    pub fn update_id(&self) -> u64 {
        self.data.update_id
    }

    /// Shortcut to sequence
    #[inline]
    pub fn sequence(&self) -> u64 {
        self.data.sequence
    }

    /// Exchange's name
    pub fn exchange_name(&self) -> String {
        "Bybit".to_string()
    }

    /// Convert to an exchange-agnostic [`NormalizedDelta`].
    ///
    /// Returns `None` if the data payload is empty (defensive guard;
    /// should not happen with valid Bybit messages).  This matches
    /// the return type of `CoinbaseOrderbookResponse::to_normalized()`
    /// and `KrakenBookResponse::to_normalized()`.
    pub fn to_normalized(&self) -> Option<NormalizedDelta> {
        if self.data.bids.is_empty()
            && self.data.asks.is_empty()
            && self.data.symbol.is_empty()
        {
            return None;
        }
        Some(NormalizedDelta {
            symbol: self.data.symbol.clone(),
            bids: self
                .data
                .bids
                .iter()
                .map(|pl| (pl.0.clone(), pl.1.clone()))
                .collect(),
            asks: self
                .data
                .asks
                .iter()
                .map(|pl| (pl.0.clone(), pl.1.clone()))
                .collect(),
            update_id: self.data.update_id,
            sequence: self.data.sequence,
            // Prefer the matching-engine cross timestamp; fall back to the
            // message timestamp (both venue ms; converted to the platform's
            // UTC epoch microseconds here at the wire boundary).
            source_orderbook_ts_us: self.cts.unwrap_or(self.orderbook_ts_ms) * 1_000,
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            checksum: None,
            orders: Vec::new(),
            is_snapshot: self.ty == "snapshot",
        })
    }
}
