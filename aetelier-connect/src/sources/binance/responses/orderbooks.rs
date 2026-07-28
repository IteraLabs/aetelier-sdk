use aetelier_types::orderbooks::delta::NormalizedDelta;
use serde::Deserialize;

/// Diff depth stream event from `<symbol>@depth@100ms`.
///
/// Binance WebSocket payload (single-letter field names):
///
/// ```json
/// {
///   "e": "depthUpdate",
///   "E": 1672304484978,
///   "s": "BTCUSDT",
///   "U": 18521288,
///   "u": 18521290,
///   "b": [["21921.73","0.063"]],
///   "a": [["21922.00","0.500"]]
/// }
/// ```
#[derive(Deserialize, Debug, Clone)]
pub struct BinanceDepthUpdate {
    /// Event type — always `"depthUpdate"`.
    #[serde(rename = "e")]
    pub event_type: String,

    /// Event time (Unix ms).
    #[serde(rename = "E")]
    pub event_time: u64,

    /// Symbol (e.g. `"BTCUSDT"`).
    #[serde(rename = "s")]
    pub symbol: String,

    /// First update ID in this event.
    #[serde(rename = "U")]
    pub first_update_id: u64,

    /// Final update ID in this event.
    #[serde(rename = "u")]
    pub last_update_id: u64,

    /// Bid levels to update: `[[price, qty], ...]`.
    #[serde(rename = "b")]
    pub bids: Vec<[String; 2]>,

    /// Ask levels to update: `[[price, qty], ...]`.
    #[serde(rename = "a")]
    pub asks: Vec<[String; 2]>,
}

impl BinanceDepthUpdate {
    /// Convert to exchange-agnostic [`NormalizedDelta`].
    pub fn to_normalized(&self) -> NormalizedDelta {
        NormalizedDelta {
            symbol: self.symbol.clone(),
            bids: self
                .bids
                .iter()
                .map(|l| (l[0].clone(), l[1].clone()))
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|l| (l[0].clone(), l[1].clone()))
                .collect(),
            update_id: self.last_update_id,
            sequence: self.first_update_id,
            source_orderbook_ts_us: crate::framework::model::epoch_to_us(self.event_time),
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            checksum: None,
            orders: Vec::new(),
            is_snapshot: false,
        }
    }
}

/// Full depth snapshot from REST `GET /api/v3/depth?symbol=...&limit=5000`.
///
/// ```json
/// {
///   "lastUpdateId": 18521290,
///   "bids": [["21921.73","0.063"], ...],
///   "asks": [["21922.00","0.500"], ...]
/// }
/// ```
#[derive(Deserialize, Debug, Clone)]
pub struct BinanceDepthSnapshot {
    /// Sequence marker for delta reconciliation.
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,

    /// Full bid book: `[[price, qty], ...]` (descending by price).
    pub bids: Vec<[String; 2]>,

    /// Full ask book: `[[price, qty], ...]` (ascending by price).
    pub asks: Vec<[String; 2]>,
}

impl BinanceDepthSnapshot {
    /// Convert to exchange-agnostic [`NormalizedDelta`] (as a snapshot).
    pub fn to_normalized(&self, symbol: &str) -> NormalizedDelta {
        NormalizedDelta {
            symbol: symbol.to_string(),
            bids: self
                .bids
                .iter()
                .map(|l| (l[0].clone(), l[1].clone()))
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|l| (l[0].clone(), l[1].clone()))
                .collect(),
            update_id: self.last_update_id,
            sequence: self.last_update_id,
            source_orderbook_ts_us: 0, // REST snapshot has no exchange event time
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            checksum: None,
            orders: Vec::new(),
            is_snapshot: true,
        }
    }
}
