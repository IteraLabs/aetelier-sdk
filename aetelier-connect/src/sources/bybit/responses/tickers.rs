use serde::Deserialize;

/// Envelope for the `tickers.*` Bybit WebSocket topic.
///
/// Unlike trade and liquidation messages the `data` field is a single
/// object (not an array) because the exchange pushes incremental
/// snapshots of the full ticker state.
///
/// ```json
/// {
///   "topic": "tickers.BTCUSDT",
///   "type": "snapshot",
///   "cs": 66921506596,
///   "ts": 1672304484978,
///   "data": { "symbol": "BTCUSDT", ... }
/// }
/// ```
#[derive(Deserialize, Debug, Clone)]
pub struct BybitTickerResponse {
    /// Topic string: `"tickers.{symbol}"`.
    pub topic: String,
    /// Message type: `"snapshot"` or `"delta"`.
    #[serde(rename = "type")]
    pub ty: String,
    /// Cross-sequence number for ordering across topics.
    pub cs: i64,
    /// Server timestamp (Unix ms).
    pub ts: u64,
    /// Ticker payload (single object, not an array).
    pub data: BybitTickerData,
}

/// Ticker / instrument snapshot from the `tickers.*` topic.
///
/// Bybit pushes partial updates: only fields that changed since the
/// previous frame are present, hence almost every field is `Option`.
/// The `symbol` field is the exception — it is always populated.
///
/// Numeric values are string-encoded to preserve exchange precision.
/// Consumers should parse with `Decimal` or `f64` as appropriate.
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BybitTickerData {
    /// Ticker response timestamp (Unix ms, exchange-reported).
    pub ts: Option<u64>,
    /// Trading pair symbol (always present, e.g. `"BTCUSDT"`).
    pub symbol: String,
    /// Last tick direction relative to the previous trade.
    pub tick_direction: Option<TickDirection>,
    /// 24 h price change percentage (e.g. `"0.0125"` = 1.25 %).
    pub price_24h_pcnt: Option<String>,
    /// Last traded price.
    pub last_price: Option<String>,
    /// Price 24 hours ago.
    pub prev_price_24h: Option<String>,
    /// 24 h high price.
    pub high_price_24h: Option<String>,
    /// 24 h low price.
    pub low_price_24h: Option<String>,
    /// Price 1 hour ago.
    pub prev_price_1h: Option<String>,
    /// Mark price (used for margining / liquidation).
    pub mark_price: Option<String>,
    /// Underlying index price.
    pub index_price: Option<String>,
    /// Open interest in contract units.
    pub open_interest: Option<String>,
    /// Open interest in quote-currency value.
    pub open_interest_value: Option<String>,
    /// 24 h turnover in quote currency.
    pub turnover_24h: Option<String>,
    /// 24 h trading volume in base currency.
    pub volume_24h: Option<String>,
    /// Next funding settlement time (Unix ms string).
    pub next_funding_time: Option<String>,
    /// Predicted / current funding rate (decimal string, e.g. `"0.0001"`).
    pub funding_rate: Option<String>,
    /// Best bid price.
    pub bid1_price: Option<String>,
    /// Best bid size.
    pub bid1_size: Option<String>,
    /// Best ask price.
    pub ask1_price: Option<String>,
    /// Best ask size.
    pub ask1_size: Option<String>,
    /// Delivery time for futures contracts (Unix ms).
    pub delivery_time: Option<u64>,
    /// Basis rate for futures contracts.
    pub basis_rate: Option<String>,
    /// Delivery fee rate.
    pub delivery_fee_rate: Option<String>,
    /// Predicted delivery price.
    pub predicted_delivery_price: Option<String>,
    /// Pre-open (auction) price for pre-listing instruments.
    pub pre_open_price: Option<String>,
    /// Pre-open quantity.
    pub pre_qty: Option<String>,
    /// Current pre-listing auction phase.
    pub cur_pre_listing_phase: Option<CurPreListingPhase>,
    /// Funding interval in hours (e.g. `"8"` for 8 h).
    pub funding_interval_hour: Option<String>,
    /// Funding rate cap.
    pub funding_cap: Option<String>,
    /// Annualised basis rate.
    pub basis_rate_year: Option<String>,
}

/// Direction of the most recent price tick.
///
/// Used by the exchange to indicate whether the last trade moved the
/// price up, down, or remained flat relative to the prior fill.
#[derive(Deserialize, Debug, Clone)]
pub enum TickDirection {
    /// Price moved up.
    PlusTick,
    /// Price unchanged after a previous up-tick.
    ZeroPlusTick,
    /// Price moved down.
    MinusTick,
    /// Price unchanged after a previous down-tick.
    ZeroMinusTick,
}

/// Pre-listing auction phase for newly-listed instruments.
///
/// Bybit instruments go through a series of auction phases before
/// continuous trading begins.  An empty string is the default when
/// the instrument is already in normal trading mode.
#[derive(Default, Deserialize, Debug, Clone)]
pub enum CurPreListingPhase {
    /// Normal trading — no active auction phase.
    #[default]
    #[serde(rename = "")]
    Empty,
    /// Auction has not started yet.
    NotStarted,
    /// Auction has finished.
    Finished,
    /// Open call-auction period (orders can be placed and cancelled).
    CallAuction,
    /// Call-auction period where cancellation is no longer allowed.
    CallAuctionNoCancel,
    /// Matching phase between auction and continuous trading.
    CrossMatching,
    /// Normal continuous trading.
    ContinuousTrading,
}
