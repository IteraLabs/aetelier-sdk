use serde::Deserialize;

/// Envelope for the `allLiquidation.*` Bybit WebSocket topic.
///
/// ```json
/// {
///   "topic": "allLiquidation.BTCUSDT",
///   "type": "snapshot",
///   "ts": 1672304484978,
///   "data": [{ "T": 1672304484978, "s": "BTCUSDT", ... }]
/// }
/// ```
#[derive(Deserialize, Debug, Clone)]
pub struct BybitLiquidationResponse {
    /// Topic string: `"allLiquidation.{symbol}"`.
    pub topic: String,
    /// Message type (always `"snapshot"` for liquidations).
    #[serde(rename = "type")]
    pub ty: String,
    /// Server timestamp (Unix ms).
    pub ts: u64,
    /// One or more liquidation events in this frame.
    pub data: Vec<BybitLiquidationData>,
}

/// A single forced-liquidation event from the `allLiquidation.*` topic.
///
/// When a position's margin ratio drops below the maintenance requirement
/// the exchange force-closes the position and broadcasts this event.
#[derive(Deserialize, Debug, Clone)]
pub struct BybitLiquidationData {
    /// Liquidation timestamp (Unix ms, exchange-reported).
    #[serde(rename = "T")]
    pub liquidation_ts_ms: u64,
    /// Trading pair symbol (e.g. `"BTCUSDT"`).
    #[serde(rename = "s")]
    pub symbol: String,
    /// Side being liquidated: `"Buy"` or `"Sell"`.
    #[serde(rename = "S")]
    pub side: String,
    /// Liquidated quantity as a string.
    #[serde(rename = "v")]
    pub amount: String,
    /// Bankruptcy / fill price as a string.
    #[serde(rename = "p")]
    pub price: String,
}
