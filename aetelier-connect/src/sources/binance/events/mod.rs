use super::responses::orderbooks::{BinanceDepthSnapshot, BinanceDepthUpdate};
use super::responses::trades::BinanceTradeData;

/// Binance-specific WebSocket event variants.
///
/// The decoder maps each incoming frame to one of these variants.
/// `DepthSnapshot` is NOT produced by the decoder — it is synthesised
/// by the `BookInitializer` pipeline stage from a REST response.
#[derive(Debug, Clone)]
pub enum BinanceWssEvent {
    /// Incremental depth update from the `<symbol>@depth@100ms` stream.
    DepthUpdate(BinanceDepthUpdate),
    /// Full depth snapshot from REST `GET /api/v3/depth`.
    /// Only produced by `BookInitializer`, never by the decoder.
    DepthSnapshot(BinanceDepthSnapshot),
    /// Individual trade from the `<symbol>@trade` stream.
    TradeData(BinanceTradeData),
}
