//! Multi-source market snapshot aggregation.
//!
//! A [`MarketSnapshot`] joins orderbook state, trades, liquidations, funding
//! rates, and open interest at a single grid-aligned timestamp. This is the
//! canonical input to multi-source feature computation.

use crate::{
    funding::FundingRate,
    liquidations::Liquidation,
    open_interest::OpenInterest,
    orderbooks::{Orderbook, decimal_to_f64},
    trades::Trade,
};
use serde::{Deserialize, Serialize};

/// A point-in-time view of the market across all data sources.
///
/// Produced by the [`MarketSynchronizer`](https://docs.rs/aetelier-connect/latest/aetelier_connect/synchronizers/struct.MarketSynchronizer.html) at each grid period. Orderbook,
/// funding rate, and open interest are *state-based* (latest value carried
/// forward). Trades and liquidations are *event-based* (aggregated within
/// the period).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketSnapshot {
    /// Grid-aligned timestamp in UTC epoch microseconds.
    pub ts_us: u64,

    /// Most recent orderbook snapshot (state-based, carried forward).
    pub orderbook: Option<Orderbook>,

    /// All trades that occurred during this period (event-based).
    pub trades: Vec<Trade>,

    /// All liquidations that occurred during this period (event-based).
    pub liquidations: Vec<Liquidation>,

    /// Most recent funding rates (state-based, carried forward).
    pub funding_rate: Vec<FundingRate>,

    /// Most recent open interest values (state-based, carried forward).
    pub open_interest: Vec<OpenInterest>,
}

impl MarketSnapshot {
    /// Create an empty snapshot at the given timestamp.
    pub fn empty(ts_us: u64) -> Self {
        Self {
            ts_us,
            orderbook: None,
            trades: Vec::new(),
            liquidations: Vec::new(),
            funding_rate: Vec::new(),
            open_interest: Vec::new(),
        }
    }

    /// Whether any data source has been populated.
    pub fn has_data(&self) -> bool {
        self.orderbook.is_some()
            || !self.trades.is_empty()
            || !self.liquidations.is_empty()
            || !self.funding_rate.is_empty()
            || !self.open_interest.is_empty()
    }

    /// Total trade volume (sum of amounts) in this period.
    pub fn trade_volume(&self) -> f64 {
        self.trades.iter().map(|t| decimal_to_f64(t.amount)).sum()
    }

    /// Total trade count in this period.
    pub fn trade_count(&self) -> usize {
        self.trades.len()
    }

    /// Total liquidation notional (price * amount) in this period.
    pub fn liquidation_notional(&self) -> f64 {
        self.liquidations
            .iter()
            .map(|l| decimal_to_f64(l.price) * decimal_to_f64(l.amount))
            .sum()
    }
}
