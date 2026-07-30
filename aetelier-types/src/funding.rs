//! Funding rate data structures.
//!
//! Funding rates are periodic payments between long and short position holders
//! in perpetual futures markets. A positive rate means longs pay shorts; negative
//! means shorts pay longs.

use crate::errors::BuildError;
use serde::{Deserialize, Serialize};

use crate::trading_pair::TradingPair;

/// A single funding rate observation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingRate {
    /// Timestamp when this rate was observed (Unix µs).
    pub funding_rate_ts_us: u64,
    /// Canonical trading pair (e.g. `SOL/USDT`).
    pub pair: TradingPair,
    /// The funding rate as a decimal (e.g. 0.0001 = 1 bps).
    pub funding_rate: f64,
    /// Next funding settlement timestamp (Unix µs). 0 if unknown.
    pub next_funding_ts_us: u64,
    /// Exchange name.
    pub exchange: String,
}

impl FundingRate {
    /// Create a new [`FundingRateBuilder`].
    pub fn builder() -> FundingRateBuilder {
        FundingRateBuilder::new()
    }
}

/// Builder for constructing a [`FundingRate`] with validated fields.
///
/// Required fields: `funding_rate_ts_us`, `symbol`, `funding_rate`,
/// `exchange`.  `next_funding_ts_us` defaults to `0` when omitted.
#[derive(Debug, Clone)]
pub struct FundingRateBuilder {
    funding_rate_ts_us: Option<u64>,
    pair: Option<TradingPair>,
    funding_rate: Option<f64>,
    next_funding_ts_us: Option<u64>,
    exchange: Option<String>,
}

impl Default for FundingRateBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl FundingRateBuilder {
    /// Create an empty builder with all fields set to `None`.
    pub fn new() -> Self {
        FundingRateBuilder {
            funding_rate_ts_us: None,
            pair: None,
            funding_rate: None,
            next_funding_ts_us: None,
            exchange: None,
        }
    }

    /// Set the observation timestamp (Unix ms).
    pub fn funding_rate_ts_us(mut self, funding_rate_ts_us: u64) -> Self {
        self.funding_rate_ts_us = Some(funding_rate_ts_us);
        self
    }

    /// Set the canonical trading pair.
    pub fn pair(mut self, pair: TradingPair) -> Self {
        self.pair = Some(pair);
        self
    }

    /// Set the funding rate as a decimal (e.g. `0.0001` = 1 bps).
    pub fn funding_rate(mut self, rate: f64) -> Self {
        self.funding_rate = Some(rate);
        self
    }

    /// Set the next funding settlement timestamp (Unix ms, optional, defaults to `0`).
    pub fn next_funding_ts_us(mut self, ts: u64) -> Self {
        self.next_funding_ts_us = Some(ts);
        self
    }

    /// Set the exchange name (e.g. `"bybit"`).
    pub fn exchange(mut self, exchange: String) -> Self {
        self.exchange = Some(exchange);
        self
    }

    /// Consume the builder and produce a [`FundingRate`].
    ///
    /// # Errors
    ///
    /// Returns [`BuildError`] if any required field is missing.
    /// `next_funding_ts_us` defaults to `0` when not set.
    pub fn build(self) -> Result<FundingRate, BuildError> {
        let funding_rate_ts_us = self
            .funding_rate_ts_us
            .ok_or(BuildError::MissingField("ts"))?;
        let pair = self.pair.ok_or(BuildError::MissingField("pair"))?;
        let funding_rate = self
            .funding_rate
            .ok_or(BuildError::MissingField("funding_rate"))?;
        let next_funding_ts_us = self.next_funding_ts_us.unwrap_or(0);
        let exchange = self.exchange.ok_or(BuildError::MissingField("exchange"))?;

        Ok(FundingRate {
            funding_rate_ts_us,
            pair,
            funding_rate,
            next_funding_ts_us,
            exchange,
        })
    }
}
