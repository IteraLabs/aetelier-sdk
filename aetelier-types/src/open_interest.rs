//! Open interest data structures.
//!
//! Open interest represents the total number of outstanding derivative contracts
//! (futures/options) that have not been settled. Changes in OI signal new money
//! entering or exiting the market.

use crate::errors::BuildError;
use serde::{Deserialize, Serialize};

use crate::trading_pair::TradingPair;

/// A single open interest observation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenInterest {
    /// Timestamp when this value was observed (Unix µs).
    pub open_interest_ts_us: u64,
    /// Canonical trading pair (e.g. `SOL/USDT`).
    pub pair: TradingPair,
    /// Open interest in contract units.
    pub open_interest: f64,
    /// Open interest in quote currency (e.g. USD value).
    pub open_interest_value: f64,
    /// Exchange name.
    pub exchange: String,
}

impl OpenInterest {
    /// Create a new [`OpenInterestBuilder`].
    pub fn builder() -> OpenInterestBuilder {
        OpenInterestBuilder::new()
    }
}

/// Builder for constructing an [`OpenInterest`] with validated fields.
///
/// Required fields: `open_interest_ts_us`, `symbol`, `open_interest`,
/// `exchange`.  `open_interest_value` defaults to `0.0` when omitted.
#[derive(Debug, Clone)]
pub struct OpenInterestBuilder {
    /// Open interest observation timestamp in Unix microseconds.
    open_interest_ts_us: Option<u64>,
    /// Canonical trading pair.
    pair: Option<TradingPair>,
    /// Open interest in contract units.
    open_interest: Option<f64>,
    /// Open interest in quote-currency value.
    open_interest_value: Option<f64>,
    /// Exchange name.
    exchange: Option<String>,
}

impl Default for OpenInterestBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenInterestBuilder {
    /// Create an empty builder with all fields set to `None`.
    pub fn new() -> Self {
        OpenInterestBuilder {
            open_interest_ts_us: None,
            pair: None,
            open_interest: None,
            open_interest_value: None,
            exchange: None,
        }
    }

    /// Set the observation timestamp (Unix µs).
    pub fn open_interest_ts_us(mut self, open_interest_ts_us: u64) -> Self {
        self.open_interest_ts_us = Some(open_interest_ts_us);
        self
    }

    /// Set the canonical trading pair.
    pub fn pair(mut self, pair: TradingPair) -> Self {
        self.pair = Some(pair);
        self
    }

    /// Set the open interest in contract units.
    pub fn open_interest(mut self, oi: f64) -> Self {
        self.open_interest = Some(oi);
        self
    }

    /// Set the open interest in quote-currency value (optional, defaults to `0.0`).
    pub fn open_interest_value(mut self, value: f64) -> Self {
        self.open_interest_value = Some(value);
        self
    }

    /// Set the exchange name (e.g. `"bybit"`).
    pub fn exchange(mut self, exchange: String) -> Self {
        self.exchange = Some(exchange);
        self
    }

    /// Consume the builder and produce an [`OpenInterest`].
    ///
    /// # Errors
    ///
    /// Returns [`BuildError`] if any required field is missing.
    /// `open_interest_value` defaults to `0.0` when not set.
    pub fn build(self) -> Result<OpenInterest, BuildError> {
        let open_interest_ts_us = self
            .open_interest_ts_us
            .ok_or(BuildError::MissingField("ts"))?;
        let pair = self.pair.ok_or(BuildError::MissingField("pair"))?;
        let open_interest = self
            .open_interest
            .ok_or(BuildError::MissingField("open_interest"))?;
        let open_interest_value = self.open_interest_value.unwrap_or(0.0);
        let exchange = self.exchange.ok_or(BuildError::MissingField("exchange"))?;

        Ok(OpenInterest {
            open_interest_ts_us,
            pair,
            open_interest,
            open_interest_value,
            exchange,
        })
    }
}
