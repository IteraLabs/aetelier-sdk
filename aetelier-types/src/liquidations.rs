//! Liquidation events: data model and builder.
//!
//! Liquidations occur when a trader's collateral falls below the
//! maintenance margin requirement, forcing their position to be
//! automatically closed at market prices.

use crate::errors::BuildError;
use rand::Rng;
use rand::prelude::IndexedRandom;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::TimestampUs;
use crate::orderbooks::f64_to_decimal;
use crate::trades::TradeSide;
use crate::trading_pair::TradingPair;

/// A single forced-liquidation event observed on an exchange.
///
/// When a trader's margin falls below the maintenance requirement the
/// exchange force-closes their position at market prices.  This struct
/// is the **exchange-agnostic** normalised representation — exchange-
/// specific wire formats are mapped into `Liquidation` by the
/// per-exchange client implementations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Liquidation {
    /// Timestamp when the liquidation occurred (Unix µs).
    pub liquidation_ts_us: u64,
    /// Canonical trading pair (e.g. `BTC/USDT`).
    pub pair: TradingPair,
    /// Side being liquidated.
    pub side: TradeSide,
    /// Liquidated quantity in base-currency units. Exact decimal in memory;
    /// serialized as a JSON float and persisted as Parquet `Float64`.
    #[serde(with = "rust_decimal::serde::float")]
    pub amount: Decimal,
    /// Bankruptcy / fill price in quote-currency units. Exact decimal in
    /// memory; serialized as a JSON float and persisted as Parquet `Float64`.
    #[serde(with = "rust_decimal::serde::float")]
    pub price: Decimal,
    /// Exchange name (e.g. `"bybit"`, `"binance"`).
    pub exchange: String,
}

impl Liquidation {
    /// Create a new [`LiquidationBuilder`].
    pub fn builder() -> LiquidationBuilder {
        LiquidationBuilder::new()
    }

    /// Generate a random `Liquidation` for testing and simulation.
    ///
    /// Produces a liquidation with the current wall-clock timestamp,
    /// a randomly chosen side and exchange, and price / amount drawn
    /// from uniform distributions.
    pub fn random() -> Self {
        let r_liquidation_ts = TimestampUs::now().as_micros();

        let exchanges = ["bybit", "kraken", "coinbase", "binance"];
        let mut rng = rand::rng();
        let r_pair = TradingPair::new("BTC", "USDT");

        let r_side = TradeSide::random();

        let r_amount = rng.random_range(0.01..1.10);
        let r_price = rng.random_range(100_000.0..110_000.0);
        let r_exchange = exchanges
            .choose(&mut rng)
            .expect("Error in random exchange choice")
            .to_string();

        Self {
            liquidation_ts_us: r_liquidation_ts,
            pair: r_pair,
            side: r_side,
            amount: f64_to_decimal(r_amount),
            price: f64_to_decimal(r_price),
            exchange: r_exchange,
        }
    }
}

/// Builder for constructing a [`Liquidation`] with validated fields.
///
/// Every field is required.  Calling [`build()`](Self::build) with any
/// field missing returns `Err(String)` naming the absent field.
#[derive(Debug, Clone)]
pub struct LiquidationBuilder {
    /// Liquidation timestamp in Unix microseconds.
    pub liquidation_ts_us: Option<u64>,
    /// Canonical trading pair.
    pub pair: Option<TradingPair>,
    /// Liquidation side.
    pub side: Option<TradeSide>,
    /// Liquidated amount in base-currency units.
    pub amount: Option<f64>,
    /// Fill price in quote-currency units.
    pub price: Option<f64>,
    /// Exchange name.
    pub exchange: Option<String>,
}

impl Default for LiquidationBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl LiquidationBuilder {
    /// Create an empty builder with all fields set to `None`.
    pub fn new() -> Self {
        LiquidationBuilder {
            liquidation_ts_us: None,
            pair: None,
            side: None,
            amount: None,
            price: None,
            exchange: None,
        }
    }

    /// Set the liquidation timestamp (Unix µs).
    pub fn ts(mut self, ts: u64) -> Self {
        self.liquidation_ts_us = Some(ts);
        self
    }

    /// Set the canonical trading pair.
    pub fn pair(mut self, pair: TradingPair) -> Self {
        self.pair = Some(pair);
        self
    }

    /// Set the side being liquidated.
    pub fn side(mut self, side: TradeSide) -> Self {
        self.side = Some(side);
        self
    }

    /// Set the liquidated quantity in base-currency units.
    pub fn amount(mut self, amount: f64) -> Self {
        self.amount = Some(amount);
        self
    }

    /// Set the bankruptcy / fill price in quote-currency units.
    pub fn price(mut self, price: f64) -> Self {
        self.price = Some(price);
        self
    }

    /// Set the exchange name (e.g. `"bybit"`).
    pub fn exchange(mut self, exchange: String) -> Self {
        self.exchange = Some(exchange);
        self
    }

    /// Consume the builder and produce a [`Liquidation`].
    ///
    /// # Errors
    ///
    /// Returns [`BuildError`] if any required field is missing.
    pub fn build(self) -> Result<Liquidation, BuildError> {
        let liquidation_ts_us = self
            .liquidation_ts_us
            .ok_or(BuildError::MissingField("ts"))?;
        let pair = self.pair.ok_or(BuildError::MissingField("pair"))?;
        let side = self.side.ok_or(BuildError::MissingField("side"))?;
        let amount = self.amount.ok_or(BuildError::MissingField("amount"))?;
        let price = self.price.ok_or(BuildError::MissingField("price"))?;
        let exchange = self.exchange.ok_or(BuildError::MissingField("exchange"))?;

        Ok(Liquidation {
            liquidation_ts_us,
            pair,
            side,
            // Builder accepts f64 for call-site convenience.
            amount: f64_to_decimal(amount),
            price: f64_to_decimal(price),
            exchange,
        })
    }
}
