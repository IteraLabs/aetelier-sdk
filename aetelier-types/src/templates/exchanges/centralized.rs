use crate::errors::BuildError;
use serde::Deserialize;

/// Configuration for a centralized exchange.
#[derive(Debug, Deserialize, Clone)]
pub struct ExchangeConfig {
    /// Unique exchange identifier.
    pub id: String,
    /// Geographic region where the exchange operates.
    pub region: String,
    /// Name of the exchange.
    pub name: String,
    /// Category or type of exchange.
    pub category: String,
    /// Orderbook configuration (optional).
    pub orderbook: Option<OrderbookConfig>,
}

/// Configuration for orderbook generation.
#[derive(Debug, Deserialize, Clone)]
pub struct OrderbookConfig {
    /// Update frequency in some unit.
    pub update_freq: Option<u64>,
    /// Base bid price.
    pub bid_price: Option<f64>,
    /// Bid level ranges.
    pub bid_levels: Option<Vec<u32>>,
    /// Bid order counts.
    pub bid_orders: Option<Vec<u32>>,
    /// Tick sizes.
    pub ticksize: Option<Vec<f64>>,
    /// Base ask price.
    pub ask_price: Option<f64>,
    /// Ask level ranges.
    pub ask_levels: Option<Vec<u32>>,
    /// Ask order counts.
    pub ask_orders: Option<Vec<u32>>,
    /// Random parameters.
    pub rands: Option<Vec<f64>>,
}

impl OrderbookConfig {
    /// Create a new builder for constructing OrderbookConfig.
    pub fn builder() -> OrderbookConfigBuilder {
        OrderbookConfigBuilder::new()
    }
}

/// Builder for constructing OrderbookConfig instances.
#[derive(Debug, Deserialize, Clone)]
pub struct OrderbookConfigBuilder {
    /// Update frequency in some unit.
    pub update_freq: Option<u64>,
    /// Base bid price.
    pub bid_price: Option<f64>,
    /// Bid level ranges.
    pub bid_levels: Option<Vec<u32>>,
    /// Bid order counts.
    pub bid_orders: Option<Vec<u32>>,
    /// Tick sizes.
    pub ticksize: Option<Vec<f64>>,
    /// Base ask price.
    pub ask_price: Option<f64>,
    /// Ask level ranges.
    pub ask_levels: Option<Vec<u32>>,
    /// Ask order counts.
    pub ask_orders: Option<Vec<u32>>,
    /// Random parameters.
    pub rands: Option<Vec<f64>>,
}

impl Default for OrderbookConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderbookConfigBuilder {
    /// Create a new OrderbookConfigBuilder with all fields set to None.
    pub fn new() -> Self {
        OrderbookConfigBuilder {
            update_freq: None,
            bid_price: None,
            bid_levels: None,
            bid_orders: None,
            ticksize: None,
            ask_price: None,
            ask_levels: None,
            ask_orders: None,
            rands: None,
        }
    }

    /// Set the update frequency.
    pub fn update_freq(mut self, update_freq: u64) -> Self {
        self.update_freq = Some(update_freq);
        self
    }

    /// Set the base bid price.
    pub fn bid_price(mut self, bid_price: f64) -> Self {
        self.bid_price = Some(bid_price);
        self
    }

    /// Set the bid level ranges.
    pub fn bid_levels(mut self, bid_levels: Vec<u32>) -> Self {
        self.bid_levels = Some(bid_levels);
        self
    }

    /// Set the bid order counts.
    pub fn bid_orders(mut self, bid_orders: Vec<u32>) -> Self {
        self.bid_orders = Some(bid_orders);
        self
    }

    /// Set the base ask price.
    pub fn ask_price(mut self, ask_price: f64) -> Self {
        self.ask_price = Some(ask_price);
        self
    }

    /// Set the ask level ranges.
    pub fn ask_levels(mut self, ask_levels: Vec<u32>) -> Self {
        self.ask_levels = Some(ask_levels);
        self
    }

    /// Set the ask order counts.
    pub fn ask_orders(mut self, ask_orders: Vec<u32>) -> Self {
        self.ask_orders = Some(ask_orders);
        self
    }

    /// Set the tick sizes.
    pub fn ticksize(mut self, ticksize: Vec<f64>) -> Self {
        self.ticksize = Some(ticksize);
        self
    }

    /// Build the OrderbookConfig, validating all required fields are set.
    pub fn build(self) -> Result<OrderbookConfig, BuildError> {
        let update_freq = self
            .update_freq
            .ok_or(BuildError::MissingField("initial update freq"))?;
        let bid_price = self
            .bid_price
            .ok_or(BuildError::MissingField("initial bid price"))?;
        let bid_levels = self
            .bid_levels
            .ok_or(BuildError::MissingField("initial bid levels"))?;
        let bid_orders = self
            .bid_orders
            .ok_or(BuildError::MissingField("initial bid orders"))?;
        let ticksize = self
            .ticksize
            .ok_or(BuildError::MissingField("initial tick size"))?;
        let ask_price = self
            .ask_price
            .ok_or(BuildError::MissingField("initial ask price"))?;
        let ask_levels = self
            .ask_levels
            .ok_or(BuildError::MissingField("initial ask levels"))?;
        let ask_orders = self
            .ask_orders
            .ok_or(BuildError::MissingField("initial ask orders"))?;
        let rands = self
            .rands
            .ok_or(BuildError::MissingField("initial random numbers"))?;

        Ok(OrderbookConfig {
            update_freq: Some(update_freq),
            bid_price: Some(bid_price),
            bid_levels: Some(bid_levels),
            bid_orders: Some(bid_orders),
            ticksize: Some(ticksize),
            ask_price: Some(ask_price),
            ask_levels: Some(ask_levels),
            ask_orders: Some(ask_orders),
            rands: Some(rands),
        })
    }
}
