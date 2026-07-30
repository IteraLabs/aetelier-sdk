//! Order primitives: side, type, ID encoding, and builder.
//!
//! This module defines the core types for representing orders in an
//! in-memory orderbook.  `OrderId` is a bit-packed `u64` that
//! encodes side, type, and timestamp into a single sortable value.
//! `OrderBuilder` provides a validated construction path for
//! `Order` instances.

use crate::errors::BuildError;
use std::fmt;
use std::str::FromStr;

use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::TimestampUs;

/// Side of an order book: bid (buy) or ask (sell).
///
/// `OrderSide` represents the book side for orderbook levels and
/// price-level records.  It is intentionally separate from
/// [`TradeSide`](crate::trades::TradeSide), which represents the
/// taker direction of a trade or liquidation.
///
/// # Serialization
///
/// Serde and `Display`/`as_str` produce lowercase **singular** forms
/// (`"bid"`, `"ask"`) to match the Parquet / CSV column convention.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Serialize, Deserialize)]
pub enum OrderSide {
    /// Buy side (bid).
    #[serde(rename = "bid")]
    Bids,
    /// Sell side (ask).
    #[serde(rename = "ask")]
    Asks,
}

impl fmt::Display for OrderSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl OrderSide {
    /// Lowercase singular string: `"bid"` or `"ask"`.
    ///
    /// Matches the Parquet/CSV column convention and serde output.
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderSide::Bids => "bid",
            OrderSide::Asks => "ask",
        }
    }

    /// Lenient parser that accepts common wire-format variants.
    ///
    /// Accepted inputs (case-insensitive):
    /// `"bid"`, `"bids"`, `"buy"` → `Bids`
    /// `"ask"`, `"asks"`, `"sell"`, `"offer"` → `Asks`
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "bid" | "bids" | "buy" => Some(OrderSide::Bids),
            "ask" | "asks" | "sell" | "offer" => Some(OrderSide::Asks),
            _ => None,
        }
    }

    /// Return a pseudo-random `OrderSide` based on the parity of the
    /// current Unix timestamp.
    pub fn random() -> Self {
        let now_ts = TimestampUs::now().as_micros() / 1_000_000;

        if now_ts.is_multiple_of(2) {
            OrderSide::Bids
        } else {
            OrderSide::Asks
        }
    }
}

impl FromStr for OrderSide {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_loose(s).ok_or_else(|| format!("unknown order side: {s:?}"))
    }
}

/// Supported order types: market or limit.
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum OrderType {
    /// Market order executed at current market price.
    Market,
    /// Limit order at a specific price.
    Limit,
}

impl OrderType {
    /// Return a pseudo-random `OrderType` based on the parity of the
    /// current Unix timestamp.
    pub fn random() -> Self {
        let now_secs = TimestampUs::now().as_micros() / 1_000_000;

        if now_secs.is_multiple_of(2) {
            OrderType::Limit
        } else {
            OrderType::Market
        }
    }
}

/// Compact order identifier that bit-packs side, type, and timestamp
/// into a single `u64`.
///
/// # Bit layout
///
/// ```text
/// STTT TTTT TTTT TTTT TTTT TTTT TTTT TTTT TTTT TTTT TTTT TTTT TTTT TTTT TTTT TTTT
/// │└─┤                                                                            │
/// │  └──────────────────────── 60 bits: timestamp (valid until ~2079)
/// │
/// ├── bit 63: side  (0 = Bids, 1 = Asks)
/// └── bit 62: type  (0 = Market, 1 = Limit)
/// ```
///
/// Use [`OrderId::new`] to construct and the accessor methods
/// ([`timestamp`](Self::timestamp), [`side`](Self::side),
/// [`order`](Self::order)) to decode.
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct OrderId(u64);

impl OrderId {
    /// Mask that isolates the lower 60 bits (timestamp portion).
    const TIME_MASK: u64 = 0x0FFF_FFFF_FFFF_FFFF;

    /// Construct a new `OrderId` from its constituent parts.
    ///
    /// The timestamp is truncated to 60 bits and packed together with
    /// the side bit (63) and type bit (62).
    pub fn new(ts: u64, order_side: OrderSide, order_type: OrderType) -> Self {
        let mut val = ts & Self::TIME_MASK;
        val |= (order_side as u64) << 63;
        val |= (order_type as u64) << 62;
        OrderId(val)
    }

    /// Extract the timestamp component (lower 60 bits).
    pub fn timestamp(&self) -> u64 {
        self.0 & Self::TIME_MASK
    }

    /// Extract the order side from bit 62.
    pub fn side(&self) -> OrderSide {
        if self.0 >> 62 & 1 == 0 {
            OrderSide::Bids
        } else {
            OrderSide::Asks
        }
    }

    /// Extract the order type from bit 63.
    pub fn order(&self) -> OrderType {
        if self.0 >> 63 & 1 == 0 {
            OrderType::Market
        } else {
            OrderType::Limit
        }
    }
}

/// Builder for constructing an [`Order`] with validated fields.
///
/// Required fields: `side` and `order_type`.  If `order_ts_us` is omitted
/// the builder fills it with the current wall-clock time in microseconds.
/// `price` and `amount` are optional (e.g. market orders may have no
/// explicit price).
///
/// The `order_id` is derived automatically from `side`, `order_type`,
/// and `order_ts_us` via [`Order::encode_order_id`].
#[derive(Debug, Copy, Clone)]
pub struct OrderBuilder {
    order_ts_us: Option<u64>,
    order_type: Option<OrderType>,
    side: Option<OrderSide>,
    price: Option<f64>,
    amount: Option<f64>,
}

impl Default for OrderBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderBuilder {
    /// Create an empty builder with all fields set to `None`.
    pub fn new() -> Self {
        OrderBuilder {
            side: None,
            order_type: None,
            order_ts_us: None,
            price: None,
            amount: None,
        }
    }

    /// Generate a random [`Order`] directly (bypasses the builder chain).
    ///
    /// Price and amount are drawn from uniform distributions defined by
    /// the `(lower, upper)` range tuples.  The timestamp is the current
    /// wall-clock time in microseconds.
    pub fn random_new(
        r_order_type: OrderType,
        r_order_side: OrderSide,
        r_order_prices: (f64, f64),
        r_order_amounts: (f64, f64),
    ) -> Order {
        let mut rng = rand::rng();

        let r_order_ts = TimestampUs::now().as_micros();

        let r_order_price = rng.random_range(r_order_prices.0..r_order_prices.1);
        let r_order_amount = rng.random_range(r_order_amounts.0..r_order_amounts.1);

        let r_order_id = Order::encode_order_id(r_order_side, r_order_type, r_order_ts);

        Order {
            order_id: r_order_id,
            side: r_order_side,
            order_type: r_order_type,
            order_ts_us: r_order_ts,
            price: Some(r_order_price),
            amount: Some(r_order_amount),
        }
    }

    /// Set the order side.
    pub fn side(mut self, side: OrderSide) -> Self {
        self.side = Some(side);
        self
    }

    /// Set the order type.
    pub fn order_type(mut self, order_type: OrderType) -> Self {
        self.order_type = Some(order_type);
        self
    }

    /// Set the order timestamp in microseconds.
    ///
    /// If omitted, [`build()`](Self::build) uses the current wall-clock
    /// time.
    pub fn order_ts_us(mut self, order_ts_us: u64) -> Self {
        self.order_ts_us = Some(order_ts_us);
        self
    }

    /// Set the limit price (optional for market orders).
    pub fn price(mut self, price: f64) -> Self {
        self.price = Some(price);
        self
    }

    /// Set the order quantity (optional).
    pub fn amount(mut self, amount: f64) -> Self {
        self.amount = Some(amount);
        self
    }

    /// Consume the builder and produce an [`Order`].
    ///
    /// The `order_id` is computed automatically from `side`,
    /// `order_type`, and `order_ts_us` via [`Order::encode_order_id`].
    ///
    /// # Errors
    ///
    /// Returns `Err(&str)` if `side` or `order_type` is missing.
    pub fn build(self) -> Result<Order, BuildError> {
        let order_side = self.side.ok_or(BuildError::MissingField("side"))?;
        let order_type = self
            .order_type
            .ok_or(BuildError::MissingField("order_type"))?;
        let order_ts_us = self
            .order_ts_us
            .unwrap_or_else(|| TimestampUs::now().as_micros());

        let order_id = Order::encode_order_id(order_side, order_type, order_ts_us);

        Ok(Order {
            order_id,
            order_ts_us,
            order_type,
            side: order_side,
            price: self.price,
            amount: self.amount,
        })
    }
}

/// A single order in the orderbook.
///
/// Contains a bit-packed [`order_id`](Self::order_id) that encodes
/// side, type, and timestamp — see [`encode_order_id`](Self::encode_order_id)
/// for the layout.  `price` and `amount` are optional because market
/// orders may not carry an explicit price.
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Order {
    /// Bit-packed identifier encoding side, type, and timestamp.
    pub order_id: u64,
    /// Order creation timestamp in microseconds since epoch.
    pub order_ts_us: u64,
    /// Order type (market or limit).
    pub order_type: OrderType,
    /// Order side (bid or ask).
    pub side: OrderSide,
    /// Limit price in quote-currency units (None for market orders).
    pub price: Option<f64>,
    /// Order quantity in base-currency units.
    pub amount: Option<f64>,
}

impl Order {
    /// Create a new [`OrderBuilder`].
    pub fn builder() -> OrderBuilder {
        OrderBuilder::new()
    }

    /// Generate a random `Order` for testing and simulation.
    ///
    /// Price and amount are drawn from uniform distributions defined
    /// by the `(lower, upper)` range tuples.  Delegates to
    /// [`OrderBuilder::random_new`].
    pub fn random(
        order_type: OrderType,
        order_side: OrderSide,
        order_prices: (f64, f64),
        order_amounts: (f64, f64),
    ) -> Result<Order, BuildError> {
        Ok(OrderBuilder::random_new(
            order_type,
            order_side,
            order_prices,
            order_amounts,
        ))
    }

    /// Encode side, type, and timestamp into a single `u64` order ID.
    ///
    /// # Bit layout
    ///
    /// ```text
    /// STTTTTTT TTTTTTTT TTTTTTTT TTTTTTTT TTTTTTTT TTTTTTTT TTTTTTTT TTTTTTTT
    /// |└─ 60 bits: timestamp (valid until ~2079)
    /// ├── bit 63: side  (0 = Bid, 1 = Ask)
    /// └── bit 62: type  (0 = Market, 1 = Limit)
    /// ```
    pub fn encode_order_id(
        order_side: OrderSide,
        order_type: OrderType,
        order_ts_us: u64,
    ) -> u64 {
        // Highest bit
        let side_bit = match order_side {
            OrderSide::Bids => 0,
            OrderSide::Asks => 1,
        } << 63;
        // Second highest bit
        let type_bit = match order_type {
            OrderType::Market => 0,
            OrderType::Limit => 1,
        } << 62;
        // 60 bits starting at position 2
        let timestamp_bits = (order_ts_us & ((1 << 60) - 1)) << 2;

        side_bit | type_bit | timestamp_bits
    }

    /// Decode a bit-packed order ID back into its constituent parts.
    ///
    /// See [`encode_order_id`](Self::encode_order_id) for the bit layout.
    pub fn decode_order_id(order_id: u64) -> (OrderSide, OrderType, u64) {
        // Highest bit
        let order_side = if (order_id >> 63) & 1 == 0 {
            OrderSide::Bids
        } else {
            OrderSide::Asks
        };
        // Second highest bit
        let order_type = if (order_id >> 62) & 1 == 0 {
            OrderType::Market
        } else {
            OrderType::Limit
        };
        // 60 bits starting at position 2
        let order_ts_us = (order_id >> 2) & ((1 << 60) - 1);

        (order_side, order_type, order_ts_us)
    }
}
