//! Core orderbook structure and operations.
//!
//! Provides the [`Orderbook`] data structure for managing bid/ask levels
//! and orders, along with utility functions for decimal/f64 conversion.

use super::delta::{NormalizedDelta, OrderbookDelta};
use crate::{
    errors::OrderbookError,
    exchanges::Exchange,
    levels::Level,
    orders::{Order, OrderSide, OrderType},
    trading_pair::TradingPair,
};
use rand::{Rng, distr::Uniform};
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::str::FromStr;
use tracing::debug;

use crate::TimestampUs;

/// Represents the Limit Order Book data structure.
///
/// Bids and asks are stored as `BTreeMap<Decimal, Level>` keyed by price.
/// BTreeMap's natural ascending order means:
/// - **Best bid** = `bids.last_key_value()` (highest price)
/// - **Best ask** = `asks.first_key_value()` (lowest price)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Orderbook {
    /// Unique identifier for this orderbook snapshot.
    pub orderbook_id: u32,
    /// Timestamp of the orderbook snapshot. A synchronizer re-stamps this to
    /// its grid period; for the raw exchange time use `source_orderbook_ts_us`.
    pub orderbook_ts_us: u64,
    /// Exchange-reported event time (µs) of the last applied delta, preserved
    /// through synchronization. 0 if the venue gives no event time.
    pub source_orderbook_ts_us: u64,
    /// Local receipt time (Unix µs) of the last applied delta.
    pub local_orderbook_ts_us: u64,
    /// Connection ping/pong round-trip (µs) at the last applied delta.
    pub source_orderbook_rtt_us: u64,
    /// Canonical trading pair (e.g. `SOL/USDT`).
    pub pair: TradingPair,
    /// Exchange identifier.
    pub exchange: String,
    /// Bid levels keyed by price (ascending order).
    pub bids: BTreeMap<Decimal, Level>,
    /// Ask levels keyed by price (ascending order).
    pub asks: BTreeMap<Decimal, Level>,
}

impl Orderbook {
    /// Creates a new instance of `Orderbook` from pre-built BTreeMaps.
    pub fn new(
        orderbook_id: u32,
        orderbook_ts_us: u64,
        pair: TradingPair,
        exchange: String,
        bids: BTreeMap<Decimal, Level>,
        asks: BTreeMap<Decimal, Level>,
    ) -> Self {
        Orderbook {
            orderbook_id,
            orderbook_ts_us,
            source_orderbook_ts_us: 0,
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            pair,
            exchange,
            bids,
            asks,
        }
    }

    /// Construct an `Orderbook` from `Vec<Level>` pairs.
    ///
    /// Each level is inserted into the BTreeMap keyed by its price.
    /// This is the primary constructor for code that naturally produces
    /// vectors of levels (exchange decoders, parquet readers, etc.).
    pub fn from_levels(
        orderbook_id: u32,
        orderbook_ts_us: u64,
        pair: TradingPair,
        exchange: String,
        bids: Vec<Level>,
        asks: Vec<Level>,
    ) -> Self {
        let bids_map: BTreeMap<Decimal, Level> =
            bids.into_iter().map(|l| (l.price, l)).collect();
        let asks_map: BTreeMap<Decimal, Level> =
            asks.into_iter().map(|l| (l.price, l)).collect();

        Self::new(
            orderbook_id,
            orderbook_ts_us,
            pair,
            exchange,
            bids_map,
            asks_map,
        )
    }

    /// Create an empty orderbook for a trading pair, with no levels and zero timestamps.
    #[inline]
    pub fn empty(pair: TradingPair, exchange: &str) -> Self {
        Self::new(
            0,
            0,
            pair,
            exchange.to_string(),
            BTreeMap::new(),
            BTreeMap::new(),
        )
    }

    // ------------------------------------------------------------------- Accessors -- //

    /// Best bid price (highest). `None` if no bids.
    #[inline]
    pub fn best_bid(&self) -> Option<Decimal> {
        self.bids.last_key_value().map(|(p, _)| *p)
    }

    /// Best ask price (lowest). `None` if no asks.
    #[inline]
    pub fn best_ask(&self) -> Option<Decimal> {
        self.asks.first_key_value().map(|(p, _)| *p)
    }

    /// Mid-price: `(best_bid + best_ask) / 2`. `None` if either side is empty.
    #[inline]
    pub fn mid_price(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) => Some((b + a) / Decimal::TWO),
            _ => None,
        }
    }

    /// Spread: `best_ask - best_bid`. `None` if either side is empty.
    #[inline]
    pub fn spread(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) => Some(a - b),
            _ => None,
        }
    }

    /// A basic description of the Top of the Order Book.
    ///
    /// ## Returns
    /// Ok(`Vec<f64>`): [mid_price, total_volume, n_bids, n_asks]
    /// Err(OrderbookError::ContentsError) if either side is empty.
    #[inline]
    pub fn describe(&self) -> Result<Vec<f64>, OrderbookError> {
        let bid = self.best_bid();
        let ask = self.best_ask();

        match (bid, ask) {
            (Some(b), Some(a)) => {
                let mid = decimal_to_f64((b + a) / Decimal::TWO);
                let best_bid_vol = self
                    .bids
                    .last_key_value()
                    .map(|(_, l)| decimal_to_f64(l.volume))
                    .unwrap_or(0.0);
                let best_ask_vol = self
                    .asks
                    .first_key_value()
                    .map(|(_, l)| decimal_to_f64(l.volume))
                    .unwrap_or(0.0);
                let n_bids = self.bids.len() as f64;
                let n_asks = self.asks.len() as f64;
                Ok(vec![mid, best_bid_vol + best_ask_vol, n_bids, n_asks])
            }
            _ => Err(OrderbookError::ContentsError(
                "Operation attempt in Orderbook !".to_string(),
            )),
        }
    }

    // ---------------------------------------------------------------- Find a Level -- //

    /// Find a level at the given price, returning (side, reference).
    ///
    /// O(log n) via BTreeMap lookup — no more linear scan.
    pub fn find_level(
        &self,
        price: &Decimal,
    ) -> Result<(OrderSide, &Level), OrderbookError> {
        if let Some(level) = self.bids.get(price) {
            return Ok((OrderSide::Bids, level));
        }
        if let Some(level) = self.asks.get(price) {
            return Ok((OrderSide::Asks, level));
        }
        Err(OrderbookError::LevelNotFound {
            price: price.to_string(),
        })
    }

    // ----------------------------------------- Retrieve an Existing Level -- //

    /// If a Level exists, return a cloned version of it.
    pub fn retrieve_level(&self, price: &Decimal) -> Result<Level, OrderbookError> {
        let (side, level) = self.find_level(price)?;
        debug!(?side, "retrieve_level: level found");
        Ok(level.clone())
    }

    // ------------------------------------------- Delete an Existing Level -- //

    /// Deletes an existing level at the given price.
    pub fn delete_level(&mut self, price: &Decimal) -> Result<(), OrderbookError> {
        if self.bids.remove(price).is_some() {
            return Ok(());
        }
        if self.asks.remove(price).is_some() {
            return Ok(());
        }
        Err(OrderbookError::LevelNotFound {
            price: price.to_string(),
        })
    }

    // ------------------------------------------------- Insert a New Level -- //

    /// Inserts or replaces a level. BTreeMap maintains sorted order
    /// automatically — no manual position search needed.
    pub fn insert_level(&mut self, level: Level) -> Result<(), OrderbookError> {
        match level.side {
            OrderSide::Bids => {
                // Preserve level_id if replacing
                let level_id = self
                    .bids
                    .get(&level.price)
                    .map(|existing| existing.level_id)
                    .unwrap_or(level.level_id);
                self.bids.insert(
                    level.price,
                    Level::new(
                        level_id,
                        level.side,
                        level.price,
                        level.volume,
                        level.orders,
                    ),
                );
            }
            OrderSide::Asks => {
                let level_id = self
                    .asks
                    .get(&level.price)
                    .map(|existing| existing.level_id)
                    .unwrap_or(level.level_id);
                self.asks.insert(
                    level.price,
                    Level::new(
                        level_id,
                        level.side,
                        level.price,
                        level.volume,
                        level.orders,
                    ),
                );
            }
        }
        Ok(())
    }

    // ------------------------------------------------------ Find an Order -- //

    /// Find an `Order` by price (level) and timestamp.
    pub fn find_order(
        &self,
        price: Decimal,
        order_ts_us: u64,
    ) -> Result<(OrderSide, &Level, usize), OrderbookError> {
        let (side, level) = self.find_level(&price)?;

        if level.orders.is_empty() {
            return Err(OrderbookError::OrderNotFound { order_ts_us });
        }

        let order_idx = level
            .orders
            .binary_search_by(|order| order.order_ts_us.cmp(&order_ts_us))
            .map_err(|_| OrderbookError::OrderNotFound { order_ts_us })?;

        Ok((side, level, order_idx))
    }

    // ----------------------------------------- Retrieve an Existing Order -- //

    /// Retrieve a copy of an existing `Order`.
    pub fn retrieve_order(
        &self,
        price: Decimal,
        order_ts_us: u64,
    ) -> Result<Order, OrderbookError> {
        let (_side, level, order_idx) = self.find_order(price, order_ts_us)?;
        Ok(level.orders[order_idx])
    }

    // ------------------------------------------- Delete an Existing Order -- //

    /// Delete an existing `Order`.
    pub fn delete_order(
        &mut self,
        price: Decimal,
        order_ts_us: u64,
    ) -> Result<(), OrderbookError> {
        // Need to find side first, then get mutable reference
        let side = {
            let (s, _, _) = self.find_order(price, order_ts_us)?;
            s
        };

        let level = match side {
            OrderSide::Bids => self.bids.get_mut(&price).unwrap(),
            OrderSide::Asks => self.asks.get_mut(&price).unwrap(),
        };

        let order_idx = level
            .orders
            .binary_search_by(|order| order.order_ts_us.cmp(&order_ts_us))
            .map_err(|_| OrderbookError::OrderNotFound { order_ts_us })?;

        level.orders.remove(order_idx);
        Ok(())
    }

    // ------------------------------------------------- Insert a New Order -- //

    /// Insert a new `Order` at the level matching the given price.
    pub fn insert_order(
        &mut self,
        price: Decimal,
        amount: f64,
    ) -> Result<(), OrderbookError> {
        let (side, _) = self.find_level(&price)?;

        let ts = TimestampUs::now().as_micros();

        let order = Order::builder()
            .order_ts_us(ts)
            .order_type(OrderType::Limit)
            .side(side)
            .price(decimal_to_f64(price))
            .amount(amount)
            .build()
            .map_err(|e| OrderbookError::BuilderError(e.to_string()))?;

        let level = match side {
            OrderSide::Bids => self.bids.get_mut(&price).unwrap(),
            OrderSide::Asks => self.asks.get_mut(&price).unwrap(),
        };
        level.orders.push(order);

        Ok(())
    }

    // ---------------------------------------------------- Modify an Order -- //

    /// Modify an existing `Order` **in-place** and return a copy.
    pub fn modify_order(
        &mut self,
        order_ts_us: u64,
        price: Decimal,
        amount: f64,
    ) -> Result<Order, OrderbookError> {
        let side = {
            let (s, _, _) = self.find_order(price, order_ts_us)?;
            s
        };

        debug!(?side, "modify_order: order found");

        let level = match side {
            OrderSide::Bids => self.bids.get_mut(&price).unwrap(),
            OrderSide::Asks => self.asks.get_mut(&price).unwrap(),
        };

        let order_idx = level
            .orders
            .binary_search_by(|order| order.order_ts_us.cmp(&order_ts_us))
            .map_err(|_| OrderbookError::OrderNotFound { order_ts_us })?;

        let order_ref = &mut level.orders[order_idx];

        let order_price = order_ref.price.ok_or_else(|| {
            OrderbookError::InvalidInput("Cannot modify order: price is None".to_string())
        })?;

        let new_order = Order::builder()
            .side(order_ref.side)
            .order_type(order_ref.order_type)
            .order_ts_us(order_ref.order_ts_us)
            .price(order_price)
            .amount(amount)
            .build()
            .map_err(|e| OrderbookError::BuilderError(e.to_string()))?;

        *order_ref = new_order;
        Ok(new_order)
    }

    // --------------------------------------------------- Random Orderbook -- //

    /// Generates a randomized order book with specified parameters.
    ///
    /// Prices are generated as f64 and converted to Decimal at the boundary.
    pub fn random(
        update_ts: Option<u64>,
        bids_price: f64,
        bids_levels: Option<(u32, u32)>,
        bids_orders: Option<(u32, u32)>,
        tick_size: Option<(f64, f64)>,
        asks_price: f64,
        asks_levels: Option<(u32, u32)>,
        asks_orders: Option<(u32, u32)>,
    ) -> Result<Self, OrderbookError> {
        let mut rng = rand::rng();
        let mut bid_map = BTreeMap::new();
        let mut ask_map = BTreeMap::new();
        let current_ts = TimestampUs::now().as_micros();

        let r_orderbook_ts = match update_ts {
            Some(ts) => current_ts + ts,
            _ => current_ts,
        };

        let r_orderbook_id = 1234;

        let (bl_lo, bl_hi) = bids_levels.ok_or_else(|| {
            OrderbookError::InvalidInput("bids_levels is required".to_string())
        })?;
        let (al_lo, al_hi) = asks_levels.ok_or_else(|| {
            OrderbookError::InvalidInput("asks_levels is required".to_string())
        })?;

        let n_bids_levels = rng.sample(Uniform::new(bl_lo, bl_hi).map_err(|e| {
            OrderbookError::InvalidInput(format!(
                "Failed to create bids_levels dist: {e}"
            ))
        })?);

        let n_asks_levels = rng.sample(Uniform::new(al_lo, al_hi).map_err(|e| {
            OrderbookError::InvalidInput(format!(
                "Failed to create asks_levels dist: {e}"
            ))
        })?);

        // Generate ticks
        let mut v_bids_ticks: Vec<f64> = if let Some(bids_range) = tick_size {
            let uni_rand = Uniform::new(bids_range.0, bids_range.1).map_err(|e| {
                OrderbookError::InvalidInput(format!("Failed to create tick dist: {e}"))
            })?;
            (0..n_bids_levels).map(|_| rng.sample(uni_rand)).collect()
        } else {
            let uni_rand = Uniform::new(0.0, 1.0).map_err(|e| {
                OrderbookError::InvalidInput(format!(
                    "Failed to create default tick dist: {e}"
                ))
            })?;
            (0..n_bids_levels).map(|_| rng.sample(uni_rand)).collect()
        };
        v_bids_ticks.insert(0, 0.0);
        let mut v_bids_prices: Vec<f64> = vec![bids_price];

        let mut v_asks_ticks: Vec<f64> = if let Some(asks_range) = tick_size {
            let uni_rand = Uniform::new(asks_range.0, asks_range.1).map_err(|e| {
                OrderbookError::InvalidInput(format!("Failed to create tick dist: {e}"))
            })?;
            (0..n_asks_levels).map(|_| rng.sample(uni_rand)).collect()
        } else {
            let uni_rand = Uniform::new(0.0, 1.0).map_err(|e| {
                OrderbookError::InvalidInput(format!(
                    "Failed to create default tick dist: {e}"
                ))
            })?;
            (0..n_asks_levels).map(|_| rng.sample(uni_rand)).collect()
        };
        v_asks_ticks.insert(0, 0.0);
        let mut v_asks_prices: Vec<f64> = vec![asks_price];

        // ------------------------------------------- Bid Side Formation -- //

        for i in 1..=n_bids_levels {
            let i_bids_price_f64 =
                v_bids_prices[(i - 1) as usize] - v_bids_ticks[(i - 1) as usize];
            v_bids_prices.push(i_bids_price_f64);

            let i_bids_price = f64_to_decimal(i_bids_price_f64);

            let i_bids_orders = if let Some(bid_orders_range) = bids_orders {
                rng.random_range(bid_orders_range.0..bid_orders_range.1)
            } else {
                rng.sample(Uniform::new(1, 5).map_err(|e| {
                    OrderbookError::InvalidInput(format!("bids_orders dist: {e}"))
                })?)
            };

            let mut v_bids_orders: Vec<Order> = (0..i_bids_orders)
                .map(|_| {
                    Order::random(
                        OrderType::Limit,
                        OrderSide::Bids,
                        (10_000.01, 11_000.01),
                        (0.001, 0.100),
                    )
                    .expect("Order::random should not fail with valid ranges")
                })
                .collect();
            v_bids_orders.sort_by_key(|order| order.order_ts_us);

            let i_bids_volume: f64 = v_bids_orders
                .iter()
                .map(|order| order.amount.unwrap_or(0.0))
                .sum();

            bid_map.insert(
                i_bids_price,
                Level {
                    level_id: 4321,
                    side: OrderSide::Bids,
                    price: i_bids_price,
                    volume: f64_to_decimal(i_bids_volume),
                    orders: v_bids_orders,
                },
            );
        }

        // ------------------------------------------- Ask Side Formation -- //

        for i in 1..=n_asks_levels {
            let i_asks_price_f64 =
                v_asks_prices[(i - 1) as usize] - v_asks_ticks[(i - 1) as usize];
            v_asks_prices.push(i_asks_price_f64);

            let i_asks_price = f64_to_decimal(i_asks_price_f64);

            let i_asks_orders = if let Some(asks_orders_range) = asks_orders {
                rng.random_range(asks_orders_range.0..asks_orders_range.1)
            } else {
                rng.sample(Uniform::new(1, 5).map_err(|e| {
                    OrderbookError::InvalidInput(format!("asks_orders dist: {e}"))
                })?)
            };

            let mut v_asks_orders: Vec<Order> = (0..i_asks_orders)
                .map(|_| {
                    Order::random(
                        OrderType::Limit,
                        OrderSide::Asks,
                        (10_000.01, 11_000.01),
                        (0.001, 0.100),
                    )
                    .expect("Order::random should not fail with valid ranges")
                })
                .collect();
            v_asks_orders.sort_by_key(|order| order.order_ts_us);

            let i_asks_volume: f64 = v_asks_orders
                .iter()
                .map(|order| order.amount.unwrap_or(0.0))
                .sum();

            ask_map.insert(
                i_asks_price,
                Level {
                    level_id: 7654,
                    side: OrderSide::Asks,
                    price: i_asks_price,
                    volume: f64_to_decimal(i_asks_volume),
                    orders: v_asks_orders,
                },
            );
        }

        Ok(Orderbook {
            orderbook_id: r_orderbook_id,
            orderbook_ts_us: r_orderbook_ts,
            source_orderbook_ts_us: 0,
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            pair: TradingPair::new("BASE", "QUOTE"),
            exchange: String::from("EXCHANGE"),
            bids: bid_map,
            asks: ask_map,
        })
    }

    /// Construct an `Orderbook` from an exchange-agnostic [`NormalizedDelta`].
    ///
    /// Price/size strings are parsed directly to `Decimal`.
    /// The raw `delta.symbol` is parsed into a [`TradingPair`] using the
    /// exchange hint; falls back to lenient [`FromStr`] parsing.
    pub fn from_normalized_snapshot(
        delta: &NormalizedDelta,
        exchange: &str,
        ts_ms: u64,
    ) -> Result<Self, OrderbookError> {
        let pair = Exchange::from_str_loose(exchange)
            .and_then(|ex| TradingPair::from_exchange_symbol(&delta.symbol, ex))
            .or_else(|| delta.symbol.parse::<TradingPair>().ok())
            .ok_or_else(|| {
                OrderbookError::ParseError(format!(
                    "cannot parse trading pair from '{}' (exchange: {})",
                    delta.symbol, exchange
                ))
            })?;

        let mut bids = BTreeMap::new();
        for (i, (p, s)) in delta.bids.iter().enumerate() {
            let price = Decimal::from_str(p).map_err(|e| {
                OrderbookError::ParseError(format!("bid price '{}': {}", p, e))
            })?;
            let size = Decimal::from_str(s).map_err(|e| {
                OrderbookError::ParseError(format!("bid size '{}': {}", s, e))
            })?;
            bids.insert(
                price,
                Level::new(i as u32, OrderSide::Bids, price, size, vec![]),
            );
        }

        let mut asks = BTreeMap::new();
        for (i, (p, s)) in delta.asks.iter().enumerate() {
            let price = Decimal::from_str(p).map_err(|e| {
                OrderbookError::ParseError(format!("ask price '{}': {}", p, e))
            })?;
            let size = Decimal::from_str(s).map_err(|e| {
                OrderbookError::ParseError(format!("ask size '{}': {}", s, e))
            })?;
            asks.insert(
                price,
                Level::new(i as u32, OrderSide::Asks, price, size, vec![]),
            );
        }

        Ok(Self::new(0, ts_ms, pair, exchange.to_string(), bids, asks))
    }

    /// Extract the current book state from an `OrderbookDelta`.
    ///
    /// Converts the delta manager's `BTreeMap<Decimal, Decimal>` directly
    /// into proper `Level` structs — no lossy f64 conversion.
    pub fn capture_levels(&self, ob: &OrderbookDelta) -> Self {
        let bids: BTreeMap<Decimal, Level> = ob
            .top_bids(ob.bid_depth())
            .iter()
            .enumerate()
            .map(|(idx, (price, size))| {
                (
                    *price,
                    Level::new(idx as u32, OrderSide::Bids, *price, *size, vec![]),
                )
            })
            .collect();

        let asks: BTreeMap<Decimal, Level> = ob
            .top_asks(ob.ask_depth())
            .iter()
            .enumerate()
            .map(|(idx, (price, size))| {
                (
                    *price,
                    Level::new(idx as u32, OrderSide::Asks, *price, *size, vec![]),
                )
            })
            .collect();

        Self::new(
            0,
            self.orderbook_ts_us,
            self.pair.clone(),
            self.exchange.clone(),
            bids,
            asks,
        )
    }
}

// ─────────────────────────────────────────────── Decimal ↔ f64 helpers ── //

/// Convert a `Decimal` to `f64`. Used at serialization boundaries.
#[inline]
pub fn decimal_to_f64(d: Decimal) -> f64 {
    d.to_f64().unwrap_or(0.0)
}

/// Convert an `f64` to `Decimal`. Used at construction boundaries
/// (e.g. from random generation or exchange responses that provide f64).
#[inline]
pub fn f64_to_decimal(v: f64) -> Decimal {
    Decimal::from_f64(v).unwrap_or(Decimal::ZERO)
}
