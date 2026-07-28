use crate::orders::{Order, OrderSide};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Represents a price level in an order book.
///
/// The `Level` struct contains details about a specific price level, including
/// its unique identifier, side (buy/sell), price, total volume at that price,
/// and a vector of orders associated with that level.
#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Level {
    /// Unique identifier for this price level.
    pub level_id: u32,
    /// Side of the order book (buy/sell).
    pub side: OrderSide,
    /// Price at this level.
    pub price: Decimal,
    /// Total volume at this price.
    pub volume: Decimal,
    /// Orders at this level.
    pub orders: Vec<Order>,
}

impl Level {
    /// Creates a new instance of `Level`.
    ///
    /// # Parameters
    ///
    /// - `level_id`: The unique identifier for the price level.
    /// - `side`: The side of the order book, either `Side::Bids` or
    ///   `Side::Asks`.
    /// - `price`: The price at which orders are placed at this level.
    /// - `volume`: The total volume of orders at this price level.
    /// - `orders`: A vector of `Order` representing the orders at this level.
    ///
    /// # Returns
    ///
    /// Returns a new `Level` instance with the specified parameters.
    pub fn new(
        level_id: u32,
        side: OrderSide,
        price: Decimal,
        volume: Decimal,
        orders: Vec<Order>,
    ) -> Self {
        Level {
            level_id,
            side,
            price,
            volume,
            orders,
        }
    }
}
