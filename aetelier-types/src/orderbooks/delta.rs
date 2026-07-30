//! Delta orderbook updates and normalization.
//!
//! Provides [`NormalizedDelta`] for exchange-agnostic representation of
//! orderbook updates and [`OrderbookDelta`] for incremental state management.

use crate::{
    TimestampUs,
    errors::OrderbookError,
    orderbooks::{OrderbookUpdate, OrderbookUpdateType},
    trading_pair::TradingPair,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, str::FromStr};

// ─────────────────────────────────────────────────────────────────────────────
// NormalizedDelta — exchange-agnostic orderbook update
// ─────────────────────────────────────────────────────────────────────────────

/// One per-order (L3) change, keyed by `order_id`. Used only by L3 venues
/// (Bitso `diff-orders`); the reconstruction engine maintains an order map and
/// aggregates these to price levels. L2 venues leave [`NormalizedDelta::orders`]
/// empty.
#[derive(Debug, Clone)]
pub struct L3Order {
    /// Venue order id (the L3 key).
    pub order_id: String,
    /// `true` = ask/offer side, `false` = bid side.
    pub is_ask: bool,
    /// Order price as a decimal string.
    pub price: String,
    /// Order size as a decimal string (ignored when `removed`).
    pub size: String,
    /// `true` → the order left the book (cancelled/completed).
    pub removed: bool,
}

/// Exchange-agnostic orderbook delta/snapshot representation.
///
/// Every exchange decoder converts its raw response into a `NormalizedDelta`
/// before feeding it to [`OrderbookDelta::process`]. This keeps all
/// exchange-specific parsing confined to the decoder layer.
#[derive(Debug, Clone)]
pub struct NormalizedDelta {
    /// Trading pair symbol (e.g. `"BTCUSDT"`, `"BTC-USD"`).
    pub symbol: String,
    /// Bid levels as `(price_str, size_str)` pairs.
    pub bids: Vec<(String, String)>,
    /// Ask levels as `(price_str, size_str)` pairs.
    pub asks: Vec<(String, String)>,
    /// Sequence / update ID for gap detection.
    pub update_id: u64,
    /// Cross-sequence number (0 if the exchange does not provide one).
    pub sequence: u64,
    /// Exchange-reported orderbook event time in Unix µs (0 if the venue does
    /// not provide one). Renamed from `event_ts`.
    pub source_orderbook_ts_us: u64,
    /// Local receipt time in Unix µs, stamped by the transport driver the
    /// instant the frame is read off the socket (for a REST snapshot-seed
    /// frame it is the pre-GET time). 0 until stamped.
    pub local_orderbook_ts_us: u64,
    /// Ping/pong round-trip on this connection in microseconds (0 if the venue
    /// has no measurable RTT yet). Stamped centrally by the transport driver.
    pub source_orderbook_rtt_us: u64,
    /// Venue-reported order-book checksum for this update, if any (OKX signed
    /// i32 / Kraken unsigned u32, both widened to i64). Verified by the
    /// reconstruction engine for ChecksumDelta venues.
    pub checksum: Option<i64>,
    /// Per-order (L3) changes, for venues whose book is keyed by order id
    /// (Bitso). Empty for L2 venues; when non-empty the `bids`/`asks` level
    /// vectors are unused and the engine derives levels from the order map.
    pub orders: Vec<L3Order>,
    /// `true` → full book reset; `false` → incremental update.
    pub is_snapshot: bool,
}

/// A Delta Orderbook state from WebSocket updates
///
/// Follows these rules:
/// - Snapshot: reset entire book
/// - Delta with size=0: delete the price level
/// - Delta with new price: insert the level
/// - Delta with existing price: update the size
///
/// # Depth pruning
///
/// When `max_depth` is `Some(n)`, the book is pruned after every snapshot
/// and delta application to retain at most `n` levels per side:
///
/// - **Bids**: the `n` highest-priced levels are kept (lowest bids removed).
/// - **Asks**: the `n` lowest-priced levels are kept (highest asks removed).
///
/// This is useful for exchanges whose wire protocol delivers unbounded
/// levels (e.g. Binance diff depth stream, Coinbase full book) when you
/// only need the top-of-book region.
///
/// When `max_depth` is `None` (the default), no pruning occurs and the
/// book grows to whatever the exchange delivers — which is the correct
/// behavior for exchanges that already filter server-side (Bybit, Kraken).
#[derive(Debug, Clone, Serialize)]
pub struct OrderbookDelta {
    /// Bid side: price -> size (BTreeMap sorts ascending, use .last() for best bid)
    pub bids: BTreeMap<Decimal, Decimal>,
    /// Ask side: price -> size (BTreeMap sorts ascending, use .first() for best ask)
    pub asks: BTreeMap<Decimal, Decimal>,
    /// Last update ID for sequencing validation
    pub last_update_id: u64,
    /// Sequence number
    pub sequence: u64,
    /// Canonical trading pair this book is tracking.
    pub pair: TradingPair,
    /// Exchange from where the data is tracked
    pub exchange: String,
    /// Whether if an initial snapshot was received
    pub initialized: bool,
    /// Count of updates applied since last snapshot
    pub delta_count: u64,
    /// Maximum levels to retain per side after each update.
    ///
    /// `None` means no cap (current behavior for server-filtered exchanges
    /// like Bybit and Kraken). `Some(n)` prunes after every `process()` call
    /// so the full book is never materialized beyond `n` levels.
    #[serde(skip)]
    pub max_depth: Option<usize>,
}

impl OrderbookDelta {
    /// Create a new orderbook delta for a trading pair.
    pub fn new(pair: TradingPair) -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_id: 0,
            sequence: 0,
            pair,
            exchange: "".to_string(),
            initialized: false,
            delta_count: 0,
            max_depth: None,
        }
    }

    /// Set the maximum depth per side (builder pattern).
    ///
    /// When `Some(n)`, the book is pruned after every `process()` call to
    /// keep at most `n` bid levels and `n` ask levels. Levels furthest from
    /// the best price are discarded.
    ///
    /// Pass `None` to disable pruning (the default).
    #[inline]
    pub fn with_max_depth(mut self, max_depth: Option<usize>) -> Self {
        self.max_depth = max_depth;
        self
    }

    /// Check if the orderbook has been initialized with a snapshot
    #[inline]
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Get the trading pair this orderbook is tracking.
    #[inline]
    pub fn pair(&self) -> &TradingPair {
        &self.pair
    }

    /// Get the exchange this orderbook is tracking.
    #[inline]
    pub fn exchange(&self) -> &str {
        &self.exchange
    }

    /// Get the last update ID
    #[inline]
    pub fn last_update_id(&self) -> u64 {
        self.last_update_id
    }

    /// Get the sequence number
    #[inline]
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Get count of deltas applied since last snapshot
    #[inline]
    pub fn delta_count(&self) -> u64 {
        self.delta_count
    }

    /// Process an exchange-agnostic orderbook update (snapshot or delta).
    ///
    /// Each exchange decoder is responsible for converting its raw response
    /// into a [`NormalizedDelta`] before calling this method.
    ///
    /// The symbol validation compares the raw `delta.symbol` against this
    /// book's [`TradingPair`] by parsing it leniently (via [`FromStr`]).
    /// If parsing fails, the raw string is accepted (the exchange decoder
    /// already validated it upstream).
    pub fn process(
        &mut self,
        delta: &NormalizedDelta,
    ) -> Result<OrderbookUpdate, OrderbookError> {
        // Validate symbol: parse the raw delta symbol and compare to our pair.
        if let Ok(delta_pair) = delta.symbol.parse::<TradingPair>()
            && delta_pair != self.pair
        {
            return Err(OrderbookError::SymbolMismatch {
                expected: self.pair.to_canonical(),
                received: delta.symbol.clone(),
            });
        }

        if delta.is_snapshot {
            self.apply_snapshot(delta)
        } else {
            self.apply_delta(delta)
        }
    }

    /// Apply a snapshot (full reset)
    fn apply_snapshot(
        &mut self,
        delta: &NormalizedDelta,
    ) -> Result<OrderbookUpdate, OrderbookError> {
        self.bids.clear();
        self.asks.clear();

        let mut bids_modified = 0;
        let mut asks_modified = 0;

        for (price_str, size_str) in &delta.bids {
            let price = Self::parse_decimal(price_str)?;
            let size = Self::parse_decimal(size_str)?;
            if size > Decimal::ZERO {
                self.bids.insert(price, size);
                bids_modified += 1;
            }
        }

        for (price_str, size_str) in &delta.asks {
            let price = Self::parse_decimal(price_str)?;
            let size = Self::parse_decimal(size_str)?;
            if size > Decimal::ZERO {
                self.asks.insert(price, size);
                asks_modified += 1;
            }
        }

        self.last_update_id = delta.update_id;
        self.sequence = delta.sequence;
        self.initialized = true;
        self.delta_count = 0;

        self.prune_to_depth();

        Ok(OrderbookUpdate {
            update_type: OrderbookUpdateType::Snapshot,
            bids_modified,
            asks_modified,
            levels_deleted: 0,
            levels_inserted: bids_modified + asks_modified,
            was_reset: true,
        })
    }

    /// Apply a delta update
    fn apply_delta(
        &mut self,
        delta: &NormalizedDelta,
    ) -> Result<OrderbookUpdate, OrderbookError> {
        if !self.initialized {
            return Err(OrderbookError::NotInitialized);
        }

        let mut bids_modified = 0;
        let mut asks_modified = 0;
        let mut levels_deleted = 0;
        let mut levels_inserted = 0;

        for (price_str, size_str) in &delta.bids {
            let price = Self::parse_decimal(price_str)?;
            let size = Self::parse_decimal(size_str)?;
            let (deleted, inserted) = Self::apply_level(&mut self.bids, price, size);
            bids_modified += 1;
            if deleted {
                levels_deleted += 1;
            }
            if inserted {
                levels_inserted += 1;
            }
        }

        for (price_str, size_str) in &delta.asks {
            let price = Self::parse_decimal(price_str)?;
            let size = Self::parse_decimal(size_str)?;
            let (deleted, inserted) = Self::apply_level(&mut self.asks, price, size);
            asks_modified += 1;
            if deleted {
                levels_deleted += 1;
            }
            if inserted {
                levels_inserted += 1;
            }
        }

        self.last_update_id = delta.update_id;
        self.sequence = delta.sequence;
        self.delta_count += 1;

        self.prune_to_depth();

        Ok(OrderbookUpdate {
            update_type: OrderbookUpdateType::Delta,
            bids_modified,
            asks_modified,
            levels_deleted,
            levels_inserted,
            was_reset: false,
        })
    }

    /// Produces an [`OrderbookSnapshot`] from the current delta state.
    ///
    /// This is an internal helper that extracts all relevant data from an
    /// [`OrderbookDelta`] into a serializable snapshot structure.
    ///
    /// # Arguments
    ///
    /// * `ob` - Mutable Reference to Self `OrderbookDelta` to generate the snapshot from
    ///
    /// # Returns
    ///
    /// An [`OrderbookSnapshot`] containing:
    /// - Current timestamp (capture time, not exchange time)
    /// - All price levels as string tuples to preserve decimal precision
    /// - Derived metrics rounded to appropriate decimal places
    pub fn produce_snapshot(ob: &mut Self) -> OrderbookSnapshot {
        let bids: Vec<[String; 2]> = ob
            .top_bids(ob.bid_depth())
            .iter()
            .map(|(p, s)| [p.to_string(), s.to_string()])
            .collect();

        let asks: Vec<[String; 2]> = ob
            .top_asks(ob.ask_depth())
            .iter()
            .map(|(p, s)| [p.to_string(), s.to_string()])
            .collect();

        OrderbookSnapshot {
            timestamp_us: TimestampUs::now().as_micros(),
            pair: ob.pair().clone(),
            update_id: ob.last_update_id(),
            sequence: ob.sequence(),
            delta_count: ob.delta_count(),
            bid_depth: ob.bid_depth(),
            ask_depth: ob.ask_depth(),
            mid_price: ob.mid_price().map(|d| d.round_dp(8).to_string()),
            spread: ob.spread().map(|d| d.round_dp(8).to_string()),
            spread_bps: ob.spread_bps().map(|d| d.round_dp(4).to_string()),
            volume_imbalance: ob.volume_imbalance().map(|d| d.round_dp(6).to_string()),
            total_bid_volume: ob.total_bid_volume().round_dp(8).to_string(),
            total_ask_volume: ob.total_ask_volume().round_dp(8).to_string(),
            bids,
            asks,
        }
    }

    /// Prune both sides of the book to `max_depth` levels (if configured).
    ///
    /// - **Bids** (BTreeMap ascending): keep the *last* `n` entries (highest
    ///   prices = closest to best bid). Remove from the front (lowest prices).
    /// - **Asks** (BTreeMap ascending): keep the *first* `n` entries (lowest
    ///   prices = closest to best ask). Remove from the back (highest prices).
    ///
    /// This is a no-op when `max_depth` is `None`.
    #[inline]
    fn prune_to_depth(&mut self) {
        let Some(max) = self.max_depth else { return };
        // Bids: discard the lowest-priced levels (front of ascending BTreeMap)
        while self.bids.len() > max {
            self.bids.pop_first();
        }
        // Asks: discard the highest-priced levels (back of ascending BTreeMap)
        while self.asks.len() > max {
            self.asks.pop_last();
        }
    }

    /// Apply a single level update to a side of the book
    /// Returns (was_deleted, was_inserted)
    fn apply_level(
        book_side: &mut BTreeMap<Decimal, Decimal>,
        price: Decimal,
        size: Decimal,
    ) -> (bool, bool) {
        if size == Decimal::ZERO {
            // Delete the entry (Level was dropped)
            let existed = book_side.remove(&price).is_some();
            (existed, false)
        } else {
            match book_side.entry(price) {
                std::collections::btree_map::Entry::Occupied(mut e) => {
                    // Update existing entry (Level was modified)
                    e.insert(size);
                    (false, false)
                }
                std::collections::btree_map::Entry::Vacant(e) => {
                    // Insert new entry (Level was created)
                    e.insert(size);
                    (false, true)
                }
            }
        }
    }

    /// Parse a string to Decimal
    fn parse_decimal(s: &str) -> Result<Decimal, OrderbookError> {
        Decimal::from_str(s).map_err(|e| OrderbookError::ParseError(e.to_string()))
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Book State Accessors
    // ═══════════════════════════════════════════════════════════════════════════

    /// Get the best bid (highest bid price)
    #[inline]
    pub fn best_bid(&self) -> Option<(Decimal, Decimal)> {
        self.bids.last_key_value().map(|(&p, &s)| (p, s))
    }

    /// Get the best ask (lowest ask price)
    #[inline]
    pub fn best_ask(&self) -> Option<(Decimal, Decimal)> {
        self.asks.first_key_value().map(|(&p, &s)| (p, s))
    }

    /// Get best bid/ask as (bid_price, bid_size, ask_price, ask_size)
    pub fn bbo(&self) -> Option<(Decimal, Decimal, Decimal, Decimal)> {
        match (self.best_bid(), self.best_ask()) {
            (Some((bp, bs)), Some((ap, az))) => Some((bp, bs, ap, az)),
            _ => None,
        }
    }

    /// Get mid price
    pub fn mid_price(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some((bid, _)), Some((ask, _))) => Some((bid + ask) / Decimal::TWO),
            _ => None,
        }
    }

    /// Get spread
    pub fn spread(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some((bid, _)), Some((ask, _))) => Some(ask - bid),
            _ => None,
        }
    }

    /// Get spread in basis points
    pub fn spread_bps(&self) -> Option<Decimal> {
        let spread = self.spread()?;
        let mid = self.mid_price()?;
        if mid > Decimal::ZERO {
            Some((spread / mid) * Decimal::from(10_000))
        } else {
            None
        }
    }

    /// Get number of bid levels
    #[inline]
    pub fn bid_depth(&self) -> usize {
        self.bids.len()
    }

    /// Get number of ask levels
    #[inline]
    pub fn ask_depth(&self) -> usize {
        self.asks.len()
    }

    /// Get total bid volume
    pub fn total_bid_volume(&self) -> Decimal {
        self.bids.values().copied().sum()
    }

    /// Get total ask volume
    pub fn total_ask_volume(&self) -> Decimal {
        self.asks.values().copied().sum()
    }

    /// Volume imbalance: (bid - ask) / (bid + ask) ∈ [-1, 1]
    pub fn volume_imbalance(&self) -> Option<Decimal> {
        let bid_vol = self.total_bid_volume();
        let ask_vol = self.total_ask_volume();
        let total = bid_vol + ask_vol;
        if total > Decimal::ZERO {
            Some((bid_vol - ask_vol) / total)
        } else {
            None
        }
    }

    /// Get top N bid levels (highest to lowest price)
    pub fn top_bids(&self, n: usize) -> Vec<(Decimal, Decimal)> {
        self.bids
            .iter()
            .rev()
            .take(n)
            .map(|(&p, &s)| (p, s))
            .collect()
    }

    /// Get top N ask levels (lowest to highest price)
    pub fn top_asks(&self, n: usize) -> Vec<(Decimal, Decimal)> {
        self.asks.iter().take(n).map(|(&p, &s)| (p, s)).collect()
    }

    /// Get bid volume within a price range from best bid
    pub fn bid_volume_within(&self, depth: Decimal) -> Decimal {
        let Some((best_bid, _)) = self.best_bid() else {
            return Decimal::ZERO;
        };
        let threshold = best_bid - depth;
        self.bids.range(threshold..).map(|(_, &size)| size).sum()
    }

    /// Get ask volume within a price range from best ask
    pub fn ask_volume_within(&self, depth: Decimal) -> Decimal {
        let Some((best_ask, _)) = self.best_ask() else {
            return Decimal::ZERO;
        };
        let threshold = best_ask + depth;
        self.asks.range(..=threshold).map(|(_, &size)| size).sum()
    }

    /// Create an OrderbookDelta from pre-built BTreeMaps (minimal metadata)
    ///
    /// Used when loading from CSV or Parquet where only price/size data is available.
    pub fn from_maps(
        pair: TradingPair,
        exchange: impl Into<String>,
        bids: BTreeMap<Decimal, Decimal>,
        asks: BTreeMap<Decimal, Decimal>,
    ) -> Self {
        let bid_count = bids.len();
        let ask_count = asks.len();

        Self {
            pair,
            exchange: exchange.into(),
            bids,
            asks,
            last_update_id: 0,
            sequence: 0,
            initialized: bid_count > 0 || ask_count > 0,
            delta_count: 0,
            max_depth: None,
        }
    }

    /// Create an OrderbookDelta from a full snapshot with metadata
    ///
    /// Used when loading from JSON where all metadata is preserved.
    pub fn from_snapshot(
        pair: TradingPair,
        exchange: impl Into<String>,
        bids: BTreeMap<Decimal, Decimal>,
        asks: BTreeMap<Decimal, Decimal>,
        update_id: u64,
        sequence: u64,
        delta_count: u64,
    ) -> Self {
        let bid_count = bids.len();
        let ask_count = asks.len();

        Self {
            pair,
            exchange: exchange.into(),
            bids,
            asks,
            last_update_id: update_id,
            sequence,
            initialized: bid_count > 0 || ask_count > 0,
            delta_count,
            max_depth: None,
        }
    }

    /// Create an OrderbookDelta from vectors of (price, size) tuples
    ///
    /// Useful for testing or manual construction.
    pub fn from_levels(
        pair: TradingPair,
        exchange: impl Into<String>,
        bids: impl IntoIterator<Item = (Decimal, Decimal)>,
        asks: impl IntoIterator<Item = (Decimal, Decimal)>,
    ) -> Self {
        let bids: BTreeMap<Decimal, Decimal> = bids.into_iter().collect();
        let asks: BTreeMap<Decimal, Decimal> = asks.into_iter().collect();
        Self::from_maps(pair, exchange, bids, asks)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Convenience helper for tests: canonical BTC/USDT pair.
    fn btcusdt() -> TradingPair {
        TradingPair::new("BTC", "USDT")
    }

    /// Helper: generate `n` ascending bid levels starting at `base_price`,
    /// stepping down by 1 (so highest = `base_price`, lowest = `base_price - n + 1`).
    fn make_bids(base_price: i64, n: usize) -> Vec<(Decimal, Decimal)> {
        (0..n)
            .map(|i| {
                let price = Decimal::from(base_price - i as i64);
                let size = Decimal::ONE;
                (price, size)
            })
            .collect()
    }

    /// Helper: generate `n` ascending ask levels starting at `base_price`,
    /// stepping up by 1 (so lowest = `base_price`, highest = `base_price + n - 1`).
    fn make_asks(base_price: i64, n: usize) -> Vec<(Decimal, Decimal)> {
        (0..n)
            .map(|i| {
                let price = Decimal::from(base_price + i as i64);
                let size = Decimal::ONE;
                (price, size)
            })
            .collect()
    }

    // ── prune_to_depth tests ─────────────────────────────────────────────

    #[test]
    fn prune_none_is_noop() {
        let ob = OrderbookDelta::from_levels(
            btcusdt(),
            "test",
            make_bids(100, 50),
            make_asks(101, 50),
        );
        assert_eq!(ob.bids.len(), 50);
        assert_eq!(ob.asks.len(), 50);
        // max_depth is None by default — no pruning on process
    }

    #[test]
    fn prune_on_snapshot_caps_both_sides() {
        let bids = make_bids(100, 50); // prices 51..=100
        let asks = make_asks(101, 50); // prices 101..=150

        let mut ob = OrderbookDelta::new(btcusdt()).with_max_depth(Some(10));

        let snap = NormalizedDelta {
            symbol: "BTCUSDT".to_string(),
            bids: bids
                .iter()
                .map(|(p, s)| (p.to_string(), s.to_string()))
                .collect(),
            asks: asks
                .iter()
                .map(|(p, s)| (p.to_string(), s.to_string()))
                .collect(),
            update_id: 1,
            sequence: 0,
            source_orderbook_ts_us: 0,
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            checksum: None,
            orders: Vec::new(),
            is_snapshot: true,
        };

        let _ = ob.process(&snap).unwrap();

        assert_eq!(ob.bids.len(), 10, "bids should be pruned to 10 levels");
        assert_eq!(ob.asks.len(), 10, "asks should be pruned to 10 levels");

        // Bids: kept the 10 highest (91..=100)
        let best_bid = *ob.bids.keys().last().unwrap();
        let worst_bid = *ob.bids.keys().next().unwrap();
        assert_eq!(best_bid, Decimal::from(100));
        assert_eq!(worst_bid, Decimal::from(91));

        // Asks: kept the 10 lowest (101..=110)
        let best_ask = *ob.asks.keys().next().unwrap();
        let worst_ask = *ob.asks.keys().last().unwrap();
        assert_eq!(best_ask, Decimal::from(101));
        assert_eq!(worst_ask, Decimal::from(110));
    }

    #[test]
    fn prune_on_delta_preserves_cap() {
        // Start with a capped snapshot of 10 levels.
        let mut ob = OrderbookDelta::new(btcusdt()).with_max_depth(Some(10));

        let snap = NormalizedDelta {
            symbol: "BTCUSDT".to_string(),
            bids: make_bids(100, 10)
                .iter()
                .map(|(p, s)| (p.to_string(), s.to_string()))
                .collect(),
            asks: make_asks(101, 10)
                .iter()
                .map(|(p, s)| (p.to_string(), s.to_string()))
                .collect(),
            update_id: 1,
            sequence: 0,
            source_orderbook_ts_us: 0,
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            checksum: None,
            orders: Vec::new(),
            is_snapshot: true,
        };
        let _ = ob.process(&snap).unwrap();
        assert_eq!(ob.bids.len(), 10);
        assert_eq!(ob.asks.len(), 10);

        // Delta: add a new bid at 101 (new best) and a new ask at 100 (new best).
        // This would push both sides to 11 levels, so pruning should kick in.
        let delta = NormalizedDelta {
            symbol: "BTCUSDT".to_string(),
            bids: vec![("101".to_string(), "5".to_string())],
            asks: vec![("100".to_string(), "5".to_string())],
            update_id: 2,
            sequence: 0,
            source_orderbook_ts_us: 0,
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            checksum: None,
            orders: Vec::new(),
            is_snapshot: false,
        };
        let _ = ob.process(&delta).unwrap();

        assert_eq!(ob.bids.len(), 10, "bids capped at 10 after delta");
        assert_eq!(ob.asks.len(), 10, "asks capped at 10 after delta");

        // New best bid should be 101.
        assert_eq!(*ob.bids.keys().last().unwrap(), Decimal::from(101));
        // The old worst bid (91) should have been evicted.
        assert!(!ob.bids.contains_key(&Decimal::from(91)));

        // New best ask should be 100.
        assert_eq!(*ob.asks.keys().next().unwrap(), Decimal::from(100));
        // The old worst ask (110) should have been evicted.
        assert!(!ob.asks.contains_key(&Decimal::from(110)));
    }

    #[test]
    fn prune_zero_removes_side_is_deleted() {
        // Zero depth: remove all levels (degenerate but should not panic).
        let mut ob = OrderbookDelta::from_levels(
            btcusdt(),
            "test",
            make_bids(100, 5),
            make_asks(101, 5),
        )
        .with_max_depth(Some(0));

        // Manually trigger prune (happens internally on process, but test directly).
        ob.prune_to_depth();
        assert_eq!(ob.bids.len(), 0);
        assert_eq!(ob.asks.len(), 0);
    }

    #[test]
    fn prune_depth_larger_than_book_is_noop() {
        let mut ob = OrderbookDelta::from_levels(
            btcusdt(),
            "test",
            make_bids(100, 5),
            make_asks(101, 5),
        )
        .with_max_depth(Some(100));

        ob.prune_to_depth();
        assert_eq!(ob.bids.len(), 5, "5 bids should remain (< max_depth 100)");
        assert_eq!(ob.asks.len(), 5, "5 asks should remain (< max_depth 100)");
    }

    #[test]
    fn delta_removal_does_not_overprune() {
        // Book with exactly 10 levels, max_depth = 10.
        // A delta removes one level (size = 0) → should be 9, no pruning needed.
        let mut ob = OrderbookDelta::new(btcusdt()).with_max_depth(Some(10));

        let snap = NormalizedDelta {
            symbol: "BTCUSDT".to_string(),
            bids: make_bids(100, 10)
                .iter()
                .map(|(p, s)| (p.to_string(), s.to_string()))
                .collect(),
            asks: make_asks(101, 10)
                .iter()
                .map(|(p, s)| (p.to_string(), s.to_string()))
                .collect(),
            update_id: 1,
            sequence: 0,
            source_orderbook_ts_us: 0,
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            checksum: None,
            orders: Vec::new(),
            is_snapshot: true,
        };
        let _ = ob.process(&snap).unwrap();

        // Remove the best bid (100) by sending size=0.
        let delta = NormalizedDelta {
            symbol: "BTCUSDT".to_string(),
            bids: vec![("100".to_string(), "0".to_string())],
            asks: vec![],
            update_id: 2,
            sequence: 0,
            source_orderbook_ts_us: 0,
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            checksum: None,
            orders: Vec::new(),
            is_snapshot: false,
        };
        let _ = ob.process(&delta).unwrap();

        assert_eq!(ob.bids.len(), 9, "one bid removed, 9 remain");
        assert_eq!(ob.asks.len(), 10, "asks untouched");
        assert!(!ob.bids.contains_key(&Decimal::from(100)));
    }
}

/// Full orderbook snapshot for JSON serialization.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OrderbookSnapshot {
    /// Capture timestamp in microseconds.
    pub timestamp_us: u64,
    /// Canonical trading pair.
    pub pair: TradingPair,
    /// Last update ID.
    pub update_id: u64,
    /// Sequence number.
    pub sequence: u64,
    /// Number of deltas applied since last snapshot.
    pub delta_count: u64,
    /// Number of bid levels.
    pub bid_depth: usize,
    /// Number of ask levels.
    pub ask_depth: usize,
    /// Mid-price as a string (if both sides exist).
    pub mid_price: Option<String>,
    /// Spread as a string (if both sides exist).
    pub spread: Option<String>,
    /// Spread in basis points as a string (if both sides exist).
    pub spread_bps: Option<String>,
    /// Volume imbalance ratio as a string (if both sides exist).
    pub volume_imbalance: Option<String>,
    /// Total bid volume as a string.
    pub total_bid_volume: String,
    /// Total ask volume as a string.
    pub total_ask_volume: String,
    /// Bid levels as [price, size] string pairs.
    pub bids: Vec<[String; 2]>,
    /// Ask levels as [price, size] string pairs.
    pub asks: Vec<[String; 2]>,
}

impl OrderbookSnapshot {
    /// Pretty print the orderbook
    pub fn display(&self, levels: usize) {
        println!("╔══════════════════════════════════════════════════════════════╗");
        println!(
            "║  {} | update_id: {} | seq: {} | deltas: {}",
            self.pair, self.update_id, self.sequence, self.delta_count
        );
        println!("╠══════════════════════════════════════════════════════════════╣");
        println!(
            "║  {:^15} │ {:^12} ║ {:^12} │ {:^15}  ║",
            "ASK PRICE", "ASK SIZE", "BID SIZE", "BID PRICE"
        );
        println!("╠══════════════════════════════════════════════════════════════╣");

        let display_levels = levels.min(self.bids.len()).min(self.asks.len());

        // Show asks in reverse (highest to lowest) then bids (highest to lowest)
        let asks_rev: Vec<_> = self.asks.iter().take(display_levels).collect();

        for (i, [ask_price, ask_size]) in asks_rev.iter().rev().enumerate() {
            if i < display_levels {
                let [bid_price, bid_size] = &self.bids[display_levels - 1 - i];
                println!(
                    "║  {:>15} │ {:>12} ║ {:>12} │ {:>15}  ║",
                    ask_price, ask_size, bid_size, bid_price
                );
            }
        }
        println!("╚══════════════════════════════════════════════════════════════╝");
    }
}
