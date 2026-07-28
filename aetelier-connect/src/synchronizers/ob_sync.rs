//! src/syncrhonizers/ob_sync.rs
//!
//! Time-synchronized orderbook snapshot sampling.
//!
//! Produces uniformly-spaced `Orderbook` snapshots from an irregular stream
//! of full-book updates by selecting the most recent snapshot at each discrete
//! grid point.
//!
//! # Model
//!
//! ```text
//!   stream:  S₁  S₂     S₃  S₄  S₅        S₆   S₇
//!   time: ───┼───┼───────┼───┼───┼──────────┼────┼──→
//!   grid: ───────|───────────|───────────|───────────|
//!            t₀         t₁         t₂         t₃
//!
//!   output:       S₂          S₅          S₇
//!                (@ t₁)      (@ t₂)      (@ t₃ via finalize)
//! ```
//!
//! At each grid boundary, the **last snapshot received before the crossing**
//! is emitted with its `orderbook_ts_us` reassigned to the grid-aligned
//! UTC epoch microsecond timestamp. Gaps (periods with no updates) are forward-filled
//! from the previous snapshot.

use aetelier_types::{
    Level, OrderSide,
    orderbooks::{Orderbook, OrderbookDelta},
    trading_pair::TradingPair,
};

use std::collections::HashMap;
use tracing::warn;

/// Extract the current book state from an `OrderbookDelta` as `Vec<Level>` pairs.
///
/// Converts the delta manager's `BTreeMap<Decimal, Decimal>` representation
/// into proper `Level` structs with indexed level IDs. Orders vectors are
/// left empty since the manager tracks aggregate price/size only.
///
/// Prices and sizes are kept as `Decimal` (no lossy f64 conversion).
pub fn capture_levels(ob: &OrderbookDelta) -> (Vec<Level>, Vec<Level>) {
    let bids: Vec<Level> = ob
        .top_bids(ob.bid_depth())
        .iter()
        .enumerate()
        .map(|(idx, (price, size))| {
            Level::new(idx as u32, OrderSide::Bids, *price, *size, vec![])
        })
        .collect();

    let asks: Vec<Level> = ob
        .top_asks(ob.ask_depth())
        .iter()
        .enumerate()
        .map(|(idx, (price, size))| {
            Level::new(idx as u32, OrderSide::Asks, *price, *size, vec![])
        })
        .collect();

    (bids, asks)
}

/// Maximum number of periods to forward-fill during a gap.
/// Prevents unbounded memory growth from network disconnects.
const MAX_GAP_FILL: u64 = 10_000;

/// Produces uniformly-spaced `Orderbook` snapshots from an irregular update stream.
///
/// Operates on a discrete time grid with spacing `period_us`. On each incoming
/// update, checks whether one or more grid points have been crossed since the
/// last update for that symbol. If so, captures the current book state as a
/// full `Orderbook` for each crossed grid point.
///
/// Each buffered `Orderbook` has its `orderbook_ts_us` set to the grid-aligned
/// UTC epoch microsecond timestamp (i.e. `period_index × period_us`).
///
/// # Timestamp Resolution
///
/// Exchange timestamps from exchange's millisecond resolution. The effective
/// timestamps are UTC epoch microseconds (platform standard). Periods finer than 1ms
/// will still work but cannot distinguish sub-millisecond ordering.
pub struct ObSynchronizer {
    /// Grid spacing in microseconds
    pub period_us: u64,
    /// Last completed period index per symbol
    pub last_period: HashMap<String, u64>,
    /// Most recent snapshot per symbol (updated every message).
    last_snapshot: HashMap<String, Orderbook>,
    /// Buffered full orderbook snapshots awaiting flush
    pub buffer: Vec<Orderbook>,
    /// Lifetime capture count (across flushes)
    pub total_captured: usize,
    /// Events dropped because they arrived after `finalize` closed the stream.
    pub late_events_dropped: u64,
    /// Set by `finalize`: the stream is closed (terminal). Further `finalize`
    /// calls are no-ops; further `on_*` events are dropped and counted.
    finalized: bool,
}

impl ObSynchronizer {
    pub fn new(period_us: u64) -> Self {
        assert!(period_us > 0, "period_us must be positive");
        Self {
            period_us,
            last_period: HashMap::new(),
            last_snapshot: HashMap::new(),
            buffer: Vec::new(),
            total_captured: 0,
            late_events_dropped: 0,
            finalized: false,
        }
    }

    /// Whether `finalize` has closed the stream (terminal FINALIZED state).
    pub fn is_finalized(&self) -> bool {
        self.finalized
    }

    fn drop_post_finalize(&mut self) {
        self.late_events_dropped += 1;
        warn!(
            total = self.late_events_dropped,
            "ob_sync.event_after_finalize_dropped"
        );
    }

    /// Process an incoming update. Call this BEFORE applying the delta to the
    /// orderbook so the captured state reflects the last update in the
    /// completed period, not the first update in the new one.
    ///
    /// `capture_fn` is invoked lazily — only when a grid boundary is crossed —
    /// and returns `(bids, asks)` as `Vec<Level>`. The synchronizer then
    /// constructs full `Orderbook` objects for each crossed grid point.
    ///
    /// Returns the number of snapshots captured (0 if still within the same period).
    pub fn on_update<F>(
        &mut self,
        pair: &TradingPair,
        exchange: &str,
        exchange_ts_us: u64,
        capture_fn: F,
    ) -> usize
    where
        F: FnOnce() -> (Vec<Level>, Vec<Level>),
    {
        if self.finalized {
            self.drop_post_finalize();
            return 0;
        }
        let ts_us = exchange_ts_us;
        let current_period = ts_us / self.period_us;

        let key = pair.to_canonical();
        let prev_period = match self.last_period.get(&key) {
            Some(&p) => p,
            None => {
                // First update for this symbol — initialize, no capture yet
                self.last_period.insert(key, current_period);
                return 0;
            }
        };

        if current_period <= prev_period {
            return 0;
        }

        // We crossed at least one grid boundary. Capture the current state.
        let gap = current_period - prev_period;
        let (bids, asks) = capture_fn();

        let fill_count = gap.min(MAX_GAP_FILL);
        if gap > MAX_GAP_FILL {
            warn!(
                "[{}] Gap of {} periods exceeds MAX_GAP_FILL ({}), \
                 filling last {} only",
                pair, gap, MAX_GAP_FILL, MAX_GAP_FILL
            );
        }

        // Build a full Orderbook for each crossed grid point.
        // All share the same book state; only `orderbook_ts_us` differs.
        let start = prev_period + 1;
        let end = prev_period + fill_count; // inclusive
        for p in start..=end {
            self.buffer.push(Orderbook::from_levels(
                0,
                p * self.period_us,
                pair.clone(),
                exchange.to_string(),
                bids.clone(),
                asks.clone(),
            ));
        }

        self.last_period.insert(key, current_period);
        self.total_captured += fill_count as usize;
        fill_count as usize
    }

    /// Feed a full-book snapshot received from the exchange.
    ///
    /// `exchange_ts_us` is the exchange-reported timestamp in **UTC epoch microseconds**
    /// (i.e. `BybitOrderbookResponse.orderbook_ts_us`).
    ///
    /// If one or more grid boundaries have been crossed since the previous
    /// call for this symbol, the **previously stored** snapshot is emitted
    /// for each crossed grid point (forward-filled for gaps).
    ///
    /// Returns the number of grid-aligned snapshots appended to the buffer
    /// (0 if still within the same period).
    pub fn on_snapshot(
        &mut self,
        pair: &TradingPair,
        exchange_ts_us: u64,
        snapshot: Orderbook,
    ) -> usize {
        if self.finalized {
            self.drop_post_finalize();
            return 0;
        }
        let ts_us = exchange_ts_us as u128;
        let current_period = (ts_us / self.period_us as u128) as u64;

        let key = pair.to_canonical();
        let prev_period = match self.last_period.get(&key) {
            Some(&p) => p,
            None => {
                // First update for this symbol — anchor the grid.
                self.last_period.insert(key.clone(), current_period);
                self.last_snapshot.insert(key, snapshot);
                return 0;
            }
        };

        if current_period <= prev_period {
            // Same period — just refresh the stored snapshot.
            self.last_snapshot.insert(key, snapshot);
            return 0;
        }

        // Grid boundary crossed
        let gap = current_period - prev_period;
        let fill_count = gap.min(MAX_GAP_FILL);

        if gap > MAX_GAP_FILL {
            warn!(
                "[{}] gap of {} periods exceeds MAX_GAP_FILL ({}), capping",
                pair, gap, MAX_GAP_FILL,
            );
        }

        // Emit the previous snapshot at each crossed grid point.
        if let Some(prev_ob) = self.last_snapshot.get(&key) {
            for p in (prev_period + 1)..=(prev_period + fill_count) {
                let mut ob = prev_ob.clone();
                ob.orderbook_ts_us = p * self.period_us;
                self.buffer.push(ob);
            }
        }

        self.last_period.insert(key.clone(), current_period);
        self.last_snapshot.insert(key, snapshot);
        self.total_captured += fill_count as usize;
        fill_count as usize
    }

    /// Emit the final snapshot for every tracked symbol, closing the
    /// current (incomplete) period. Call when the stream ends.
    pub fn finalize(&mut self) {
        if self.finalized {
            return; // idempotent: FINALIZED is terminal.
        }
        self.finalized = true;
        let periods: Vec<(String, u64)> = self
            .last_period
            .iter()
            .map(|(s, &p)| (s.clone(), p))
            .collect();

        for (symbol, period) in periods {
            if let Some(ob) = self.last_snapshot.get(&symbol) {
                let mut final_ob = ob.clone();
                final_ob.orderbook_ts_us = (period + 1) * self.period_us;
                self.buffer.push(final_ob);
                self.total_captured += 1;
            }
        }
    }

    #[inline]
    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }

    #[inline]
    pub fn total_captured(&self) -> usize {
        self.total_captured
    }

    /// Drain the buffer, returning all accumulated snapshots.
    pub fn drain(&mut self) -> Vec<Orderbook> {
        std::mem::take(&mut self.buffer)
    }

    /// Drain the buffer and write all accumulated snapshots to Parquet.
    ///
    /// **Requires** the `parquet` feature on `aetelier_io`. Without it this
    /// method always returns an error.
    ///
    /// After a successful write the internal buffer is empty and
    /// `total_captured` is unchanged (it is a lifetime counter).
    pub fn flush_to_parquet(
        &mut self,
        _output_dir: &std::path::Path,
        _mode: &str,
    ) -> anyhow::Result<()> {
        anyhow::bail!(
            "parquet support requires calling via \
             aetelier_io::flush::FlushObSyncToParquet"
        )
    }
}
