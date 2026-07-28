//! Multi-source time synchronizer with configurable clock modes.
//!
//! Aligns orderbook snapshots, trades, liquidations, funding rates, and open
//! interest to a uniform time grid. Produces [`MarketSnapshot`] at each period.
//!
//! # Clock Modes
//!
//! The synchronizer supports four clock modes via [`ClockMode`]:
//!
//! - **OrderbookDriven** (default): Grid periods are triggered by orderbook
//!   timestamp crossings, matching the original `MarketSynchronizer` behavior.
//! - **TradeDriven**: Grid periods are triggered by trade timestamps.
//! - **LiquidationDriven**: Grid periods are triggered by liquidation timestamps.
//! - **ExternalClock**: Grid periods are driven by explicit `on_time()` calls
//!   with an external UTC-epoch-microsecond timestamp.
//!
//! # Semantics
//!
//! - **Orderbook, funding rate, OI** are *state-based*: the latest value is
//!   carried forward into each period.
//! - **Trades, liquidations** are *event-based*: all events within a period
//!   are collected into a single bucket.
//!
//! # Usage
//!
//! ```ignore
//! use aetelier_connect::synchronizers::{MarketSynchronizer, ClockMode};
//!
//! // Orderbook-driven (default, backward compatible)
//! let mut sync = MarketSynchronizer::new(100_000_000);
//!
//! // External clock
//! let mut sync = MarketSynchronizer::external_clock(100_000_000);
//! sync.on_orderbook("BTCUSDT", exchange_ts_us, orderbook); // updates state only
//! sync.on_trade(trade);                            // accumulates only
//! sync.on_time(ts_us);                             // drives the grid
//!
//! // Trade-driven
//! let mut sync = MarketSynchronizer::trade_driven(100_000_000);
//! sync.on_orderbook("BTCUSDT", exchange_ts_us, orderbook); // updates state only
//! sync.on_trade(trade);                            // accumulates + drives grid
//!
//! // Liquidation-driven
//! let mut sync = MarketSynchronizer::liquidation_driven(100_000_000);
//! sync.on_liquidation(liq);                        // accumulates + drives grid
//!
//! // At end of stream:
//! sync.finalize();
//! let snapshots: Vec<MarketSnapshot> = sync.drain();
//! ```

use aetelier_types::{
    funding::FundingRate, liquidations::Liquidation, open_interest::OpenInterest,
    orderbooks::Orderbook, snapshots::MarketSnapshot, trades::Trade,
    trading_pair::TradingPair,
};

use std::collections::{BTreeMap, HashMap};
use tracing::warn;

/// Maximum periods to forward-fill during gaps.
const MAX_GAP_FILL: u64 = 10_000;

// ---------------------------------------------------------------------------
// ClockMode
// ---------------------------------------------------------------------------

/// One emitted period's events, grouped by timestamp membership.
#[derive(Default)]
struct PeriodEvents {
    trades: Vec<Trade>,
    liquidations: Vec<Liquidation>,
    funding: Vec<FundingRate>,
    open_interest: Vec<OpenInterest>,
}

/// The grid clock mode — single definition in `aetelier_types` (the
/// duplicate enum previously declared here is retired; OQ-SY-8).
pub use aetelier_types::synchronizers::ClockMode;

// ---------------------------------------------------------------------------
// MarketSynchronizer
// ---------------------------------------------------------------------------

/// Multi-source time synchronizer that produces [`MarketSnapshot`] at each
/// grid period, combining all data sources.
///
/// See [module-level documentation](self) for usage examples.
pub struct MarketSynchronizer {
    /// Grid spacing in microseconds (the platform timestamp standard).
    pub period_us: u64,

    /// Emission hold-back window W (µs, default 0 = emit at the boundary).
    ///
    /// A row becomes ripe only once the driving clock has advanced W PAST its
    /// closing boundary, so late-but-correctly-timestamped events (REST-
    /// recovered trades from live reconciliation, venue RTT ~300ms) still land
    /// in their true — still-buffered — rows. Applied ONLY to the clock-
    /// ripeness computation; event→row membership keys on the event's own
    /// timestamp and must never shift.
    emission_delay_us: u64,

    /// Which data feed drives the grid clock.
    clock_mode: ClockMode,

    // -- OrderbookDriven state (per-symbol tracking) -------------------------
    /// Last completed period index per symbol.
    /// Only used in [`ClockMode::OrderbookDriven`] mode.
    last_period: HashMap<String, u64>,

    // -- Global clock state (TradeDriven / LiquidationDriven / ExternalClock)
    /// Global clock period index. `None` until the first clock-driving call.
    /// Used by all modes except [`ClockMode::OrderbookDriven`].
    clock_period: Option<u64>,

    // -- Data accumulators ---------------------------------------------------
    /// Per-symbol book history keyed by grid period (last book per period,
    /// pruned after emission) — snapshots embed the as-of-boundary book, so
    /// a book arriving inside the still-open period can never leak into a
    /// completed period's row.
    books: HashMap<String, BTreeMap<u64, Orderbook>>,

    /// Trades accumulated in the current (incomplete) period.
    current_trades: Vec<Trade>,

    /// Liquidations accumulated in the current (incomplete) period.
    current_liquidations: Vec<Liquidation>,

    /// Most recent funding rates (state-based, carried forward).
    current_funding_rates: Vec<FundingRate>,

    /// Most recent open interest records (state-based, carried forward).
    current_open_interests: Vec<OpenInterest>,

    // -- Output --------------------------------------------------------------
    /// Buffered output snapshots.
    pub buffer: Vec<MarketSnapshot>,

    /// Total snapshots produced across all drains.
    pub total_captured: usize,

    /// Events dropped because their timestamp belongs to an already-emitted
    /// period (late data / pre-anchor backlog), or arrived after `finalize`
    /// closed the stream. Never silently re-homed.
    pub late_events_dropped: u64,

    /// Set by `finalize`: the stream is closed. Further `finalize` calls are
    /// no-ops and further `on_*` events are dropped and counted. Reuse is a
    /// fresh construction.
    finalized: bool,
}

impl MarketSynchronizer {
    // -- Constructors --------------------------------------------------------

    /// Create a new synchronizer with the default [`ClockMode::OrderbookDriven`].
    ///
    /// Backward-compatible with the pre-`ClockMode` API.
    pub fn new(period_us: u64) -> Self {
        Self::with_clock_mode(period_us, ClockMode::OrderbookDriven)
    }

    /// Create a new synchronizer with an explicit clock mode.
    pub fn with_clock_mode(period_us: u64, clock_mode: ClockMode) -> Self {
        assert!(period_us > 0, "period_us must be positive");
        Self {
            period_us,
            emission_delay_us: 0,
            clock_mode,
            last_period: HashMap::new(),
            clock_period: None,
            books: HashMap::new(),
            current_trades: Vec::new(),
            current_liquidations: Vec::new(),
            current_funding_rates: Vec::new(),
            current_open_interests: Vec::new(),
            buffer: Vec::new(),
            total_captured: 0,
            late_events_dropped: 0,
            finalized: false,
        }
    }

    /// Set the emission hold-back window W (µs). Rows emit W after their
    /// closing boundary instead of at it — the disclosed latency cost of live
    /// reconciliation, letting REST-recovered prints land in their true rows.
    /// Zero (the default) is the historical emit-at-boundary behaviour.
    pub fn set_emission_delay_us(&mut self, delay_us: u64) {
        self.emission_delay_us = delay_us;
    }

    /// The configured emission hold-back window (µs).
    pub fn emission_delay_us(&self) -> u64 {
        self.emission_delay_us
    }

    /// Convenience constructor for [`ClockMode::ExternalClock`].
    #[inline]
    pub fn external_clock(period_us: u64) -> Self {
        Self::with_clock_mode(period_us, ClockMode::ExternalClock)
    }

    /// Convenience constructor for [`ClockMode::TradeDriven`].
    #[inline]
    pub fn trade_driven(period_us: u64) -> Self {
        Self::with_clock_mode(period_us, ClockMode::TradeDriven)
    }

    /// Convenience constructor for [`ClockMode::LiquidationDriven`].
    #[inline]
    pub fn liquidation_driven(period_us: u64) -> Self {
        Self::with_clock_mode(period_us, ClockMode::LiquidationDriven)
    }

    /// Returns the active clock mode.
    #[inline]
    pub fn clock_mode(&self) -> ClockMode {
        self.clock_mode
    }

    // -- Private: timestamp-membership attribution ----------------------------

    /// Record a book under the grid period its own event timestamp falls in.
    fn record_book(&mut self, key: String, ts_us: u64, book: Orderbook) {
        let period = ts_us / self.period_us;
        self.books.entry(key).or_default().insert(period, book);
    }

    /// The as-of-boundary book for the row labeled `row` (content period
    /// `row - 1`): the last book whose own timestamp is strictly before the
    /// closing boundary `row * period_us`. The book keeps its true
    /// `orderbook_ts_us` — staleness stays measurable downstream.
    fn as_of_book(&self, key: &str, row: u64) -> Option<Orderbook> {
        self.books
            .get(key)
            .and_then(|m| m.range(..row).next_back())
            .map(|(_, b)| b.clone())
    }

    /// Global-clock variant: the tracked set is single-symbol by
    /// documented assumption (multi-symbol attribution is an open question).
    fn as_of_book_any(&self, row: u64) -> Option<Orderbook> {
        self.books
            .values()
            .next()
            .and_then(|m| m.range(..row).next_back())
            .map(|(_, b)| b.clone())
    }

    /// Drop book history below the as-of entry for the last emitted row
    /// (that entry stays as the carry-forward for gap fills).
    fn prune_books(&mut self, last_row: u64) {
        for m in self.books.values_mut() {
            if let Some(keep) = m.range(..last_row).next_back().map(|(k, _)| *k) {
                m.retain(|&k, _| k >= keep);
            }
        }
    }

    /// Partition every buffered event by the snapshot ROW its own timestamp
    /// belongs to. A snapshot labeled `p` closes the content period
    /// `[(p-1)*period_us, p*period_us)`, so an event in grid period `q`
    /// belongs to the row labeled `q + 1`. Rows in
    /// `(prev_period, last_emitted]` get their events; events for rows not
    /// yet emitted stay buffered; events for already-emitted rows are
    /// dropped and counted — late data is never silently re-homed.
    fn drain_events(
        &mut self,
        prev_period: u64,
        last_emitted: u64,
    ) -> HashMap<u64, PeriodEvents> {
        let mut out: HashMap<u64, PeriodEvents> = HashMap::new();
        let mut late: u64 = 0;

        for tr in std::mem::take(&mut self.current_trades) {
            let row = tr.source_trade_ts_us / self.period_us + 1;
            if row <= prev_period {
                late += 1;
            } else if row <= last_emitted {
                out.entry(row).or_default().trades.push(tr);
            } else {
                self.current_trades.push(tr);
            }
        }
        for lq in std::mem::take(&mut self.current_liquidations) {
            let row = lq.liquidation_ts_us / self.period_us + 1;
            if row <= prev_period {
                late += 1;
            } else if row <= last_emitted {
                out.entry(row).or_default().liquidations.push(lq);
            } else {
                self.current_liquidations.push(lq);
            }
        }
        for fr in std::mem::take(&mut self.current_funding_rates) {
            let row = fr.funding_rate_ts_us / self.period_us + 1;
            if row <= prev_period {
                late += 1;
            } else if row <= last_emitted {
                out.entry(row).or_default().funding.push(fr);
            } else {
                self.current_funding_rates.push(fr);
            }
        }
        for oi in std::mem::take(&mut self.current_open_interests) {
            let row = oi.open_interest_ts_us / self.period_us + 1;
            if row <= prev_period {
                late += 1;
            } else if row <= last_emitted {
                out.entry(row).or_default().open_interest.push(oi);
            } else {
                self.current_open_interests.push(oi);
            }
        }

        if late > 0 {
            self.late_events_dropped += late;
            warn!(
                late,
                total = self.late_events_dropped,
                "market_sync.late_events_dropped"
            );
        }
        out
    }

    // -- Private: grid advancement -------------------------------------------

    /// Core clock advancement logic for non-OrderbookDriven modes.
    ///
    /// Computes the current period from `ts_us` (UTC epoch microseconds, the
    /// platform standard), emits snapshots for all crossed grid periods, and
    /// drains event buffers into the first emitted snapshot.
    ///
    /// Returns the number of snapshots emitted.
    fn advance_grid(&mut self, ts_us: u128) -> usize {
        let current_period = (ts_us.saturating_sub(self.emission_delay_us as u128)
            / self.period_us as u128) as u64;

        let prev_period = match self.clock_period {
            Some(p) => p,
            None => {
                self.clock_period = Some(current_period);
                return 0;
            }
        };

        if current_period <= prev_period {
            return 0;
        }

        let gap = current_period - prev_period;
        let fill_count = gap.min(MAX_GAP_FILL);

        if gap > MAX_GAP_FILL {
            warn!(
                "[clock] gap of {} periods exceeds MAX_GAP_FILL ({}), capping",
                gap, MAX_GAP_FILL,
            );
        }

        // Timestamp-membership attribution: each emitted row receives exactly
        // the events whose own timestamps fall in the content period it closes
        // (gap fills included); events for not-yet-emitted rows stay buffered;
        // late data is dropped and counted. Books embed the as-of-boundary
        // state with their true timestamps — a book from the still-open period
        // cannot leak in.
        let last_emitted = prev_period + fill_count;
        let mut by_period = self.drain_events(prev_period, last_emitted);

        for p in (prev_period + 1)..=last_emitted {
            let ts = p * self.period_us;
            let bundle = by_period.remove(&p).unwrap_or_default();
            let snap = MarketSnapshot {
                ts_us: ts,
                orderbook: self.as_of_book_any(p),
                trades: bundle.trades,
                liquidations: bundle.liquidations,
                funding_rate: bundle.funding,
                open_interest: bundle.open_interest,
            };
            self.buffer.push(snap);
        }
        self.prune_books(last_emitted);

        self.clock_period = Some(current_period);
        self.total_captured += fill_count as usize;
        fill_count as usize
    }

    // -- Public: clock drivers -----------------------------------------------

    /// Advance the grid clock to `ts_us` (UTC epoch microseconds).
    ///
    /// Only effective in [`ClockMode::ExternalClock`] mode. In other modes
    /// this is a no-op that returns 0.
    ///
    /// Emits snapshots for all grid periods crossed since the last clock
    /// advance. The clock is independent of any data feed — orderbook,
    /// trades, etc. are accumulated passively and drained into the emitted
    /// snapshot(s).
    ///
    /// Returns the number of snapshots emitted.
    /// Whether `finalize` has closed the stream (the terminal FINALIZED
    /// state). Once true, `on_*` events are dropped+counted and `finalize`
    /// is a no-op.
    pub fn is_finalized(&self) -> bool {
        self.finalized
    }

    /// Record one event dropped because the stream is finalized.
    fn drop_post_finalize(&mut self) {
        self.late_events_dropped += 1;
        warn!(
            total = self.late_events_dropped,
            "market_sync.event_after_finalize_dropped"
        );
    }

    pub fn on_time(&mut self, ts_us: u64) -> usize {
        if self.finalized {
            self.drop_post_finalize();
            return 0;
        }
        if self.clock_mode != ClockMode::ExternalClock {
            return 0;
        }
        self.advance_grid(ts_us as u128)
    }

    /// Feed an orderbook snapshot.
    ///
    /// - In [`ClockMode::OrderbookDriven`]: this is the clock driver. Grid
    ///   periods are triggered by orderbook timestamp crossings (per-symbol).
    /// - In all other modes: only updates the latest orderbook state for
    ///   the given symbol, without advancing the grid.
    ///
    /// `exchange_ts_us` is the exchange-reported timestamp in UTC epoch
    /// microseconds (normalized by `epoch_to_us` upstream).
    ///
    /// Returns the number of snapshots emitted (always 0 in non-OB modes).
    pub fn on_orderbook(
        &mut self,
        pair: &TradingPair,
        exchange_ts_us: u64,
        snapshot: Orderbook,
    ) -> usize {
        if self.finalized {
            self.drop_post_finalize();
            return 0;
        }
        let key = pair.to_canonical();

        if self.clock_mode != ClockMode::OrderbookDriven {
            // Non-OB mode: passive state update, recorded under the grid
            // period the book's own timestamp falls in.
            self.record_book(key, exchange_ts_us, snapshot);
            return 0;
        }

        // --- OrderbookDriven: original per-symbol grid logic ----------------
        let ts_us = exchange_ts_us as u128;
        let current_period = (ts_us.saturating_sub(self.emission_delay_us as u128)
            / self.period_us as u128) as u64;

        let prev_period = match self.last_period.get(&key) {
            Some(&p) => p,
            None => {
                self.last_period.insert(key.clone(), current_period);
                self.record_book(key, exchange_ts_us, snapshot);
                return 0;
            }
        };

        if current_period <= prev_period {
            // Still in the same period — update latest state.
            self.record_book(key, exchange_ts_us, snapshot);
            return 0;
        }

        // Grid boundary crossed — emit snapshot(s) for completed period(s).
        let gap = current_period - prev_period;
        let fill_count = gap.min(MAX_GAP_FILL);

        if gap > MAX_GAP_FILL {
            warn!(
                "[{}] gap of {} periods exceeds MAX_GAP_FILL ({}), capping",
                pair, gap, MAX_GAP_FILL,
            );
        }

        // Emit each completed period with timestamp-membership attribution:
        // its own events, its as-of-boundary book (true timestamps kept),
        // gap fills carrying whatever belongs to them.
        let last_emitted = prev_period + fill_count;
        let mut by_period = self.drain_events(prev_period, last_emitted);

        for p in (prev_period + 1)..=last_emitted {
            let ts = p * self.period_us;
            let bundle = by_period.remove(&p).unwrap_or_default();
            let snap = MarketSnapshot {
                ts_us: ts,
                orderbook: self.as_of_book(&key, p),
                trades: bundle.trades,
                liquidations: bundle.liquidations,
                funding_rate: bundle.funding,
                open_interest: bundle.open_interest,
            };
            self.buffer.push(snap);
        }
        self.prune_books(last_emitted);

        self.last_period.insert(key.clone(), current_period);
        self.record_book(key, exchange_ts_us, snapshot);
        self.total_captured += fill_count as usize;
        fill_count as usize
    }

    /// Feed a trade event.
    ///
    /// The trade is always accumulated in the current period's buffer.
    ///
    /// - In [`ClockMode::TradeDriven`]: also checks if the trade's `trade_ts`
    ///   crosses a grid period boundary. Attribution is by timestamp
    ///   membership — the crossing trade lands in the period its own
    ///   timestamp falls in (the newly opened one), never the completed one.
    /// - In all other modes: accumulation only.
    ///
    /// Returns the number of snapshots emitted (always 0 in non-Trade modes).
    pub fn on_trade(&mut self, trade: Trade) -> usize {
        if self.finalized {
            self.drop_post_finalize();
            return 0;
        }
        let ts_us = trade.source_trade_ts_us;
        self.current_trades.push(trade);

        if self.clock_mode == ClockMode::TradeDriven {
            self.advance_grid(ts_us as u128)
        } else {
            0
        }
    }

    /// Feed a liquidation event.
    ///
    /// The liquidation is always accumulated in the current period's buffer.
    ///
    /// - In [`ClockMode::LiquidationDriven`]: also checks if the liquidation's
    ///   `liquidation_ts_us` crosses a grid period boundary. The liquidation is
    ///   included in the emitted snapshot.
    /// - In all other modes: accumulation only.
    ///
    /// Returns the number of snapshots emitted (always 0 in non-Liq modes).
    pub fn on_liquidation(&mut self, liq: Liquidation) -> usize {
        if self.finalized {
            self.drop_post_finalize();
            return 0;
        }
        let ts_us = liq.liquidation_ts_us;
        self.current_liquidations.push(liq);

        if self.clock_mode == ClockMode::LiquidationDriven {
            self.advance_grid(ts_us as u128)
        } else {
            0
        }
    }

    /// Feed a funding rate update. State-based — latest value is carried forward.
    ///
    /// Funding rates never drive the clock, regardless of mode.
    pub fn on_funding(&mut self, fr: FundingRate) {
        if self.finalized {
            self.drop_post_finalize();
            return;
        }
        self.current_funding_rates.push(fr);
    }

    /// Feed an open interest update. State-based — latest value is carried forward.
    ///
    /// Open interest never drives the clock, regardless of mode.
    pub fn on_open_interest(&mut self, oi: OpenInterest) {
        if self.finalized {
            self.drop_post_finalize();
            return;
        }
        self.current_open_interests.push(oi);
    }

    // -- Finalization --------------------------------------------------------

    /// Emit the final snapshot, closing the current (incomplete) period.
    /// Call when the data stream ends.
    ///
    /// In [`ClockMode::OrderbookDriven`], emits one snapshot per tracked symbol.
    /// In all other modes, emits a single final snapshot from the global clock.
    pub fn finalize(&mut self) {
        if self.finalized {
            return; // idempotent: FINALIZED is terminal.
        }
        self.finalized = true;
        match self.clock_mode {
            ClockMode::OrderbookDriven => self.finalize_ob_driven(),
            _ => self.finalize_global_clock(),
        }
    }

    /// Finalize logic for [`ClockMode::OrderbookDriven`] — one snapshot per
    /// tracked symbol using the per-symbol period map.
    fn finalize_ob_driven(&mut self) {
        let periods: Vec<(String, u64)> = self
            .last_period
            .iter()
            .map(|(s, &p)| (s.clone(), p))
            .collect();

        let trades = std::mem::take(&mut self.current_trades);
        let liquidations = std::mem::take(&mut self.current_liquidations);
        let funding_rates = std::mem::take(&mut self.current_funding_rates);
        let open_interests = std::mem::take(&mut self.current_open_interests);

        for (symbol, period) in periods {
            let ts = (period + 1) * self.period_us;

            let snap = MarketSnapshot {
                ts_us: ts,
                // Terminal snapshot embeds the latest book with its true
                // timestamp (no boundary overwrite).
                orderbook: self.as_of_book(&symbol, u64::MAX),
                trades: trades.clone(),
                liquidations: liquidations.clone(),
                funding_rate: funding_rates.clone(),
                open_interest: open_interests.clone(),
            };

            self.buffer.push(snap);
            self.total_captured += 1;
        }
    }

    /// Finalize logic for global-clock modes — single final snapshot from
    /// the shared `clock_period`.
    fn finalize_global_clock(&mut self) {
        let period = match self.clock_period {
            Some(p) => p,
            None => return, // Never received a clock-driving event.
        };

        let ts = (period + 1) * self.period_us;
        let latest_ob = self.as_of_book_any(u64::MAX);
        let trades = std::mem::take(&mut self.current_trades);
        let liquidations = std::mem::take(&mut self.current_liquidations);
        let funding_rates = std::mem::take(&mut self.current_funding_rates);
        let open_interests = std::mem::take(&mut self.current_open_interests);

        let snap = MarketSnapshot {
            ts_us: ts,
            // Terminal snapshot embeds the latest book with its true
            // timestamp (no boundary overwrite).
            orderbook: latest_ob,
            trades,
            liquidations,
            funding_rate: funding_rates,
            open_interest: open_interests,
        };

        self.buffer.push(snap);
        self.total_captured += 1;
    }

    // -- Buffer access -------------------------------------------------------

    /// Drain the buffer, returning all accumulated snapshots.
    pub fn drain(&mut self) -> Vec<MarketSnapshot> {
        std::mem::take(&mut self.buffer)
    }

    #[inline]
    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }

    #[inline]
    pub fn total_captured(&self) -> usize {
        self.total_captured
    }

    // -- Aggregation ---------------------------------------------------------

    /// Compute per-snapshot aggregated statistics from the buffer.
    ///
    /// Drains the buffer (like [`drain()`](Self::drain)) and returns one
    /// [`MarketAggregate`](aetelier_types::snapshots::aggregate::MarketAggregate) per
    /// [`MarketSnapshot`].
    ///
    /// Open interest change (`oi_change`) is computed relative to the
    /// previous snapshot in sequence; the first snapshot uses 0.0.
    pub fn market_aggregate(
        &mut self,
    ) -> Vec<aetelier_types::snapshots::aggregate::MarketAggregate> {
        use aetelier_types::snapshots::aggregate::MarketAggregate;

        let snapshots = std::mem::take(&mut self.buffer);
        let mut aggregates = Vec::with_capacity(snapshots.len());
        let mut prev_oi: f64 = 0.0;

        for snap in &snapshots {
            let agg = MarketAggregate::from_snapshot(snap, prev_oi);
            prev_oi = agg.oi_contracts;
            aggregates.push(agg);
        }

        aggregates
    }

    // -- Parquet persistence (delegated to aetelier-io) --

    /// Compute aggregated statistics and write to a Parquet file.
    ///
    /// **Note**: the real implementation lives in `aetelier_io` as an
    /// extension trait (`FlushAggregateToParquet`).  Import it and call
    /// `.flush_aggregate_to_parquet()` from there — this avoids a
    /// circular dependency between `aetelier_connect` and `aetelier_io`.
    pub fn flush_aggregate_to_parquet(
        &mut self,
        _output_dir: &std::path::Path,
    ) -> Result<std::path::PathBuf, aetelier_types::errors::PersistError> {
        Err(aetelier_types::errors::PersistError::UnsupportedFormat(
            "parquet support requires calling via aetelier_io::flush::FlushAggregateToParquet"
                .to_string(),
        ))
    }
}
