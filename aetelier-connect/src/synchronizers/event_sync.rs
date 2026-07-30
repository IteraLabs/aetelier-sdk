//! Market-event-aligned snapshot synchronizer.
//!
//! Unlike the grid-based [`MarketSynchronizer`](super::MarketSynchronizer),
//! [`EventSynchronizer`] emits a [`MarketSnapshot`] each time a designated
//! **reference market event** is received. Reference events are configurable
//! and can be any combination of: new orderbook delta, new trade, or new
//! liquidation — all originating from the exchange WSS feed.
//!
//! # Semantics
//!
//! - **Orderbook** is *state-based*: the most recent full snapshot is carried
//!   forward into every emitted `MarketSnapshot`.
//! - **Trades, liquidations** are *event-based*: accumulated between
//!   consecutive reference events. Buffers are drained on emission.
//! - **Funding rate, open interest** are *state-based*: the latest values are
//!   cloned into each emitted snapshot.
//!
//! A snapshot is emitted only when:
//!   1. A reference event arrives, **and**
//!   2. At least one orderbook has been observed (the synchronizer is
//!      *initialized*).
//!
//! # Usage
//!
//! ```ignore
//! use aetelier_connect::synchronizers::{EventSynchronizer, ReferenceEventType};
//!
//! // Emit a snapshot on every orderbook delta:
//! let mut sync = EventSynchronizer::orderbook_only();
//!
//! // Or on every trade and liquidation:
//! let mut sync = EventSynchronizer::new(vec![
//!     ReferenceEventType::Trade,
//!     ReferenceEventType::Liquidation,
//! ]);
//!
//! // Feed events as they arrive from the WSS stream:
//! sync.on_orderbook("BTCUSDT", ts_us, orderbook);
//! sync.on_trade(trade);
//! sync.on_liquidation(liquidation);
//! sync.on_funding(funding_rate);
//! sync.on_open_interest(oi);
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

/// Which market event type triggers snapshot emission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReferenceEventType {
    /// A new orderbook delta (snapshot or update) from the exchange.
    Orderbook,
    /// A new public trade execution.
    Trade,
    /// A new liquidation event.
    Liquidation,
}

/// Event-driven snapshot synchronizer.
///
/// Produces [`MarketSnapshot`]s aligned to the arrival of reference market
/// events rather than a fixed time grid. See the [module docs](self) for
/// full semantics.
pub struct EventSynchronizer {
    /// Which event types trigger snapshot emission.
    reference_events: Vec<ReferenceEventType>,

    /// Whether we have received at least one orderbook (required before
    /// any snapshot can be emitted).
    initialized: bool,

    /// Most recent orderbook state (state-based, carried forward).
    last_orderbook: Option<Orderbook>,

    /// Trades accumulated since the last emitted snapshot.
    current_trades: Vec<Trade>,

    /// Liquidations accumulated since the last emitted snapshot.
    current_liquidations: Vec<Liquidation>,

    /// Most recent funding rate observations (state-based, carried forward).
    current_funding_rates: Vec<FundingRate>,

    /// Most recent open interest observations (state-based, carried forward).
    current_open_interests: Vec<OpenInterest>,

    /// Buffered output snapshots awaiting drain.
    pub buffer: Vec<MarketSnapshot>,

    /// Total snapshots produced across all drains.
    pub total_captured: usize,
}

impl EventSynchronizer {
    /// Create a new synchronizer that triggers on the given reference events.
    ///
    /// # Panics
    ///
    /// Panics if `reference_events` is empty.
    pub fn new(reference_events: Vec<ReferenceEventType>) -> Self {
        assert!(
            !reference_events.is_empty(),
            "at least one reference event type is required"
        );
        Self {
            reference_events,
            initialized: false,
            last_orderbook: None,
            current_trades: Vec::new(),
            current_liquidations: Vec::new(),
            current_funding_rates: Vec::new(),
            current_open_interests: Vec::new(),
            buffer: Vec::new(),
            total_captured: 0,
        }
    }

    /// Convenience: trigger on every orderbook delta event only.
    pub fn orderbook_only() -> Self {
        Self::new(vec![ReferenceEventType::Orderbook])
    }

    /// Convenience: trigger on every public trade event only.
    pub fn trade_only() -> Self {
        Self::new(vec![ReferenceEventType::Trade])
    }

    /// Convenience: trigger on every liquidation event only.
    pub fn liquidation_only() -> Self {
        Self::new(vec![ReferenceEventType::Liquidation])
    }

    /// Convenience: trigger on all three event types.
    pub fn all_events() -> Self {
        Self::new(vec![
            ReferenceEventType::Orderbook,
            ReferenceEventType::Trade,
            ReferenceEventType::Liquidation,
        ])
    }

    // ------------------------------------------------------------------ //
    //  Internal helpers
    // ------------------------------------------------------------------ //

    /// Returns `true` if `event_type` is configured as a reference event.
    #[inline]
    fn is_reference(&self, event_type: ReferenceEventType) -> bool {
        self.reference_events.contains(&event_type)
    }

    /// Emit a snapshot with the current accumulated state.
    ///
    /// - `ts_us`: timestamp (UTC epoch microseconds) for the snapshot.
    /// - Event-based buffers (trades, liquidations) are **drained**.
    /// - State-based data (orderbook, funding, OI) are **cloned**.
    fn emit_snapshot(&mut self, ts_us: u64) {
        let snap = MarketSnapshot {
            ts_us,
            orderbook: self.last_orderbook.clone().map(|mut ob| {
                ob.orderbook_ts_us = ts_us;
                ob
            }),
            trades: std::mem::take(&mut self.current_trades),
            liquidations: std::mem::take(&mut self.current_liquidations),
            funding_rate: self.current_funding_rates.clone(),
            open_interest: self.current_open_interests.clone(),
        };
        self.buffer.push(snap);
        self.total_captured += 1;
    }

    // ------------------------------------------------------------------ //
    //  Public feed methods
    // ------------------------------------------------------------------ //

    /// Feed an orderbook snapshot.
    ///
    /// The orderbook is always stored as the latest state. If `Orderbook` is a
    /// reference event, a snapshot is emitted.
    ///
    /// `ts_us` is the event timestamp in **UTC epoch microseconds**.
    ///
    /// Returns the number of snapshots emitted (0 or 1).
    pub fn on_orderbook(
        &mut self,
        _pair: &TradingPair,
        ts_us: u64,
        snapshot: Orderbook,
    ) -> usize {
        self.last_orderbook = Some(snapshot);

        if !self.initialized {
            self.initialized = true;
        }

        if self.is_reference(ReferenceEventType::Orderbook) {
            self.emit_snapshot(ts_us);
            return 1;
        }

        0
    }

    /// Feed a trade event.
    ///
    /// The trade is always accumulated. If `Trade` is a reference event **and**
    /// the synchronizer is initialized, a snapshot is emitted.
    ///
    /// Returns the number of snapshots emitted (0 or 1).
    pub fn on_trade(&mut self, trade: Trade) -> usize {
        let ts_us = trade.source_trade_ts_us;
        self.current_trades.push(trade);

        if self.initialized && self.is_reference(ReferenceEventType::Trade) {
            self.emit_snapshot(ts_us);
            return 1;
        }

        0
    }

    /// Feed a liquidation event.
    ///
    /// The liquidation is always accumulated. If `Liquidation` is a reference
    /// event **and** the synchronizer is initialized, a snapshot is emitted.
    ///
    /// Returns the number of snapshots emitted (0 or 1).
    pub fn on_liquidation(&mut self, liq: Liquidation) -> usize {
        let ts_us = liq.liquidation_ts_us;
        self.current_liquidations.push(liq);

        if self.initialized && self.is_reference(ReferenceEventType::Liquidation) {
            self.emit_snapshot(ts_us);
            return 1;
        }

        0
    }

    /// Feed a funding rate update (state-based, carried forward).
    ///
    /// Funding rate updates never trigger snapshot emission — they simply
    /// update the state that will be included in the next emitted snapshot.
    ///
    /// Returns 0 (never emits).
    pub fn on_funding(&mut self, fr: FundingRate) -> usize {
        self.current_funding_rates = vec![fr];
        0
    }

    /// Feed an open interest update (state-based, carried forward).
    ///
    /// Open interest updates never trigger snapshot emission — they simply
    /// update the state that will be included in the next emitted snapshot.
    ///
    /// Returns 0 (never emits).
    pub fn on_open_interest(&mut self, oi: OpenInterest) -> usize {
        self.current_open_interests = vec![oi];
        0
    }

    /// Emit a final snapshot with any remaining buffered events.
    ///
    /// Call when the data stream ends. This ensures no accumulated
    /// trades/liquidations are lost.
    pub fn finalize(&mut self) {
        if !self.initialized {
            return;
        }

        // Only emit if there is buffered event-based data that has not
        // been captured yet.
        if !self.current_trades.is_empty() || !self.current_liquidations.is_empty() {
            let ts_us = self
                .last_orderbook
                .as_ref()
                .map(|ob| ob.orderbook_ts_us)
                .unwrap_or(0);
            self.emit_snapshot(ts_us);
        }
    }

    /// Drain the buffer, returning all accumulated snapshots.
    pub fn drain(&mut self) -> Vec<MarketSnapshot> {
        std::mem::take(&mut self.buffer)
    }

    /// Number of snapshots currently in the buffer.
    #[inline]
    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }

    /// Total snapshots produced across all drains.
    #[inline]
    pub fn total_captured(&self) -> usize {
        self.total_captured
    }
}
