//! Unit tests for `MarketSynchronizer` with `ClockMode` support.

use aetelier_connect::synchronizers::{ClockMode, MarketSynchronizer};
use aetelier_types::{
    Level, Liquidation, OrderSide, Trade, TradeSide,
    funding::FundingRate,
    open_interest::OpenInterest,
    orderbooks::{Orderbook, decimal_to_f64, f64_to_decimal},
    trading_pair::TradingPair,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn btcusdt() -> TradingPair {
    TradingPair::new("BTC", "USDT")
}

fn make_ob(ts: u64) -> Orderbook {
    Orderbook::from_levels(
        0,
        ts,
        btcusdt(),
        "bybit".to_string(),
        vec![Level::new(
            0,
            OrderSide::Bids,
            f64_to_decimal(100.0),
            f64_to_decimal(1.0),
            vec![],
        )],
        vec![Level::new(
            0,
            OrderSide::Asks,
            f64_to_decimal(101.0),
            f64_to_decimal(1.0),
            vec![],
        )],
    )
}

fn make_trade(ts_us: u64, price: f64, amount: f64) -> Trade {
    Trade {
        source_trade_ts_us: ts_us,
        local_trade_ts_us: 0,
        source_trade_rtt_us: 0,
        pair: btcusdt(),
        side: TradeSide::Buy,
        amount: f64_to_decimal(amount),
        price: f64_to_decimal(price),
        exchange: "bybit".to_string(),
        id: "t1".to_string(),
        origin: Default::default(),
    }
}

fn make_liquidation(ts_us: u64, price: f64, amount: f64, side: TradeSide) -> Liquidation {
    Liquidation {
        liquidation_ts_us: ts_us,
        pair: btcusdt(),
        side,
        amount: f64_to_decimal(amount),
        price: f64_to_decimal(price),
        exchange: "bybit".to_string(),
    }
}

fn make_funding(ts_us: u64, rate: f64) -> FundingRate {
    FundingRate {
        funding_rate_ts_us: ts_us,
        local_funding_ts_us: 0,
        recv_seq: 0,
        conn_epoch: 0,
        pair: btcusdt(),
        funding_rate: f64_to_decimal(rate),
        premium: None,
        interval_hours: 8,
        next_funding_ts_us: ts_us + 28_800_000_000,
        exchange: "bybit".to_string(),
    }
}

fn make_oi(ts_us: u64, contracts: f64, value: f64) -> OpenInterest {
    OpenInterest {
        open_interest_ts_us: ts_us,
        local_oi_ts_us: 0,
        recv_seq: 0,
        conn_epoch: 0,
        pair: btcusdt(),
        open_interest: f64_to_decimal(contracts),
        open_interest_value: Some(f64_to_decimal(value)),
        mark_px: None,
        exchange: "bybit".to_string(),
    }
}

// ---------------------------------------------------------------------------
// ClockMode constructors
// ---------------------------------------------------------------------------

#[test]
fn test_default_clock_mode_is_orderbook_driven() {
    let sync = MarketSynchronizer::new(1_000_000);
    assert_eq!(sync.clock_mode(), ClockMode::OrderbookDriven);
}

#[test]
fn test_with_clock_mode_sets_mode() {
    let sync = MarketSynchronizer::with_clock_mode(1_000_000, ClockMode::TradeDriven);
    assert_eq!(sync.clock_mode(), ClockMode::TradeDriven);
}

#[test]
fn test_convenience_constructors() {
    assert_eq!(
        MarketSynchronizer::external_clock(1_000_000).clock_mode(),
        ClockMode::ExternalClock,
    );
    assert_eq!(
        MarketSynchronizer::trade_driven(1_000_000).clock_mode(),
        ClockMode::TradeDriven,
    );
    assert_eq!(
        MarketSynchronizer::liquidation_driven(1_000_000).clock_mode(),
        ClockMode::LiquidationDriven,
    );
}

#[test]
#[should_panic(expected = "period_us must be positive")]
fn test_zero_period_panics() {
    let _sync = MarketSynchronizer::new(0);
}

// ---------------------------------------------------------------------------
// ExternalClock mode — on_time
// ---------------------------------------------------------------------------

/// 1s period = 1_000_000 us
const PERIOD_1S: u64 = 1_000_000;

#[test]
fn test_on_time_first_call_initializes_no_emission() {
    let mut sync = MarketSynchronizer::external_clock(PERIOD_1S);
    let n = sync.on_time(1_500_000); // 1.5s
    assert_eq!(n, 0);
    assert_eq!(sync.buffer_len(), 0);
}

#[test]
fn test_on_time_same_period_no_emission() {
    let mut sync = MarketSynchronizer::external_clock(PERIOD_1S);
    sync.on_time(1_000_000); // period 1
    let n = sync.on_time(1_500_000); // still period 1
    assert_eq!(n, 0);
    assert_eq!(sync.buffer_len(), 0);
}

#[test]
fn test_on_time_crosses_boundary_emits_one() {
    let mut sync = MarketSynchronizer::external_clock(PERIOD_1S);
    sync.on_time(1_000_000); // init at period 1 (anchor)

    // Trade inside the anchor period [1s, 2s)
    sync.on_trade(make_trade(1_500_000, 50_000.0, 0.1));

    let n = sync.on_time(2_000_000); // crosses to period 2
    assert_eq!(n, 1);
    assert_eq!(sync.buffer_len(), 1);
    assert_eq!(sync.total_captured(), 1);

    let snaps = sync.drain();
    // Row 2 closes the content period [1s, 2s): the anchor period's own
    // events emit at the first crossing — nothing is late.
    assert_eq!(snaps[0].ts_us, 2 * PERIOD_1S);
    assert_eq!(snaps[0].trades.len(), 1);
    assert_eq!(sync.late_events_dropped, 0);

    // Consumed at emission — finalize does not double-count it.
    sync.finalize();
    let finals = sync.drain();
    assert!(finals[0].trades.is_empty());
}

#[test]
fn test_on_time_gap_fill() {
    let mut sync = MarketSynchronizer::external_clock(PERIOD_1S);
    sync.on_time(1_000_000); // init at period 1

    // Jump 3 periods: 1 → 4
    let n = sync.on_time(4_000_000);
    assert_eq!(n, 3);
    assert_eq!(sync.buffer_len(), 3);

    let snaps = sync.drain();
    assert_eq!(snaps[0].ts_us, 2 * PERIOD_1S);
    assert_eq!(snaps[1].ts_us, 3 * PERIOD_1S);
    assert_eq!(snaps[2].ts_us, 4 * PERIOD_1S);
}

#[test]
fn test_on_time_noop_in_orderbook_mode() {
    let mut sync = MarketSynchronizer::new(PERIOD_1S);
    sync.on_time(1_000_000);
    let n = sync.on_time(2_000_000);
    assert_eq!(n, 0);
    assert_eq!(sync.buffer_len(), 0);
}

#[test]
fn test_on_time_includes_orderbook_state() {
    let mut sync = MarketSynchronizer::external_clock(PERIOD_1S);
    sync.on_time(1_000_000); // init

    // Feed OB — should just update state, no emission
    let ob = make_ob(1_500_000);
    let n_ob = sync.on_orderbook(&btcusdt(), 1_500_000, ob);
    assert_eq!(n_ob, 0);

    // Advance clock past boundary
    let n = sync.on_time(2_000_000);
    assert_eq!(n, 1);

    let snaps = sync.drain();
    assert!(snaps[0].orderbook.is_some());
    assert_eq!(snaps[0].orderbook.as_ref().unwrap().bids.len(), 1);
}

// ---------------------------------------------------------------------------
// TradeDriven mode
// ---------------------------------------------------------------------------

#[test]
fn test_trade_driven_first_trade_initializes() {
    let mut sync = MarketSynchronizer::trade_driven(PERIOD_1S);
    let n = sync.on_trade(make_trade(1_000_000, 50_000.0, 0.1));
    assert_eq!(n, 0);
    assert_eq!(sync.buffer_len(), 0);
}

#[test]
fn test_trade_driven_crossing_trade_lands_in_next_period() {
    let mut sync = MarketSynchronizer::trade_driven(PERIOD_1S);

    // First trade at 1s → init (anchor period 1)
    sync.on_trade(make_trade(1_000_000, 50_000.0, 0.1));

    // Second trade at 2s → crosses into period 2
    let n = sync.on_trade(make_trade(2_000_000, 51_000.0, 0.2));
    assert_eq!(n, 1);

    let snaps = sync.drain();
    // The emitted row closes [1s, 2s): it carries the first trade only.
    // The crossing trade belongs to the newly opened period [2s, 3s) and
    // never appears in the row emitted at its own crossing.
    assert_eq!(snaps[0].ts_us, 2 * PERIOD_1S);
    assert_eq!(snaps[0].trades.len(), 1);
    assert!((decimal_to_f64(snaps[0].trades[0].price) - 50_000.0).abs() < f64::EPSILON);
    assert_eq!(sync.late_events_dropped, 0);

    // The crossing trade stays buffered with any same-period successors
    // and surfaces when its own period closes — here, at finalize.
    let n = sync.on_trade(make_trade(2_500_000, 52_000.0, 0.3));
    assert_eq!(n, 0);
    sync.finalize();
    let finals = sync.drain();
    assert_eq!(finals[0].ts_us, 3 * PERIOD_1S);
    assert_eq!(finals[0].trades.len(), 2);
    assert!((decimal_to_f64(finals[0].trades[0].price) - 51_000.0).abs() < f64::EPSILON);
    assert!((decimal_to_f64(finals[0].trades[1].price) - 52_000.0).abs() < f64::EPSILON);
}

#[test]
fn test_trade_driven_orderbook_state_passthrough() {
    let mut sync = MarketSynchronizer::trade_driven(PERIOD_1S);

    // Init with trade
    sync.on_trade(make_trade(1_000_000, 50_000.0, 0.1));

    // Feed OB — returns 0 (not driving)
    let n_ob = sync.on_orderbook(&btcusdt(), 1_500_000, make_ob(1_500_000));
    assert_eq!(n_ob, 0);

    // Cross boundary with trade
    sync.on_trade(make_trade(2_000_000, 51_000.0, 0.2));

    let snaps = sync.drain();
    assert!(snaps[0].orderbook.is_some());
}

// ---------------------------------------------------------------------------
// LiquidationDriven mode
// ---------------------------------------------------------------------------

#[test]
fn test_liquidation_driven_crossing_emits() {
    let mut sync = MarketSynchronizer::liquidation_driven(PERIOD_1S);

    // Init with liq at 1s (anchor period 1)
    sync.on_liquidation(make_liquidation(1_000_000, 50_000.0, 1.0, TradeSide::Buy));

    // Cross boundary at 2s
    let n =
        sync.on_liquidation(make_liquidation(2_000_000, 48_000.0, 2.0, TradeSide::Sell));
    assert_eq!(n, 1);

    let snaps = sync.drain();
    // The emitted row closes [1s, 2s): it carries the anchor-period liq.
    // The crossing liq belongs to [2s, 3s) and stays buffered.
    assert_eq!(snaps[0].ts_us, 2 * PERIOD_1S);
    assert_eq!(snaps[0].liquidations.len(), 1);
    assert!(
        (decimal_to_f64(snaps[0].liquidations[0].price) - 50_000.0).abs() < f64::EPSILON
    );
    assert_eq!(sync.late_events_dropped, 0);

    // The crossing liq surfaces when its own period closes — at finalize.
    sync.finalize();
    let finals = sync.drain();
    assert_eq!(finals[0].ts_us, 3 * PERIOD_1S);
    assert_eq!(finals[0].liquidations.len(), 1);
    assert!(
        (decimal_to_f64(finals[0].liquidations[0].price) - 48_000.0).abs() < f64::EPSILON
    );
}

#[test]
fn test_liquidation_driven_trades_accumulate_passively() {
    let mut sync = MarketSynchronizer::liquidation_driven(PERIOD_1S);

    sync.on_liquidation(make_liquidation(1_000_000, 50_000.0, 1.0, TradeSide::Buy));
    let n_trade = sync.on_trade(make_trade(1_500_000, 51_000.0, 0.5));
    assert_eq!(n_trade, 0); // Trades don't drive clock in liq mode

    sync.on_liquidation(make_liquidation(2_000_000, 48_000.0, 2.0, TradeSide::Sell));

    let snaps = sync.drain();
    // Row 2 closes [1s, 2s): the passively-buffered trade and the anchor
    // liq both belong to it; the crossing liq ([2s, 3s)) stays buffered.
    assert_eq!(snaps[0].trades.len(), 1);
    assert!((decimal_to_f64(snaps[0].trades[0].price) - 51_000.0).abs() < f64::EPSILON);
    assert_eq!(snaps[0].liquidations.len(), 1);
    assert!(
        (decimal_to_f64(snaps[0].liquidations[0].price) - 50_000.0).abs() < f64::EPSILON
    );
    assert_eq!(sync.late_events_dropped, 0);

    // Period-2 events — the crossing liq plus a later passive trade —
    // surface when the stream ends, at finalize.
    let n_trade = sync.on_trade(make_trade(2_500_000, 52_000.0, 0.5));
    assert_eq!(n_trade, 0);
    sync.finalize();
    let finals = sync.drain();
    assert_eq!(finals[0].trades.len(), 1);
    assert!((decimal_to_f64(finals[0].trades[0].price) - 52_000.0).abs() < f64::EPSILON);
    assert_eq!(finals[0].liquidations.len(), 1);
    assert!(
        (decimal_to_f64(finals[0].liquidations[0].price) - 48_000.0).abs() < f64::EPSILON
    );
}

// ---------------------------------------------------------------------------
// OrderbookDriven mode (backward compat)
// ---------------------------------------------------------------------------

#[test]
fn test_orderbook_driven_crossing_embeds_as_of_boundary_book() {
    let mut sync = MarketSynchronizer::new(PERIOD_1S);

    // First OB at 1s — init (anchor period 1)
    let n = sync.on_orderbook(&btcusdt(), 1_000_000, make_ob(1_000_000));
    assert_eq!(n, 0);

    // Trade inside the anchor period [1s, 2s)
    sync.on_trade(make_trade(1_500_000, 50_000.0, 0.1));

    // Second OB at 2s — crosses boundary
    let n = sync.on_orderbook(&btcusdt(), 2_000_000, make_ob(2_000_000));
    assert_eq!(n, 1);

    let snaps = sync.drain();
    // Row 2 closes [1s, 2s): the trade belongs to it — nothing is late.
    assert_eq!(snaps[0].ts_us, 2 * PERIOD_1S);
    assert_eq!(snaps[0].trades.len(), 1);
    assert_eq!(sync.late_events_dropped, 0);
    // The embedded book is the last book strictly before the closing
    // boundary, keeping its true timestamp — not the crossing book, and
    // not re-stamped to the grid.
    let book = snaps[0].orderbook.as_ref().unwrap();
    assert_eq!(book.orderbook_ts_us, 1_000_000);
}

#[test]
fn test_orderbook_driven_on_trade_returns_zero() {
    let mut sync = MarketSynchronizer::new(PERIOD_1S);
    let n = sync.on_trade(make_trade(1_000_000, 50_000.0, 0.1));
    assert_eq!(n, 0);
}

// ---------------------------------------------------------------------------
// Finalize
// ---------------------------------------------------------------------------

#[test]
fn test_finalize_external_clock() {
    let mut sync = MarketSynchronizer::external_clock(PERIOD_1S);
    sync.on_time(1_000_000); // init at period 1
    sync.on_trade(make_trade(1_500_000, 50_000.0, 0.1));

    sync.finalize();
    assert_eq!(sync.buffer_len(), 1);

    let snaps = sync.drain();
    assert_eq!(snaps[0].ts_us, 2 * PERIOD_1S); // period+1
    assert_eq!(snaps[0].trades.len(), 1);
}

#[test]
fn test_finalize_before_any_clock_event_is_noop() {
    let mut sync = MarketSynchronizer::external_clock(PERIOD_1S);
    sync.on_trade(make_trade(1_500_000, 50_000.0, 0.1)); // no clock init
    sync.finalize();
    assert_eq!(sync.buffer_len(), 0);
}

#[test]
fn test_finalize_trade_driven() {
    let mut sync = MarketSynchronizer::trade_driven(PERIOD_1S);
    sync.on_trade(make_trade(1_000_000, 50_000.0, 0.1)); // init
    sync.on_funding(make_funding(1_200_000, 0.0001));

    sync.finalize();
    let snaps = sync.drain();
    assert_eq!(snaps.len(), 1);
    assert_eq!(snaps[0].funding_rate.len(), 1);
}

#[test]
fn test_finalize_orderbook_driven() {
    let mut sync = MarketSynchronizer::new(PERIOD_1S);
    sync.on_orderbook(&btcusdt(), 1_000_000, make_ob(1_000_000));
    sync.on_trade(make_trade(1_500_000, 50_000.0, 0.1));

    sync.finalize();
    let snaps = sync.drain();
    assert_eq!(snaps.len(), 1);
    assert_eq!(snaps[0].trades.len(), 1);
    assert!(snaps[0].orderbook.is_some());
}

// ---------------------------------------------------------------------------
// Mixed data feeds
// ---------------------------------------------------------------------------

#[test]
fn test_external_clock_all_feeds() {
    let mut sync = MarketSynchronizer::external_clock(PERIOD_1S);
    sync.on_time(1_000_000); // init (anchor period 1)

    sync.on_orderbook(&btcusdt(), 1_200_000, make_ob(1_200_000));
    sync.on_trade(make_trade(1_300_000, 50_000.0, 0.1));
    sync.on_liquidation(make_liquidation(1_400_000, 48_000.0, 1.0, TradeSide::Sell));
    sync.on_funding(make_funding(1_500_000, 0.0001));
    sync.on_open_interest(make_oi(1_600_000, 5000.0, 250_000_000.0));

    let n = sync.on_time(2_000_000);
    assert_eq!(n, 1);

    let snaps = sync.drain();
    let snap = &snaps[0];
    // Row 2 closes [1s, 2s): every feed's event belongs to it. The book
    // is state-based and embeds as-of the closing boundary, keeping its
    // true timestamp.
    assert_eq!(snap.ts_us, 2 * PERIOD_1S);
    let book = snap.orderbook.as_ref().unwrap();
    assert_eq!(book.orderbook_ts_us, 1_200_000);
    assert_eq!(book.bids.len(), 1);
    assert_eq!(snap.trades.len(), 1);
    assert_eq!(snap.liquidations.len(), 1);
    assert_eq!(snap.funding_rate.len(), 1);
    assert_eq!(snap.open_interest.len(), 1);
    assert_eq!(sync.late_events_dropped, 0);

    // Period-2 feeds stay buffered and drain into the final snapshot at
    // finalize — every feed type flows through there too.
    sync.on_orderbook(&btcusdt(), 2_200_000, make_ob(2_200_000));
    sync.on_trade(make_trade(2_300_000, 50_100.0, 0.1));
    sync.on_liquidation(make_liquidation(2_400_000, 48_100.0, 1.0, TradeSide::Sell));
    sync.on_funding(make_funding(2_500_000, 0.0002));
    sync.on_open_interest(make_oi(2_600_000, 5100.0, 255_000_000.0));
    sync.finalize();

    let finals = sync.drain();
    let last = &finals[0];
    assert_eq!(last.ts_us, 3 * PERIOD_1S);
    let book = last.orderbook.as_ref().unwrap();
    assert_eq!(book.orderbook_ts_us, 2_200_000);
    assert_eq!(last.trades.len(), 1);
    assert_eq!(last.liquidations.len(), 1);
    assert_eq!(last.funding_rate.len(), 1);
    assert_eq!(last.open_interest.len(), 1);
    assert_eq!(sync.late_events_dropped, 0); // finalize drops nothing
}

// ---------------------------------------------------------------------------
// Drain and buffer management
// ---------------------------------------------------------------------------

#[test]
fn test_drain_clears_buffer_preserves_count() {
    let mut sync = MarketSynchronizer::external_clock(PERIOD_1S);
    sync.on_time(1_000_000);
    sync.on_time(2_000_000);
    assert_eq!(sync.total_captured(), 1);

    let snaps = sync.drain();
    assert_eq!(snaps.len(), 1);
    assert_eq!(sync.buffer_len(), 0);
    assert_eq!(sync.total_captured(), 1); // preserved
}

// ---------------------------------------------------------------------------
// Timestamp-membership attribution (look-ahead and pre-anchor cases)
// ---------------------------------------------------------------------------

const P: u64 = 1_000_000; // 1s grid in microseconds

/// A book that arrives inside the still-open period must not leak into the
/// completed row, and the completed row must carry the as-of-boundary book
/// with its TRUE timestamp (no grid-boundary overwrite).
#[test]
fn ob_driven_snapshot_excludes_look_ahead_book_and_keeps_true_ts() {
    let mut sync = MarketSynchronizer::with_clock_mode(P, ClockMode::OrderbookDriven);
    // period 1 anchor
    sync.on_orderbook(&btcusdt(), P + 100, make_ob(P + 100));
    // a later book still inside period 1 (updates the as-of state)
    sync.on_orderbook(&btcusdt(), P + 900_000, make_ob(P + 900_000));
    // cross into period 2 — emits the row closing period 1
    sync.on_orderbook(&btcusdt(), 2 * P + 50, make_ob(2 * P + 50));

    let snaps = sync.drain();
    assert_eq!(snaps.len(), 1, "one row closing period 1");
    let ob = snaps[0].orderbook.as_ref().expect("row carries a book");
    // The row's book is the last one INSIDE period 1, never the crossing book.
    assert_eq!(ob.orderbook_ts_us, P + 900_000);
    assert_ne!(
        ob.orderbook_ts_us, snaps[0].ts_us,
        "true ts kept, not the boundary label"
    );
    assert_eq!(snaps[0].ts_us, 2 * P, "row labeled by its closing boundary");
}

/// Events whose period is already emitted (pre-anchor backlog / late data)
/// are dropped and counted, never silently re-homed into the wrong row.
#[test]
fn late_events_are_dropped_and_counted() {
    let mut sync = MarketSynchronizer::with_clock_mode(P, ClockMode::TradeDriven);
    // Anchor the clock in period 5.
    sync.on_trade(make_trade(5 * P + 10, 100.0, 1.0));
    // Advance to period 7 (emits rows 6 and 7).
    sync.on_trade(make_trade(7 * P + 10, 101.0, 1.0));
    let _ = sync.drain();
    // Now feed a trade belonging to period 5 — already emitted → dropped.
    sync.on_trade(make_trade(5 * P + 500, 999.0, 1.0));
    // And a valid future-period driver to force another emission.
    sync.on_trade(make_trade(9 * P + 10, 102.0, 1.0));

    assert_eq!(
        sync.late_events_dropped, 1,
        "the period-5 straggler is counted"
    );
    let snaps = sync.drain();
    assert!(
        snaps
            .iter()
            .all(|s| s.trades.iter().all(|t| decimal_to_f64(t.price) != 999.0)),
        "the late trade never appears in any row"
    );
}

// ---------------------------------------------------------------------------
// finalize() idempotence + terminal FINALIZED state
// ---------------------------------------------------------------------------

#[test]
fn finalize_is_idempotent_and_terminal() {
    let mut sync = MarketSynchronizer::with_clock_mode(P, ClockMode::TradeDriven);
    sync.on_trade(make_trade(P + 10, 100.0, 1.0));
    sync.on_trade(make_trade(2 * P + 10, 101.0, 1.0)); // cross → emits row 2
    let _ = sync.drain();

    assert!(!sync.is_finalized());
    sync.finalize();
    assert!(sync.is_finalized(), "finalize closes the stream");
    let first = sync.drain();

    // Second finalize is a no-op — no duplicate rows.
    sync.finalize();
    let second = sync.drain();
    assert!(second.is_empty(), "double finalize emits nothing");
    assert!(!first.is_empty(), "first finalize emitted the closing row");
}

#[test]
fn events_after_finalize_are_dropped_and_counted() {
    let mut sync = MarketSynchronizer::with_clock_mode(P, ClockMode::TradeDriven);
    sync.on_trade(make_trade(P + 10, 100.0, 1.0));
    sync.finalize();
    let before = sync.late_events_dropped;

    // Every feed type after finalize is rejected.
    assert_eq!(sync.on_trade(make_trade(3 * P, 999.0, 1.0)), 0);
    assert_eq!(sync.on_time(4 * P), 0);
    assert_eq!(
        sync.on_liquidation(make_liquidation(3 * P, 1.0, 1.0, TradeSide::Sell)),
        0
    );
    sync.on_funding(make_funding(3 * P, 0.01));
    sync.on_open_interest(make_oi(3 * P, 1.0, 1.0));

    assert_eq!(
        sync.late_events_dropped - before,
        5,
        "all five feeds counted"
    );
    assert!(
        sync.drain()
            .iter()
            .all(|s| s.trades.iter().all(|t| decimal_to_f64(t.price) != 999.0)),
        "no post-finalize event reaches any row"
    );
}

// ── Emission hold-back (live reconciliation, W) ─────────────────────────────

/// W delays row RIPENESS only: at boundary+ε the row stays buffered, a
/// late-but-correctly-timestamped print (a REST-recovered trade) still lands
/// in it, and the row emits complete once the clock passes boundary+W.
/// Membership math is untouched — the recovered print homes by its own ts.
#[test]
fn emission_holdback_lets_late_recovered_prints_land_in_their_true_row() {
    const PERIOD: u64 = 100_000; // 100ms grid
    const W: u64 = 1_000_000; // 1s hold-back
    let mut sync = MarketSynchronizer::external_clock(PERIOD);
    sync.set_emission_delay_us(W);

    // Establish the clock baseline (delayed clock = period 0).
    sync.on_time(50_000);
    // A live print inside the period closing at 200_000 (row 2).
    sync.on_trade(make_trade(150_000, 100.0, 1.0));
    // Clock reaches the boundary + a bit: WITHOUT W row 2 would emit now.
    sync.on_time(250_000);
    assert!(sync.drain().is_empty(), "row held back inside W");

    // The recovered print arrives ~RTT later, timestamped INSIDE the closed
    // period — under W it is not late and joins the still-buffered row.
    let mut recovered = make_trade(180_000, 100.5, 0.5);
    recovered.id = "recovered".into();
    recovered.origin = aetelier_types::trades::TradeOrigin::Rest;
    sync.on_trade(recovered);
    assert_eq!(sync.late_events_dropped, 0, "not late under the window");

    // Clock passes boundary + W → rows emit, complete.
    sync.on_time(200_000 + W + PERIOD);
    let rows = sync.drain();
    let row2 = rows
        .iter()
        .find(|s| s.ts_us == 200_000)
        .expect("row for the 100_000..200_000 period");
    assert_eq!(
        row2.trades.len(),
        2,
        "live + recovered in the same true row"
    );
    assert!(
        row2.trades
            .iter()
            .any(|t| t.origin == aetelier_types::trades::TradeOrigin::Rest),
        "provenance survives into the row"
    );
}

/// W = 0 (the default) preserves the historical emit-at-boundary timing —
/// the guard that every existing collector is unaffected.
#[test]
fn zero_holdback_is_the_historical_behaviour() {
    const PERIOD: u64 = 100_000;
    let mut sync = MarketSynchronizer::external_clock(PERIOD);
    assert_eq!(sync.emission_delay_us(), 0);
    sync.on_time(50_000);
    sync.on_trade(make_trade(150_000, 100.0, 1.0));
    sync.on_time(250_000);
    let rows = sync.drain();
    assert!(
        rows.iter().any(|s| s.ts_us == 200_000),
        "boundary crossing emits immediately at W=0"
    );
}

#[test]
fn without_hold_back_a_trade_arriving_after_its_boundary_is_lost() {
    let period_us = 500_000;
    let mut sync = MarketSynchronizer::external_clock(period_us);
    sync.on_time(10_000_000);

    sync.on_trade(make_trade(10_400_000, 50_000.0, 0.1));
    sync.on_time(10_500_000);
    sync.on_trade(make_trade(10_450_000, 50_000.0, 0.2));
    sync.on_time(11_000_000);

    assert_eq!(sync.late_events_dropped, 1);
    let rows = sync.drain();
    let total: usize = rows.iter().map(|r| r.trades.len()).sum();
    assert_eq!(total, 1, "the late print never reaches a row");
}

#[test]
fn hold_back_lands_a_late_trade_in_its_own_true_row() {
    let period_us = 500_000;
    let mut sync = MarketSynchronizer::external_clock(period_us);
    sync.set_emission_delay_us(1_000_000);
    sync.on_time(10_000_000);

    sync.on_trade(make_trade(10_400_000, 50_000.0, 0.1));
    sync.on_time(10_500_000);
    sync.on_trade(make_trade(10_450_000, 50_000.0, 0.2));
    sync.on_time(11_000_000);
    sync.on_time(12_000_000);

    assert_eq!(sync.late_events_dropped, 0, "hold-back keeps the row open");
    let rows = sync.drain();
    let total: usize = rows.iter().map(|r| r.trades.len()).sum();
    assert_eq!(total, 2, "both prints survive");

    let row = rows
        .iter()
        .find(|r| r.trades.len() == 2)
        .expect("both prints belong to the same true row");
    let mut stamps: Vec<u64> = row.trades.iter().map(|t| t.source_trade_ts_us).collect();
    stamps.sort_unstable();
    assert_eq!(stamps, vec![10_400_000, 10_450_000]);
}

#[test]
fn test_backfilled_settlement_persists_despite_past_funding_time() {
    use aetelier_types::funding::FundingSettlement;
    use rust_decimal::Decimal;

    let mut sync = MarketSynchronizer::trade_driven(PERIOD_1S);
    let now = 1_700_000_000 * PERIOD_1S;
    sync.on_trade(make_trade(now, 100.0, 1.0));

    let fs = FundingSettlement {
        funding_time_us: now - 24 * 3_600 * 1_000_000,
        local_ts_us: now + 100,
        rtt_us: 5_000,
        pair: btcusdt(),
        funding_rate: Decimal::new(1, 4),
        premium: None,
        exchange: "hyperliquid".to_string(),
    };
    sync.on_funding_settlement(fs);

    sync.on_trade(make_trade(now + 2 * PERIOD_1S, 101.0, 1.0));
    let snaps = sync.drain();
    let landed: usize = snaps.iter().map(|s| s.funding_settlements.len()).sum();
    assert_eq!(landed, 1, "venue-time-keyed settlement must ride the arrival-clock row");
    assert_eq!(sync.late_events_dropped, 0, "settlement must never count as late");
}

#[test]
fn test_settlement_arriving_ahead_of_grid_stays_buffered_then_lands() {
    use aetelier_types::funding::FundingSettlement;
    use rust_decimal::Decimal;

    let mut sync = MarketSynchronizer::trade_driven(PERIOD_1S);
    let now = 1_700_000_000 * PERIOD_1S;
    sync.on_trade(make_trade(now, 100.0, 1.0));

    let fs = FundingSettlement {
        funding_time_us: now - 3_600 * 1_000_000,
        local_ts_us: now + 10 * PERIOD_1S,
        rtt_us: 1_000,
        pair: btcusdt(),
        funding_rate: Decimal::new(-2, 4),
        premium: None,
        exchange: "hyperliquid".to_string(),
    };
    sync.on_funding_settlement(fs);

    sync.on_trade(make_trade(now + PERIOD_1S, 100.5, 1.0));
    let early: usize = sync.drain().iter().map(|s| s.funding_settlements.len()).sum();
    assert_eq!(early, 0, "future-arrival settlement stays buffered");

    sync.on_trade(make_trade(now + 12 * PERIOD_1S, 101.0, 1.0));
    let landed: usize = sync.drain().iter().map(|s| s.funding_settlements.len()).sum();
    assert_eq!(landed, 1);
    assert_eq!(sync.late_events_dropped, 0);
}
