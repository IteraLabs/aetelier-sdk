//! Unit tests for `MarketAggregate` computation.

use aetelier_connect::synchronizers::MarketSynchronizer;
use aetelier_types::{
    Level, Liquidation, OrderSide, Trade, TradeSide,
    funding::FundingRate,
    open_interest::OpenInterest,
    orderbooks::{Orderbook, f64_to_decimal},
    snapshots::{MarketAggregate, MarketSnapshot},
    trading_pair::TradingPair,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn btcusdt() -> TradingPair {
    TradingPair::new("BTC", "USDT")
}

fn make_ob_with_levels(
    ts: u64,
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
) -> Orderbook {
    let bid_levels: Vec<Level> = bids
        .into_iter()
        .enumerate()
        .map(|(i, (price, vol))| {
            Level::new(
                i as u32,
                OrderSide::Bids,
                f64_to_decimal(price),
                f64_to_decimal(vol),
                vec![],
            )
        })
        .collect();
    let ask_levels: Vec<Level> = asks
        .into_iter()
        .enumerate()
        .map(|(i, (price, vol))| {
            Level::new(
                i as u32,
                OrderSide::Asks,
                f64_to_decimal(price),
                f64_to_decimal(vol),
                vec![],
            )
        })
        .collect();

    Orderbook::from_levels(
        0,
        ts,
        btcusdt(),
        "bybit".to_string(),
        bid_levels,
        ask_levels,
    )
}

fn make_trade(ts_ms: u64, price: f64, amount: f64, side: TradeSide) -> Trade {
    Trade {
        source_trade_ts_us: ts_ms,
        local_trade_ts_us: 0,
        source_trade_rtt_us: 0,
        pair: btcusdt(),
        side,
        amount: f64_to_decimal(amount),
        price: f64_to_decimal(price),
        exchange: "bybit".to_string(),
        id: "t1".to_string(),
        origin: Default::default(),
    }
}

fn make_liquidation(ts_ms: u64, price: f64, amount: f64, side: TradeSide) -> Liquidation {
    Liquidation {
        liquidation_ts_us: ts_ms,
        pair: btcusdt(),
        side,
        amount: f64_to_decimal(amount),
        price: f64_to_decimal(price),
        exchange: "bybit".to_string(),
    }
}

fn make_funding(ts_ms: u64, rate: f64, next_ts: u64) -> FundingRate {
    FundingRate {
        funding_rate_ts_us: ts_ms,
        local_funding_ts_us: 0,
        recv_seq: 0,
        conn_epoch: 0,
        pair: btcusdt(),
        funding_rate: f64_to_decimal(rate),
        premium: None,
        interval_hours: 8,
        next_funding_ts_us: next_ts,
        exchange: "bybit".to_string(),
    }
}

fn make_oi(ts_ms: u64, contracts: f64, value: f64) -> OpenInterest {
    OpenInterest {
        open_interest_ts_us: ts_ms,
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
// Empty snapshot
// ---------------------------------------------------------------------------

#[test]
fn test_aggregate_empty_snapshot() {
    let snap = MarketSnapshot::empty(1_000_000_000);
    let agg = MarketAggregate::from_snapshot(&snap, 0.0);

    assert_eq!(agg.ts_us, 1_000_000_000);
    assert_eq!(agg.ob_mid_price, 0.0);
    assert_eq!(agg.ob_spread, 0.0);
    assert_eq!(agg.ob_imbalance, 0.0);
    assert_eq!(agg.trade_volume, 0.0);
    assert_eq!(agg.trade_vwap, 0.0);
    assert_eq!(agg.trade_count, 0);
    assert_eq!(agg.liq_notional, 0.0);
    assert_eq!(agg.liq_count, 0);
    assert_eq!(agg.liq_imbalance, 0.0);
    assert_eq!(agg.fr_rate, 0.0);
    assert_eq!(agg.fr_annualized, 0.0);
    assert_eq!(agg.fr_next_settlement_delta, 0);
    assert_eq!(agg.oi_contracts, 0.0);
    assert_eq!(agg.oi_value, 0.0);
    assert_eq!(agg.oi_change, 0.0);
}

// ---------------------------------------------------------------------------
// Orderbook aggregates
// ---------------------------------------------------------------------------

#[test]
fn test_aggregate_orderbook_mid_price() {
    let ob = make_ob_with_levels(1_000_000_000, vec![(100.0, 5.0)], vec![(102.0, 3.0)]);

    let snap = MarketSnapshot {
        ts_us: 1_000_000_000,
        orderbook: Some(ob),
        trades: vec![],
        liquidations: vec![],
        funding_rate: vec![],
        open_interest: vec![],
        funding_settlements: vec![],
    };

    let agg = MarketAggregate::from_snapshot(&snap, 0.0);
    assert!((agg.ob_mid_price - 101.0).abs() < f64::EPSILON);
    assert!((agg.ob_spread - 2.0).abs() < f64::EPSILON);
}

#[test]
fn test_aggregate_orderbook_imbalance() {
    // bid_vol=10, ask_vol=5 → imbalance = (10-5)/(10+5) = 5/15 ≈ 0.333
    let ob = make_ob_with_levels(
        1_000_000_000,
        vec![(100.0, 6.0), (99.0, 4.0)],
        vec![(101.0, 3.0), (102.0, 2.0)],
    );

    let snap = MarketSnapshot {
        ts_us: 1_000_000_000,
        orderbook: Some(ob),
        trades: vec![],
        liquidations: vec![],
        funding_rate: vec![],
        open_interest: vec![],
        funding_settlements: vec![],
    };

    let agg = MarketAggregate::from_snapshot(&snap, 0.0);
    let expected = (10.0 - 5.0) / (10.0 + 5.0);
    assert!((agg.ob_imbalance - expected).abs() < 1e-10);
}

// ---------------------------------------------------------------------------
// Trade aggregates
// ---------------------------------------------------------------------------

#[test]
fn test_aggregate_trade_vwap() {
    // Trade 1: 100 @ 0.5 = 50 notional
    // Trade 2: 200 @ 1.0 = 200 notional
    // VWAP = (50 + 200) / (0.5 + 1.0) = 250 / 1.5 ≈ 166.667
    let snap = MarketSnapshot {
        ts_us: 1_000_000_000,
        orderbook: None,
        trades: vec![
            make_trade(1000, 100.0, 0.5, TradeSide::Buy),
            make_trade(1100, 200.0, 1.0, TradeSide::Sell),
        ],
        liquidations: vec![],
        funding_rate: vec![],
        open_interest: vec![],
        funding_settlements: vec![],
    };

    let agg = MarketAggregate::from_snapshot(&snap, 0.0);
    assert_eq!(agg.trade_count, 2);
    assert!((agg.trade_volume - 1.5).abs() < f64::EPSILON);
    let expected_vwap = (100.0 * 0.5 + 200.0 * 1.0) / 1.5;
    assert!((agg.trade_vwap - expected_vwap).abs() < 1e-10);
}

// ---------------------------------------------------------------------------
// Liquidation aggregates
// ---------------------------------------------------------------------------

#[test]
fn test_aggregate_liquidation_imbalance() {
    // Buy liq: 100 * 2 = 200 notional
    // Sell liq: 90 * 1 = 90 notional
    // imbalance = (200 - 90) / 290 ≈ 0.3793
    let snap = MarketSnapshot {
        ts_us: 1_000_000_000,
        orderbook: None,
        trades: vec![],
        liquidations: vec![
            make_liquidation(1000, 100.0, 2.0, TradeSide::Buy),
            make_liquidation(1100, 90.0, 1.0, TradeSide::Sell),
        ],
        funding_rate: vec![],
        open_interest: vec![],
        funding_settlements: vec![],
    };

    let agg = MarketAggregate::from_snapshot(&snap, 0.0);
    assert_eq!(agg.liq_count, 2);
    assert!((agg.liq_notional - 290.0).abs() < f64::EPSILON);
    let expected = (200.0 - 90.0) / 290.0;
    assert!((agg.liq_imbalance - expected).abs() < 1e-10);
}

// ---------------------------------------------------------------------------
// Funding rate aggregates
// ---------------------------------------------------------------------------

#[test]
fn test_aggregate_funding_rate() {
    let snap = MarketSnapshot {
        ts_us: 1_000_000_000,
        orderbook: None,
        trades: vec![],
        liquidations: vec![],
        funding_rate: vec![make_funding(1000, 0.0001, 29_800_000)],
        open_interest: vec![],
        funding_settlements: vec![],
    };

    let agg = MarketAggregate::from_snapshot(&snap, 0.0);
    assert!((agg.fr_rate - 0.0001).abs() < f64::EPSILON);
    assert!((agg.fr_annualized - 0.0001 * 3.0 * 365.0).abs() < 1e-10);
    assert_eq!(agg.fr_next_settlement_delta, 29_800_000 - 1000);
}

// ---------------------------------------------------------------------------
// Open interest aggregates
// ---------------------------------------------------------------------------

#[test]
fn test_aggregate_oi_change() {
    let snap = MarketSnapshot {
        ts_us: 1_000_000_000,
        orderbook: None,
        trades: vec![],
        liquidations: vec![],
        funding_rate: vec![],
        open_interest: vec![make_oi(1000, 5000.0, 250_000_000.0)],
        funding_settlements: vec![],
    };

    // First snapshot: prev_oi=0.0, change = 5000 - 0 = 5000
    let agg1 = MarketAggregate::from_snapshot(&snap, 0.0);
    assert!((agg1.oi_contracts - 5000.0).abs() < f64::EPSILON);
    assert!((agg1.oi_value - 250_000_000.0).abs() < f64::EPSILON);
    assert!((agg1.oi_change - 5000.0).abs() < f64::EPSILON);

    // Second snapshot: prev_oi=5000.0, OI=5200, change = 200
    let snap2 = MarketSnapshot {
        ts_us: 2_000_000_000,
        orderbook: None,
        trades: vec![],
        liquidations: vec![],
        funding_rate: vec![],
        open_interest: vec![make_oi(2000, 5200.0, 260_000_000.0)],
        funding_settlements: vec![],
    };
    let agg2 = MarketAggregate::from_snapshot(&snap2, agg1.oi_contracts);
    assert!((agg2.oi_change - 200.0).abs() < f64::EPSILON);
}

// ---------------------------------------------------------------------------
// market_aggregate() on MarketSynchronizer
// ---------------------------------------------------------------------------

#[test]
fn test_market_aggregate_drains_buffer() {
    let mut sync = MarketSynchronizer::external_clock(1_000_000_000);
    sync.on_time(1_000_000_000);
    // Pre-anchor trade: its row label is behind the clock anchor, so it
    // is dropped and counted — invisible to the aggregates.
    sync.on_trade(make_trade(1500, 50_000.0, 0.1, TradeSide::Buy));
    // Anchor-period trade: emits in the row that closes its own period.
    sync.on_trade(make_trade(1_500_000_000, 50_000.0, 0.1, TradeSide::Buy));
    sync.on_time(2_000_000_000);

    assert_eq!(sync.buffer_len(), 1);
    assert_eq!(sync.late_events_dropped, 1);

    let aggregates = sync.market_aggregate();
    assert_eq!(aggregates.len(), 1);
    assert_eq!(sync.buffer_len(), 0); // drained

    assert_eq!(aggregates[0].trade_count, 1);
    assert!((aggregates[0].trade_volume - 0.1).abs() < f64::EPSILON);
}

#[test]
fn test_market_aggregate_oi_change_across_snapshots() {
    let mut sync = MarketSynchronizer::external_clock(1_000_000_000);
    sync.on_time(1_000_000_000);

    // Each OI record lands in the row that closes its own period: row 2
    // covers [1e9, 2e9), row 3 covers [2e9, 3e9).
    sync.on_open_interest(make_oi(1_500_000_000, 1000.0, 50_000_000.0));
    sync.on_time(2_000_000_000);

    sync.on_open_interest(make_oi(2_500_000_000, 1500.0, 75_000_000.0));
    sync.on_time(3_000_000_000);

    let aggs = sync.market_aggregate();
    assert_eq!(aggs.len(), 2);
    assert_eq!(aggs[0].ts_us, 2_000_000_000);
    assert_eq!(aggs[1].ts_us, 3_000_000_000);
    assert_eq!(sync.late_events_dropped, 0);

    // First: oi_change = 1000 - 0 = 1000
    assert!((aggs[0].oi_change - 1000.0).abs() < f64::EPSILON);
    // Second: oi_change = 1500 - 1000 = 500
    assert!((aggs[1].oi_change - 500.0).abs() < f64::EPSILON);
}
