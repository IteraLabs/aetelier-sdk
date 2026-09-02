//! Unit tests for [`MarketSnapshot`].

#[cfg(test)]
mod tests {
    use aetelier_types::{
        TradingPair,
        funding::FundingRate,
        levels::Level,
        liquidations::Liquidation,
        open_interest::OpenInterest,
        orderbooks::{Orderbook, f64_to_decimal},
        orders::OrderSide,
        snapshots::MarketSnapshot,
        trades::Trade,
        trades::TradeSide,
    };

    // ------------------------------------------------------------------ //
    //  Helpers
    // ------------------------------------------------------------------ //

    fn make_ob(base: &str, exchange: &str, ts_us: u64) -> Orderbook {
        let bids = vec![Level::new(
            0,
            OrderSide::Bids,
            f64_to_decimal(100.0),
            f64_to_decimal(1.0),
            vec![],
        )];
        let asks = vec![Level::new(
            0,
            OrderSide::Asks,
            f64_to_decimal(101.0),
            f64_to_decimal(1.0),
            vec![],
        )];
        Orderbook::from_levels(
            0,
            ts_us,
            TradingPair::new(base, "USDT"),
            exchange.to_string(),
            bids,
            asks,
        )
    }

    fn make_trade(ts_us: u64, amount: f64, price: f64) -> Trade {
        make_trade_side(ts_us, amount, price, TradeSide::Buy)
    }

    fn make_trade_side(ts_us: u64, amount: f64, price: f64, side: TradeSide) -> Trade {
        Trade {
            source_trade_ts_us: ts_us,
            local_trade_ts_us: 0,
            source_trade_rtt_us: 0,
            pair: TradingPair::new("BTC", "USDT"),
            side,
            amount: f64_to_decimal(amount),
            price: f64_to_decimal(price),
            exchange: "test".to_string(),
            id: "t1".to_string(),
            origin: Default::default(),
        }
    }

    fn make_liq(ts_us: u64, amount: f64, price: f64) -> Liquidation {
        Liquidation {
            liquidation_ts_us: ts_us,
            pair: TradingPair::new("BTC", "USDT"),
            side: TradeSide::Sell,
            amount: f64_to_decimal(amount),
            price: f64_to_decimal(price),
            exchange: "test".to_string(),
        }
    }

    fn make_fr(ts_us: u64, rate: f64) -> FundingRate {
        FundingRate {
            funding_rate_ts_us: ts_us,
            local_funding_ts_us: 0,
            recv_seq: 0,
            conn_epoch_us: 0,
            pair: TradingPair::new("BTC", "USDT"),
            funding_rate: f64_to_decimal(rate),
            premium: None,
            interval_hours: 8,
            // 8 hours in microseconds.
            next_funding_ts_us: ts_us + 28_800_000_000,
            exchange: "test".to_string(),
        }
    }

    fn make_oi(ts_us: u64, oi: f64, oi_value: f64) -> OpenInterest {
        OpenInterest {
            open_interest_ts_us: ts_us,
            local_oi_ts_us: 0,
            recv_seq: 0,
            conn_epoch_us: 0,
            pair: TradingPair::new("BTC", "USDT"),
            open_interest: f64_to_decimal(oi),
            open_interest_value: Some(f64_to_decimal(oi_value)),
            mark_px: None,
            exchange: "test".to_string(),
        }
    }

    // ------------------------------------------------------------------ //
    //  Tests: empty / has_data
    // ------------------------------------------------------------------ //

    #[test]
    fn empty_snapshot_has_no_data() {
        let snap = MarketSnapshot::empty(0);
        assert!(!snap.has_data());
        assert!(snap.orderbook.is_none());
        assert!(snap.trades.is_empty());
        assert!(snap.liquidations.is_empty());
        assert!(snap.funding_rate.is_empty());
        assert!(snap.open_interest.is_empty());
    }

    #[test]
    fn snapshot_with_orderbook_has_data() {
        let snap = MarketSnapshot {
            ts_us: 1_000,
            orderbook: Some(make_ob("BTC", "test", 1_000)),
            trades: Vec::new(),
            liquidations: Vec::new(),
            funding_rate: Vec::new(),
            open_interest: Vec::new(),
            funding_settlements: vec![],
        };
        assert!(snap.has_data());
    }

    #[test]
    fn snapshot_with_trades_has_data() {
        let snap = MarketSnapshot {
            ts_us: 1_000,
            orderbook: None,
            trades: vec![make_trade(1, 0.5, 100.0)],
            liquidations: Vec::new(),
            funding_rate: Vec::new(),
            open_interest: Vec::new(),
            funding_settlements: vec![],
        };
        assert!(snap.has_data());
    }

    #[test]
    fn snapshot_with_liquidations_has_data() {
        let snap = MarketSnapshot {
            ts_us: 1_000,
            orderbook: None,
            trades: Vec::new(),
            liquidations: vec![make_liq(1, 10.0, 50_000.0)],
            funding_rate: Vec::new(),
            open_interest: Vec::new(),
            funding_settlements: vec![],
        };
        assert!(snap.has_data());
    }

    #[test]
    fn snapshot_with_funding_has_data() {
        let snap = MarketSnapshot {
            ts_us: 1_000,
            orderbook: None,
            trades: Vec::new(),
            liquidations: Vec::new(),
            funding_rate: vec![make_fr(1, 0.0001)],
            open_interest: Vec::new(),
            funding_settlements: vec![],
        };
        assert!(snap.has_data());
    }

    #[test]
    fn snapshot_with_open_interest_has_data() {
        let snap = MarketSnapshot {
            ts_us: 1_000,
            orderbook: None,
            trades: Vec::new(),
            liquidations: Vec::new(),
            funding_rate: Vec::new(),
            open_interest: vec![make_oi(1, 1000.0, 50_000_000.0)],
            funding_settlements: vec![],
        };
        assert!(snap.has_data());
    }

    // ------------------------------------------------------------------ //
    //  Tests: aggregation methods
    // ------------------------------------------------------------------ //

    #[test]
    fn trade_volume_correct() {
        let snap = MarketSnapshot {
            ts_us: 1_000,
            orderbook: None,
            trades: vec![
                make_trade(1, 0.5, 100.0),
                make_trade(2, 1.5, 100.0),
                make_trade(3, 0.25, 100.0),
            ],
            liquidations: Vec::new(),
            funding_rate: Vec::new(),
            open_interest: Vec::new(),
            funding_settlements: vec![],
        };
        let vol = snap.trade_volume();
        assert!((vol - 2.25).abs() < f64::EPSILON);
    }

    #[test]
    fn trade_count_correct() {
        let snap = MarketSnapshot {
            ts_us: 1_000,
            orderbook: None,
            trades: vec![make_trade(1, 0.5, 100.0), make_trade(2, 1.5, 100.0)],
            liquidations: Vec::new(),
            funding_rate: Vec::new(),
            open_interest: Vec::new(),
            funding_settlements: vec![],
        };
        assert_eq!(snap.trade_count(), 2);
    }

    #[test]
    fn liquidation_notional_correct() {
        let snap = MarketSnapshot {
            ts_us: 1_000,
            orderbook: None,
            trades: Vec::new(),
            liquidations: vec![
                make_liq(1, 2.0, 50_000.0), // 100_000
                make_liq(2, 0.5, 60_000.0), // 30_000
            ],
            funding_rate: Vec::new(),
            open_interest: Vec::new(),
            funding_settlements: vec![],
        };
        let notional = snap.liquidation_notional();
        assert!((notional - 130_000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn empty_snapshot_aggregates_are_zero() {
        let snap = MarketSnapshot::empty(0);
        assert_eq!(snap.trade_volume(), 0.0);
        assert_eq!(snap.trade_count(), 0);
        assert_eq!(snap.liquidation_notional(), 0.0);
    }

    // ------------------------------------------------------------------ //
    //  Tests: MarketAggregate trade side split
    // ------------------------------------------------------------------ //

    #[test]
    fn market_aggregate_splits_trades_by_side() {
        use aetelier_types::snapshots::MarketAggregate;

        let snap = MarketSnapshot {
            ts_us: 1_000,
            orderbook: None,
            trades: vec![
                make_trade_side(1, 3.0, 100.0, TradeSide::Buy),
                make_trade_side(2, 1.0, 102.0, TradeSide::Sell),
                make_trade_side(3, 2.0, 101.0, TradeSide::Buy),
            ],
            liquidations: Vec::new(),
            funding_rate: Vec::new(),
            open_interest: Vec::new(),
            funding_settlements: vec![],
        };
        let agg = MarketAggregate::from_snapshot(&snap, 0.0);

        // Totals conserve across the split.
        assert_eq!(agg.trade_count, 3);
        assert!((agg.trade_volume - 6.0).abs() < 1e-9);
        assert!((agg.trade_notional - (300.0 + 102.0 + 202.0)).abs() < 1e-9);

        // Buy: 3@100 + 2@101 = 5 vol, 502 notional, 2 trades.
        assert_eq!(agg.trade_buy_count, 2);
        assert!((agg.trade_buy_volume - 5.0).abs() < 1e-9);
        assert!((agg.trade_buy_notional - 502.0).abs() < 1e-9);
        // Sell: 1@102 = 1 vol, 102 notional, 1 trade.
        assert_eq!(agg.trade_sell_count, 1);
        assert!((agg.trade_sell_volume - 1.0).abs() < 1e-9);

        // Order-flow imbalance = (5 - 1) / 6.
        assert!((agg.trade_imbalance - (4.0 / 6.0)).abs() < 1e-9);
        // Price path over the period.
        assert!((agg.trade_px_first - 100.0).abs() < 1e-9);
        assert!((agg.trade_px_last - 101.0).abs() < 1e-9);
    }

    #[test]
    fn market_aggregate_trades_empty_is_zero() {
        use aetelier_types::snapshots::MarketAggregate;

        let agg = MarketAggregate::from_snapshot(&MarketSnapshot::empty(0), 0.0);
        assert_eq!(agg.trade_count, 0);
        assert_eq!(agg.trade_buy_volume, 0.0);
        assert_eq!(agg.trade_sell_volume, 0.0);
        assert_eq!(agg.trade_imbalance, 0.0);
        assert_eq!(agg.trade_px_first, 0.0);
        assert_eq!(agg.trade_px_last, 0.0);
    }
}
