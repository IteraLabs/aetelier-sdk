//! Unit tests for [`EventSynchronizer`].

#[cfg(test)]
mod tests {
    use aetelier_connect::synchronizers::{EventSynchronizer, ReferenceEventType};
    use aetelier_types::{
        funding::FundingRate,
        levels::Level,
        liquidations::Liquidation,
        open_interest::OpenInterest,
        orderbooks::{Orderbook, decimal_to_f64, f64_to_decimal},
        orders::OrderSide,
        trades::{Trade, TradeSide},
        trading_pair::TradingPair,
    };

    // ------------------------------------------------------------------ //
    //  Helpers
    // ------------------------------------------------------------------ //

    fn btcusdt() -> TradingPair {
        TradingPair::new("BTC", "USDT")
    }

    /// Create an orderbook with `depth` levels on each side.
    /// Bids: 100 - i, Asks: 101 + i (i = 0..depth).
    fn make_ob(pair: TradingPair, exchange: &str, ts_us: u64, depth: usize) -> Orderbook {
        let bids: Vec<Level> = (0..depth)
            .map(|i| {
                Level::new(
                    i as u32,
                    OrderSide::Bids,
                    f64_to_decimal(100.0 - i as f64),
                    f64_to_decimal(1.0),
                    vec![],
                )
            })
            .collect();
        let asks: Vec<Level> = (0..depth)
            .map(|i| {
                Level::new(
                    i as u32,
                    OrderSide::Asks,
                    f64_to_decimal(101.0 + i as f64),
                    f64_to_decimal(1.0),
                    vec![],
                )
            })
            .collect();
        Orderbook::from_levels(0, ts_us, pair, exchange.to_string(), bids, asks)
    }

    fn make_trade(
        ts_ms: u64,
        pair: TradingPair,
        side: TradeSide,
        amount: f64,
        price: f64,
    ) -> Trade {
        Trade {
            source_trade_ts_us: ts_ms,
            local_trade_ts_us: 0,
            source_trade_rtt_us: 0,
            pair,
            side,
            amount: f64_to_decimal(amount),
            price: f64_to_decimal(price),
            exchange: "test".to_string(),
            id: format!("t_{}", ts_ms),
            origin: Default::default(),
        }
    }

    fn make_liq(
        ts_ms: u64,
        pair: TradingPair,
        side: TradeSide,
        amount: f64,
        price: f64,
    ) -> Liquidation {
        Liquidation {
            liquidation_ts_us: ts_ms,
            pair,
            side,
            amount: f64_to_decimal(amount),
            price: f64_to_decimal(price),
            exchange: "test".to_string(),
        }
    }

    fn make_fr(ts_ms: u64, pair: TradingPair, rate: f64) -> FundingRate {
        FundingRate {
            funding_rate_ts_us: ts_ms,
            local_funding_ts_us: 0,
            recv_seq: 0,
            conn_epoch: 0,
            pair,
            funding_rate: f64_to_decimal(rate),
            premium: None,
            interval_hours: 8,
            next_funding_ts_us: ts_ms + 28_800_000,
            exchange: "test".to_string(),
        }
    }

    fn make_oi(ts_ms: u64, pair: TradingPair, oi: f64, oi_value: f64) -> OpenInterest {
        OpenInterest {
            open_interest_ts_us: ts_ms,
            local_oi_ts_us: 0,
            recv_seq: 0,
            conn_epoch: 0,
            pair,
            open_interest: f64_to_decimal(oi),
            open_interest_value: Some(f64_to_decimal(oi_value)),
            mark_px: None,
            exchange: "test".to_string(),
        }
    }

    // ------------------------------------------------------------------ //
    //  Tests: basic emission
    // ------------------------------------------------------------------ //

    #[test]
    fn first_orderbook_emits_when_orderbook_is_reference() {
        let mut sync = EventSynchronizer::orderbook_only();
        let ob = make_ob(btcusdt(), "test", 1_000_000_000, 5);
        let emitted = sync.on_orderbook(&btcusdt(), 1_000_000_000, ob);
        assert_eq!(emitted, 1);
        assert_eq!(sync.buffer_len(), 1);
        assert_eq!(sync.total_captured(), 1);
    }

    #[test]
    fn first_orderbook_does_not_emit_when_not_reference() {
        let mut sync = EventSynchronizer::new(vec![ReferenceEventType::Trade]);
        let ob = make_ob(btcusdt(), "test", 1_000_000_000, 5);
        let emitted = sync.on_orderbook(&btcusdt(), 1_000_000_000, ob);
        assert_eq!(emitted, 0);
        assert_eq!(sync.buffer_len(), 0);
    }

    #[test]
    fn no_emission_before_first_orderbook() {
        // Trade is a reference event, but we haven't seen any orderbook yet.
        let mut sync = EventSynchronizer::new(vec![ReferenceEventType::Trade]);
        let trade = make_trade(1000, btcusdt(), TradeSide::Buy, 1.0, 50_000.0);
        let emitted = sync.on_trade(trade);
        assert_eq!(emitted, 0);
        assert_eq!(sync.buffer_len(), 0);
    }

    // ------------------------------------------------------------------ //
    //  Tests: event accumulation
    // ------------------------------------------------------------------ //

    #[test]
    fn trades_accumulate_between_reference_events() {
        let mut sync = EventSynchronizer::orderbook_only();

        // First OB → emit snapshot #1
        let ob1 = make_ob(btcusdt(), "test", 1_000_000_000, 3);
        sync.on_orderbook(&btcusdt(), 1_000_000_000, ob1);

        // Two trades arrive (not reference events)
        sync.on_trade(make_trade(1010, btcusdt(), TradeSide::Buy, 1.0, 50_000.0));
        sync.on_trade(make_trade(1020, btcusdt(), TradeSide::Sell, 0.5, 50_010.0));

        // Second OB → emit snapshot #2 containing the 2 trades
        let ob2 = make_ob(btcusdt(), "test", 2_000_000_000, 3);
        sync.on_orderbook(&btcusdt(), 2_000_000_000, ob2);

        assert_eq!(sync.buffer_len(), 2);
        assert_eq!(sync.buffer[1].trades.len(), 2);
    }

    #[test]
    fn liquidations_accumulate_between_reference_events() {
        let mut sync = EventSynchronizer::orderbook_only();

        let ob1 = make_ob(btcusdt(), "test", 1_000_000_000, 3);
        sync.on_orderbook(&btcusdt(), 1_000_000_000, ob1);

        sync.on_liquidation(make_liq(1010, btcusdt(), TradeSide::Sell, 10.0, 49_000.0));

        let ob2 = make_ob(btcusdt(), "test", 2_000_000_000, 3);
        sync.on_orderbook(&btcusdt(), 2_000_000_000, ob2);

        assert_eq!(sync.buffer_len(), 2);
        assert_eq!(sync.buffer[1].liquidations.len(), 1);
        assert_eq!(decimal_to_f64(sync.buffer[1].liquidations[0].amount), 10.0);
    }

    #[test]
    fn non_reference_events_do_not_emit() {
        // Only Orderbook is reference — trades should never emit.
        let mut sync = EventSynchronizer::orderbook_only();

        let ob = make_ob(btcusdt(), "test", 1_000_000_000, 3);
        sync.on_orderbook(&btcusdt(), 1_000_000_000, ob);

        let emitted =
            sync.on_trade(make_trade(1010, btcusdt(), TradeSide::Buy, 1.0, 50_000.0));
        assert_eq!(emitted, 0);
        assert_eq!(sync.buffer_len(), 1); // only the first OB snapshot
    }

    // ------------------------------------------------------------------ //
    //  Tests: trade / liquidation as reference events
    // ------------------------------------------------------------------ //

    #[test]
    fn trade_as_reference_event() {
        let mut sync = EventSynchronizer::new(vec![ReferenceEventType::Trade]);

        // OB arrives first (not reference) — updates state but no emission
        let ob = make_ob(btcusdt(), "test", 1_000_000_000, 3);
        sync.on_orderbook(&btcusdt(), 1_000_000_000, ob);
        assert_eq!(sync.buffer_len(), 0);

        // Trade arrives — IS reference → emit
        let trade = make_trade(1010, btcusdt(), TradeSide::Buy, 1.0, 50_000.0);
        let emitted = sync.on_trade(trade);
        assert_eq!(emitted, 1);
        assert_eq!(sync.buffer_len(), 1);

        // Snapshot should have the trade AND the orderbook state
        assert_eq!(sync.buffer[0].trades.len(), 1);
        assert!(sync.buffer[0].orderbook.is_some());
    }

    #[test]
    fn liquidation_as_reference_event() {
        let mut sync = EventSynchronizer::new(vec![ReferenceEventType::Liquidation]);

        let ob = make_ob(btcusdt(), "test", 1_000_000_000, 3);
        sync.on_orderbook(&btcusdt(), 1_000_000_000, ob);

        let liq = make_liq(1010, btcusdt(), TradeSide::Sell, 10.0, 49_000.0);
        let emitted = sync.on_liquidation(liq);
        assert_eq!(emitted, 1);
        assert_eq!(sync.buffer[0].liquidations.len(), 1);
        assert!(sync.buffer[0].orderbook.is_some());
    }

    // ------------------------------------------------------------------ //
    //  Tests: state-based carry-forward
    // ------------------------------------------------------------------ //

    #[test]
    fn funding_rates_carried_forward() {
        let mut sync = EventSynchronizer::orderbook_only();

        let ob1 = make_ob(btcusdt(), "test", 1_000_000_000, 3);
        sync.on_orderbook(&btcusdt(), 1_000_000_000, ob1);

        // Feed funding rate (state-based, never triggers emission)
        let emitted = sync.on_funding(make_fr(1010, btcusdt(), 0.0001));
        assert_eq!(emitted, 0);

        // Next OB → snapshot should carry forward the funding rate
        let ob2 = make_ob(btcusdt(), "test", 2_000_000_000, 3);
        sync.on_orderbook(&btcusdt(), 2_000_000_000, ob2);

        assert_eq!(sync.buffer[1].funding_rate.len(), 1);
        assert!(sync.buffer[1].funding_rate[0].funding_rate == f64_to_decimal(0.0001));
    }

    #[test]
    fn open_interest_carried_forward() {
        let mut sync = EventSynchronizer::orderbook_only();

        let ob1 = make_ob(btcusdt(), "test", 1_000_000_000, 3);
        sync.on_orderbook(&btcusdt(), 1_000_000_000, ob1);

        let emitted =
            sync.on_open_interest(make_oi(1010, btcusdt(), 1000.0, 50_000_000.0));
        assert_eq!(emitted, 0);

        let ob2 = make_ob(btcusdt(), "test", 2_000_000_000, 3);
        sync.on_orderbook(&btcusdt(), 2_000_000_000, ob2);

        assert_eq!(sync.buffer[1].open_interest.len(), 1);
        assert!(sync.buffer[1].open_interest[0].open_interest == f64_to_decimal(1000.0));
    }

    #[test]
    fn funding_rate_updates_replace_previous() {
        let mut sync = EventSynchronizer::orderbook_only();

        let ob1 = make_ob(btcusdt(), "test", 1_000_000_000, 3);
        sync.on_orderbook(&btcusdt(), 1_000_000_000, ob1);

        sync.on_funding(make_fr(1010, btcusdt(), 0.0001));
        sync.on_funding(make_fr(1020, btcusdt(), 0.0002)); // replaces

        let ob2 = make_ob(btcusdt(), "test", 2_000_000_000, 3);
        sync.on_orderbook(&btcusdt(), 2_000_000_000, ob2);

        assert_eq!(sync.buffer[1].funding_rate.len(), 1);
        assert!(sync.buffer[1].funding_rate[0].funding_rate == f64_to_decimal(0.0002));
    }

    // ------------------------------------------------------------------ //
    //  Tests: accumulation reset
    // ------------------------------------------------------------------ //

    #[test]
    fn accumulation_resets_after_emission() {
        let mut sync = EventSynchronizer::orderbook_only();

        let ob1 = make_ob(btcusdt(), "test", 1_000_000_000, 3);
        sync.on_orderbook(&btcusdt(), 1_000_000_000, ob1); // snapshot #1

        sync.on_trade(make_trade(1010, btcusdt(), TradeSide::Buy, 1.0, 50_000.0));

        let ob2 = make_ob(btcusdt(), "test", 2_000_000_000, 3);
        sync.on_orderbook(&btcusdt(), 2_000_000_000, ob2); // snapshot #2 (1 trade)

        let ob3 = make_ob(btcusdt(), "test", 3_000_000_000, 3);
        sync.on_orderbook(&btcusdt(), 3_000_000_000, ob3); // snapshot #3 (0 trades)

        assert_eq!(sync.buffer.len(), 3);
        assert_eq!(sync.buffer[1].trades.len(), 1);
        assert_eq!(sync.buffer[2].trades.len(), 0); // reset after #2
    }

    // ------------------------------------------------------------------ //
    //  Tests: all_events mode
    // ------------------------------------------------------------------ //

    #[test]
    fn all_events_mode_emits_on_each_reference() {
        let mut sync = EventSynchronizer::all_events();

        let ob = make_ob(btcusdt(), "test", 1_000_000_000, 3);
        let e1 = sync.on_orderbook(&btcusdt(), 1_000_000_000, ob); // emit
        assert_eq!(e1, 1);

        let e2 =
            sync.on_trade(make_trade(1010, btcusdt(), TradeSide::Buy, 1.0, 50_000.0)); // emit
        assert_eq!(e2, 1);

        let e3 = sync.on_liquidation(make_liq(
            1020,
            btcusdt(),
            TradeSide::Sell,
            5.0,
            49_000.0,
        )); // emit
        assert_eq!(e3, 1);

        assert_eq!(sync.buffer_len(), 3);
        assert_eq!(sync.total_captured(), 3);
    }

    // ------------------------------------------------------------------ //
    //  Tests: drain / buffer management
    // ------------------------------------------------------------------ //

    #[test]
    fn drain_clears_buffer_preserves_count() {
        let mut sync = EventSynchronizer::orderbook_only();

        let ob1 = make_ob(btcusdt(), "test", 1_000_000_000, 3);
        let ob2 = make_ob(btcusdt(), "test", 2_000_000_000, 3);
        sync.on_orderbook(&btcusdt(), 1_000_000_000, ob1);
        sync.on_orderbook(&btcusdt(), 2_000_000_000, ob2);

        assert_eq!(sync.buffer_len(), 2);
        let drained = sync.drain();
        assert_eq!(drained.len(), 2);
        assert_eq!(sync.buffer_len(), 0);
        assert_eq!(sync.total_captured(), 2);
    }

    #[test]
    fn snapshot_has_correct_timestamp() {
        let mut sync = EventSynchronizer::orderbook_only();
        let ob = make_ob(btcusdt(), "test", 1_500_000_000, 3);
        sync.on_orderbook(&btcusdt(), 1_500_000_000, ob);

        assert_eq!(sync.buffer[0].ts_us, 1_500_000_000);
    }

    // ------------------------------------------------------------------ //
    //  Tests: finalize
    // ------------------------------------------------------------------ //

    #[test]
    fn finalize_emits_remaining_data() {
        let mut sync = EventSynchronizer::orderbook_only();

        let ob = make_ob(btcusdt(), "test", 1_000_000_000, 3);
        sync.on_orderbook(&btcusdt(), 1_000_000_000, ob); // snapshot #1

        // Trades arrive but no more reference events
        sync.on_trade(make_trade(1010, btcusdt(), TradeSide::Buy, 1.0, 50_000.0));
        sync.on_trade(make_trade(1020, btcusdt(), TradeSide::Sell, 0.5, 50_010.0));

        assert_eq!(sync.buffer_len(), 1); // only the first snapshot

        sync.finalize();

        assert_eq!(sync.buffer_len(), 2); // finalize emitted one more
        assert_eq!(sync.buffer[1].trades.len(), 2);
    }

    #[test]
    fn finalize_does_nothing_when_no_pending_events() {
        let mut sync = EventSynchronizer::orderbook_only();

        let ob = make_ob(btcusdt(), "test", 1_000_000_000, 3);
        sync.on_orderbook(&btcusdt(), 1_000_000_000, ob);

        sync.finalize();

        // No extra snapshot — nothing pending
        assert_eq!(sync.buffer_len(), 1);
    }

    #[test]
    fn finalize_does_nothing_before_initialization() {
        let mut sync = EventSynchronizer::orderbook_only();
        sync.finalize();
        assert_eq!(sync.buffer_len(), 0);
    }

    // ------------------------------------------------------------------ //
    //  Tests: constructor panics
    // ------------------------------------------------------------------ //

    #[test]
    #[should_panic(expected = "at least one reference event type is required")]
    fn empty_reference_events_panics() {
        EventSynchronizer::new(vec![]);
    }
}
