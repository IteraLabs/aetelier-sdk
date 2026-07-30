#[cfg(test)]
mod tests {
    use aetelier_connect::synchronizers::ob_sync::*;
    use aetelier_types::orderbooks::Orderbook;
    use aetelier_types::trading_pair::TradingPair;

    fn btcusdt() -> TradingPair {
        TradingPair::new("BTC", "USDT")
    }

    fn ethusdt() -> TradingPair {
        TradingPair::new("ETH", "USDT")
    }

    fn make_ob(_symbol: &str, exchange: &str, ts_us: u64) -> Orderbook {
        Orderbook::from_levels(0, ts_us, btcusdt(), exchange.to_string(), vec![], vec![])
    }

    #[test]
    fn first_snapshot_anchors_grid() {
        let mut sync = ObSynchronizer::new(1_000_000); // 1s grid
        let captured = sync.on_snapshot(
            &btcusdt(),
            1_000_000,
            make_ob("BTC", "Exchange", 1_000_000),
        );
        assert_eq!(captured, 0);
        assert_eq!(sync.buffer_len(), 0);
    }

    #[test]
    fn same_period_updates_are_silent() {
        let mut sync = ObSynchronizer::new(1_000_000);
        sync.on_snapshot(&btcusdt(), 1_000_000, make_ob("BTC", "Exchange", 1_000_000));
        sync.on_snapshot(&btcusdt(), 1_100_000, make_ob("BTC", "Exchange", 1_100_000));
        sync.on_snapshot(&btcusdt(), 1_500_000, make_ob("BTC", "Exchange", 1_500_000));
        assert_eq!(sync.buffer_len(), 0);
    }

    #[test]
    fn boundary_crossing_emits_previous_snapshot() {
        let mut sync = ObSynchronizer::new(1_000_000); // 1s = 1e6 us

        // Period 1 (ts_us=1e6 → period=1)
        sync.on_snapshot(&btcusdt(), 1_000_000, make_ob("BTC", "Exchange", 1_000_000));

        // Period 1 still, later snapshot
        sync.on_snapshot(&btcusdt(), 1_500_000, make_ob("BTC", "Exchange", 1_500_000));

        // Period 2 (ts_us=2e6 → period=2)
        let captured = sync.on_snapshot(
            &btcusdt(),
            2_000_000,
            make_ob("BTC", "Exchange", 2_000_000),
        );
        assert_eq!(captured, 1);
        assert_eq!(sync.buffer_len(), 1);

        // The buffered snapshot should be the one from ts_us=1_500_000
        // but with grid-aligned timestamp = period 2 * 1e6
        let emitted = &sync.buffer[0];
        assert_eq!(emitted.orderbook_ts_us, 2 * 1_000_000);
    }

    #[test]
    fn gap_is_forward_filled() {
        let mut sync = ObSynchronizer::new(1_000_000);
        sync.on_snapshot(&btcusdt(), 1_000_000, make_ob("BTC", "Exchange", 1_000_000));

        // Jump from period 1 to period 4 → fill periods 2, 3, 4
        let captured = sync.on_snapshot(
            &btcusdt(),
            4_000_000,
            make_ob("BTC", "Exchange", 4_000_000),
        );
        assert_eq!(captured, 3);
        assert_eq!(sync.buffer_len(), 3);
    }

    #[test]
    fn finalize_captures_tail() {
        let mut sync = ObSynchronizer::new(1_000_000);
        sync.on_snapshot(&btcusdt(), 1_000_000, make_ob("BTC", "Exchange", 1_000_000));
        sync.on_snapshot(&btcusdt(), 1_800_000, make_ob("BTC", "Exchange", 1_800_000));
        assert_eq!(sync.buffer_len(), 0);

        sync.finalize();
        assert_eq!(sync.buffer_len(), 1);
        assert_eq!(sync.buffer[0].orderbook_ts_us, 2 * 1_000_000);
    }

    #[test]
    fn multi_symbol_independent() {
        let mut sync = ObSynchronizer::new(1_000_000);

        sync.on_snapshot(&btcusdt(), 1_000_000, make_ob("BTC", "Exchange", 1_000_000));
        sync.on_snapshot(&ethusdt(), 1_000_000, make_ob("ETH", "Exchange", 1_000_000));

        // BTC crosses, ETH doesn't
        let btc = sync.on_snapshot(
            &btcusdt(),
            2_000_000,
            make_ob("BTC", "Exchange", 2_000_000),
        );
        let eth = sync.on_snapshot(
            &ethusdt(),
            1_500_000,
            make_ob("ETH", "Exchange", 1_500_000),
        );

        assert_eq!(btc, 1);
        assert_eq!(eth, 0);
        assert_eq!(sync.buffer_len(), 1);
        assert_eq!(sync.buffer[0].pair, btcusdt());
    }

    #[test]
    fn drain_clears_buffer() {
        let mut sync = ObSynchronizer::new(1_000_000);
        sync.on_snapshot(&btcusdt(), 1_000_000, make_ob("BTC", "Exchange", 1_000_000));
        sync.on_snapshot(&btcusdt(), 2_000_000, make_ob("BTC", "Exchange", 2_000_000));
        assert_eq!(sync.buffer_len(), 1);

        let drained = sync.drain();
        assert_eq!(drained.len(), 1);
        assert_eq!(sync.buffer_len(), 0);
        assert_eq!(sync.total_captured(), 1); // lifetime count preserved
    }

    #[test]
    fn finalize_is_idempotent_and_terminal() {
        let mut sync = ObSynchronizer::new(1_000_000);
        sync.on_snapshot(&btcusdt(), 1_000_000, make_ob("BTC", "Exchange", 1_000_000));
        sync.on_snapshot(&btcusdt(), 2_000_000, make_ob("BTC", "Exchange", 2_000_000));
        let _ = sync.drain();

        assert!(!sync.is_finalized());
        sync.finalize();
        assert!(sync.is_finalized());
        let first = sync.drain();
        assert!(
            !first.is_empty(),
            "first finalize emits the closing snapshot"
        );

        sync.finalize();
        assert!(sync.drain().is_empty(), "double finalize emits nothing");
    }

    #[test]
    fn events_after_finalize_are_dropped_and_counted() {
        let mut sync = ObSynchronizer::new(1_000_000);
        sync.on_snapshot(&btcusdt(), 1_000_000, make_ob("BTC", "Exchange", 1_000_000));
        sync.finalize();
        let before = sync.late_events_dropped;

        assert_eq!(
            sync.on_snapshot(
                &btcusdt(),
                3_000_000,
                make_ob("BTC", "Exchange", 3_000_000)
            ),
            0
        );
        assert_eq!(
            sync.on_update(&ethusdt(), "Exchange", 3_000_000, || (vec![], vec![])),
            0
        );
        assert_eq!(sync.late_events_dropped - before, 2);
        // finalize emitted the anchored period's closing snapshot (ts 2e6);
        // the post-finalize events at 3e6 must not appear.
        let drained = sync.drain();
        assert!(
            drained.iter().all(|ob| ob.orderbook_ts_us <= 2_000_000),
            "no post-finalize snapshot reaches the buffer"
        );
    }
}
