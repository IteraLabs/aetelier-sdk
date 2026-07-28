//! Unit tests for `OrderbookDelta::process()` error paths.
//!
//! Covers: NotInitialized, SymbolMismatch, ParseError.
//! Also tests `produce_snapshot()` on an empty book.

#[cfg(test)]
mod tests {
    use aetelier_types::TradingPair;
    use aetelier_types::errors::OrderbookError;
    use aetelier_types::orderbooks::delta::{NormalizedDelta, OrderbookDelta};

    fn snapshot_delta(symbol: &str) -> NormalizedDelta {
        NormalizedDelta {
            symbol: symbol.to_string(),
            bids: vec![("100.00".into(), "1.0".into())],
            asks: vec![("101.00".into(), "1.0".into())],
            update_id: 1,
            sequence: 0,
            source_orderbook_ts_us: 0,
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            checksum: None,
            orders: Vec::new(),
            is_snapshot: true,
        }
    }

    fn incremental_delta(symbol: &str) -> NormalizedDelta {
        NormalizedDelta {
            symbol: symbol.to_string(),
            bids: vec![("99.00".into(), "2.0".into())],
            asks: vec![("102.00".into(), "2.0".into())],
            update_id: 2,
            sequence: 0,
            source_orderbook_ts_us: 0,
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            checksum: None,
            orders: Vec::new(),
            is_snapshot: false,
        }
    }

    // ── NotInitialized ──────────────────────────────────────────────────

    #[test]
    fn delta_before_snapshot_returns_not_initialized() {
        let mut ob = OrderbookDelta::new(TradingPair::new("BTC", "USDT"));
        let delta = incremental_delta("BTCUSDT");
        let err = ob.process(&delta).unwrap_err();
        assert_eq!(err, OrderbookError::NotInitialized);
    }

    // ── SymbolMismatch ──────────────────────────────────────────────────

    #[test]
    fn snapshot_with_wrong_symbol() {
        let mut ob = OrderbookDelta::new(TradingPair::new("BTC", "USDT"));
        // First init
        ob.process(&snapshot_delta("BTCUSDT")).unwrap();
        // Now send a snapshot for a different symbol
        let bad = snapshot_delta("ETHUSDT");
        let err = ob.process(&bad).unwrap_err();
        assert_eq!(
            err,
            OrderbookError::SymbolMismatch {
                expected: "BTC/USDT".into(),
                received: "ETHUSDT".into(),
            }
        );
    }

    #[test]
    fn delta_with_wrong_symbol() {
        let mut ob = OrderbookDelta::new(TradingPair::new("BTC", "USDT"));
        ob.process(&snapshot_delta("BTCUSDT")).unwrap();
        let bad = incremental_delta("ETHUSDT");
        let err = ob.process(&bad).unwrap_err();
        assert_eq!(
            err,
            OrderbookError::SymbolMismatch {
                expected: "BTC/USDT".into(),
                received: "ETHUSDT".into(),
            }
        );
    }

    // ── ParseError ──────────────────────────────────────────────────────

    #[test]
    fn bad_price_string_returns_parse_error() {
        let mut ob = OrderbookDelta::new(TradingPair::new("BTC", "USDT"));
        let bad_snap = NormalizedDelta {
            symbol: "BTCUSDT".to_string(),
            bids: vec![("not_a_number".into(), "1.0".into())],
            asks: vec![],
            update_id: 1,
            sequence: 0,
            source_orderbook_ts_us: 0,
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            checksum: None,
            orders: Vec::new(),
            is_snapshot: true,
        };
        let err = ob.process(&bad_snap).unwrap_err();
        match err {
            OrderbookError::ParseError(msg) => {
                assert!(!msg.is_empty(), "parse error message should be non-empty");
            }
            other => panic!("expected ParseError, got {other:?}"),
        }
    }

    #[test]
    fn bad_size_string_returns_parse_error() {
        let mut ob = OrderbookDelta::new(TradingPair::new("BTC", "USDT"));
        let bad_snap = NormalizedDelta {
            symbol: "BTCUSDT".to_string(),
            bids: vec![("100.00".into(), "xyz".into())],
            asks: vec![],
            update_id: 1,
            sequence: 0,
            source_orderbook_ts_us: 0,
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            checksum: None,
            orders: Vec::new(),
            is_snapshot: true,
        };
        let err = ob.process(&bad_snap).unwrap_err();
        match err {
            OrderbookError::ParseError(_) => {}
            other => panic!("expected ParseError, got {other:?}"),
        }
    }

    // ── produce_snapshot on empty book ───────────────────────────────────

    #[test]
    fn produce_snapshot_empty_book() {
        let mut ob = OrderbookDelta::new(TradingPair::new("BTC", "USDT"));
        let snap = OrderbookDelta::produce_snapshot(&mut ob);
        assert_eq!(snap.pair, TradingPair::new("BTC", "USDT"));
        assert!(snap.bids.is_empty());
        assert!(snap.asks.is_empty());
        assert_eq!(snap.bid_depth, 0);
        assert_eq!(snap.ask_depth, 0);
        assert!(snap.mid_price.is_none());
        assert!(snap.spread.is_none());
    }

    // ── Snapshot happy path ─────────────────────────────────────────────

    #[test]
    fn snapshot_initializes_book() {
        let mut ob = OrderbookDelta::new(TradingPair::new("BTC", "USDT"));
        assert!(!ob.is_initialized());

        let update = ob.process(&snapshot_delta("BTCUSDT")).unwrap();
        assert!(ob.is_initialized());
        assert!(update.was_reset);
        assert_eq!(ob.bid_depth(), 1);
        assert_eq!(ob.ask_depth(), 1);
    }

    // ── Delta after snapshot ────────────────────────────────────────────

    #[test]
    fn delta_adds_levels() {
        let mut ob = OrderbookDelta::new(TradingPair::new("BTC", "USDT"));
        ob.process(&snapshot_delta("BTCUSDT")).unwrap();

        let update = ob.process(&incremental_delta("BTCUSDT")).unwrap();
        assert!(!update.was_reset);
        assert_eq!(ob.bid_depth(), 2);
        assert_eq!(ob.ask_depth(), 2);
        assert_eq!(ob.delta_count(), 1);
    }

    // ── Delete level (size = 0) ─────────────────────────────────────────

    #[test]
    fn delta_size_zero_deletes_level() {
        let mut ob = OrderbookDelta::new(TradingPair::new("BTC", "USDT"));
        ob.process(&snapshot_delta("BTCUSDT")).unwrap();
        assert_eq!(ob.bid_depth(), 1);

        let delete = NormalizedDelta {
            symbol: "BTCUSDT".to_string(),
            bids: vec![("100.00".into(), "0".into())],
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
        let update = ob.process(&delete).unwrap();
        assert_eq!(ob.bid_depth(), 0);
        assert_eq!(update.levels_deleted, 1);
    }
}
