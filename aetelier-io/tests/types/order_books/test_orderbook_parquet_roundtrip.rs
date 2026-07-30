//! Parquet round-trip tests for orderbook data across all three exchanges.
//!
//! Each test constructs an `OrderbookDelta` from a static `NormalizedDelta`,
//! captures it as an `Orderbook` via `capture_levels`, writes to Parquet,
//! reads back, and asserts field equality.
//!
//! These tests require the `parquet` feature:
//!   cargo test --features parquet -p aetelier_types test_orderbook_parquet_roundtrip

#[cfg(test)]
#[cfg(feature = "parquet")]
mod tests {
    use aetelier_io::orderbooks::ob_parquet::{load_parquet_to_ob, write_ob_parquet};
    use aetelier_types::orderbooks::delta::NormalizedDelta;
    use aetelier_types::orderbooks::{Orderbook, OrderbookDelta, decimal_to_f64};
    use aetelier_types::trading_pair::TradingPair;
    use tempfile::tempdir;

    // ── Helpers ──────────────────────────────────────────────────────────

    /// Build a `NormalizedDelta` snapshot with known price levels.
    /// Uses prices in the ~21921 range for cross-exchange consistency
    /// with the Coinbase/Kraken/Bybit test fixtures.
    fn make_snapshot(symbol: &str) -> NormalizedDelta {
        NormalizedDelta {
            symbol: symbol.to_string(),
            bids: vec![
                ("21921.73".to_string(), "0.063".to_string()),
                ("21921.00".to_string(), "1.000".to_string()),
            ],
            asks: vec![
                ("21922.00".to_string(), "0.500".to_string()),
                ("21923.50".to_string(), "2.000".to_string()),
            ],
            update_id: 1000,
            sequence: 5000,
            source_orderbook_ts_us: 0,
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            checksum: None,
            orders: Vec::new(),
            is_snapshot: true,
        }
    }

    /// Build an `Orderbook` from a `NormalizedDelta` for a given exchange.
    ///
    /// The incoming `symbol` is only used to label the raw fixture; the book is
    /// always tracking the canonical `BTC/USDT` pair. `OrderbookDelta::process`
    /// validates the delta symbol against the pair, so the normalized snapshot
    /// uses the pair's canonical form to keep them consistent.
    fn build_orderbook(_symbol: &str, exchange: &str) -> Orderbook {
        let pair = TradingPair::new("BTC", "USDT");
        let normalized = make_snapshot(&pair.to_canonical());
        let mut delta = OrderbookDelta::new(pair.clone());
        delta
            .process(&normalized)
            .expect("snapshot processing should succeed");
        let template = Orderbook::from_levels(
            0,
            1672304484978000000, // ts in nanoseconds
            pair,
            exchange.to_string(),
            vec![],
            vec![],
        );
        template.capture_levels(&delta)
    }

    /// Assert an `Orderbook` round-tripped correctly.
    fn assert_ob(ob: &Orderbook, _symbol: &str, exchange: &str) {
        assert_eq!(ob.pair, TradingPair::new("BTC", "USDT"));
        assert_eq!(ob.exchange, exchange);
        assert_eq!(ob.bids.len(), 2, "expected 2 bid levels");
        assert_eq!(ob.asks.len(), 2, "expected 2 ask levels");

        // Best bid (highest price first in capture_levels)
        let best_bid = ob.bids.values().next_back().unwrap();
        let best_bid_price = decimal_to_f64(best_bid.price);
        assert!((best_bid_price - 21921.73).abs() < 0.01);
        let best_bid_volume = decimal_to_f64(best_bid.volume);
        assert!((best_bid_volume - 0.063).abs() < 0.001);

        // Best ask (lowest price first)
        let best_ask = ob.asks.values().next().unwrap();
        let best_ask_price = decimal_to_f64(best_ask.price);
        assert!((best_ask_price - 21922.00).abs() < 0.01);
        let best_ask_volume = decimal_to_f64(best_ask.volume);
        assert!((best_ask_volume - 0.500).abs() < 0.001);
    }

    // ── Per-exchange round-trip tests ────────────────────────────────────

    #[test]
    fn test_bybit_orderbook_parquet_roundtrip() {
        let dir = tempdir().unwrap();
        let ob = build_orderbook("btcusdt", "bybit");
        assert_eq!(ob.bids.len(), 2);
        assert_eq!(ob.asks.len(), 2);

        let path = write_ob_parquet(&[ob], dir.path(), "sync").unwrap();
        let loaded = load_parquet_to_ob(&path).unwrap();

        assert_eq!(loaded.len(), 1);
        assert_ob(&loaded[0], "btcusdt", "bybit");
    }

    #[test]
    fn test_coinbase_orderbook_parquet_roundtrip() {
        let dir = tempdir().unwrap();
        let ob = build_orderbook("btcusd", "coinbase");

        let path = write_ob_parquet(&[ob], dir.path(), "sync").unwrap();
        let loaded = load_parquet_to_ob(&path).unwrap();

        assert_eq!(loaded.len(), 1);
        assert_ob(&loaded[0], "btcusd", "coinbase");
    }

    #[test]
    fn test_kraken_orderbook_parquet_roundtrip() {
        let dir = tempdir().unwrap();
        let ob = build_orderbook("btcusd", "kraken");

        let path = write_ob_parquet(&[ob], dir.path(), "sync").unwrap();
        let loaded = load_parquet_to_ob(&path).unwrap();

        assert_eq!(loaded.len(), 1);
        assert_ob(&loaded[0], "btcusd", "kraken");
    }

    #[test]
    fn test_multi_exchange_orderbook_parquet_roundtrip() {
        let dir = tempdir().unwrap();

        // Write one file per exchange
        let exchanges = [
            ("btcusdt", "bybit"),
            ("btcusd", "coinbase"),
            ("btcusd", "kraken"),
        ];

        for (symbol, exchange) in &exchanges {
            let ob = build_orderbook(symbol, exchange);
            let path = write_ob_parquet(&[ob], dir.path(), "sync").unwrap();
            let loaded = load_parquet_to_ob(&path).unwrap();

            assert_eq!(loaded.len(), 1);
            assert_ob(&loaded[0], symbol, exchange);
        }
    }

    // ── Timestamped writer — filename convention ─────────────────────────

    #[test]
    fn test_ob_timestamped_writer_filename_and_mode() {
        let dir = tempdir().unwrap();
        let ob = build_orderbook("BTCUSDT", "bybit");

        // "sync" mode
        let path =
            write_ob_parquet(std::slice::from_ref(&ob), dir.path(), "sync").unwrap();
        let fname = path.file_name().unwrap().to_str().unwrap();
        assert!(
            fname.starts_with("bybit_BTC-USDT_ob_sync_"),
            "expected filename starting with 'bybit_BTC-USDT_ob_sync_', got: {}",
            fname
        );
        assert!(fname.ends_with(".parquet"));

        // "raw" mode
        let path_raw = write_ob_parquet(&[ob], dir.path(), "raw").unwrap();
        let fname_raw = path_raw.file_name().unwrap().to_str().unwrap();
        assert!(
            fname_raw.starts_with("bybit_BTC-USDT_ob_raw_"),
            "expected filename starting with 'bybit_BTC-USDT_ob_raw_', got: {}",
            fname_raw
        );

        // Roundtrip from timestamped file
        let loaded = load_parquet_to_ob(&path_raw).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_ob(&loaded[0], "BTCUSDT", "bybit");
    }

    // ── Multi-snapshot round-trip ────────────────────────────────────────

    #[test]
    fn test_multiple_snapshots_single_file() {
        let dir = tempdir().unwrap();

        // Two snapshots at different timestamps
        let normalized = make_snapshot("btcusdt");
        let pair = TradingPair::new("BTC", "USDT");
        let mut delta = OrderbookDelta::new(pair.clone());
        delta.process(&normalized).unwrap();
        let ob1 = Orderbook::from_levels(
            0,
            1672304484000000000,
            pair.clone(),
            "bybit".to_string(),
            vec![],
            vec![],
        )
        .capture_levels(&delta);

        let ob2 = Orderbook::from_levels(
            0,
            1672304485000000000,
            pair,
            "bybit".to_string(),
            vec![],
            vec![],
        )
        .capture_levels(&delta);

        let path = write_ob_parquet(&[ob1, ob2], dir.path(), "sync").unwrap();
        let loaded = load_parquet_to_ob(&path).unwrap();

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].orderbook_ts_us, 1672304484000000000);
        assert_eq!(loaded[1].orderbook_ts_us, 1672304485000000000);
        assert_eq!(loaded[0].bids.len(), 2);
        assert_eq!(loaded[1].bids.len(), 2);
    }

    // ── Symbol sanitization (slash → dash) ───────────────────────────────

    #[test]
    fn test_ob_slash_symbol_sanitized_in_filename() {
        let dir = tempdir().unwrap();

        let normalized = make_snapshot("BTC/USDT");
        let pair = TradingPair::new("BTC", "USDT");
        let mut delta = OrderbookDelta::new(pair.clone());
        delta.process(&normalized).unwrap();
        let ob = Orderbook::from_levels(
            0,
            1672304484000000000,
            pair,
            "kraken".to_string(),
            vec![],
            vec![],
        )
        .capture_levels(&delta);

        let path = write_ob_parquet(&[ob], dir.path(), "sync").unwrap();
        let fname = path.file_name().unwrap().to_str().unwrap();

        // Filename must use dash, not slash
        assert!(
            fname.starts_with("kraken_BTC-USDT_ob_sync_"),
            "expected slash sanitized to dash, got: {}",
            fname
        );
        assert!(!fname.contains('/'));

        // Data roundtrip preserves original pair
        let loaded = load_parquet_to_ob(&path).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].pair.to_canonical(), "BTC/USDT");
    }

    // ── Timestamp-model fields round-trip ────────────────────────────────

    /// The three timestamp-model fields on `Orderbook`
    /// (`source_orderbook_ts_us`, `local_orderbook_ts_us`, `source_orderbook_rtt_us`)
    /// must survive a Parquet write/read cycle. `orderbook_ts_us` is the grid ts
    /// and may differ from the source ts, so we assert it independently.
    #[test]
    fn test_ob_parquet_roundtrip_timestamps() {
        let dir = tempdir().unwrap();

        // Distinct, non-zero values for each timestamp-model field.
        const GRID_TS: u64 = 1_672_304_484_978_000_000; // grid/snapshot ns
        const SOURCE_TS: u64 = 1_672_304_484_123; // exchange ms
        const LOCAL_TS: u64 = 1_672_304_484_978_456_789; // receipt ns
        const SOURCE_RTT: u64 = 4_242; // us

        // Build an Orderbook via from_levels + capture_levels. Because
        // capture_levels resets the timestamp-model fields to 0, set them on
        // the resulting book *after* capture.
        let mut ob = build_orderbook("btcusdt", "bybit");
        ob.orderbook_ts_us = GRID_TS;
        ob.source_orderbook_ts_us = SOURCE_TS;
        ob.local_orderbook_ts_us = LOCAL_TS;
        ob.source_orderbook_rtt_us = SOURCE_RTT;

        let path = write_ob_parquet(&[ob], dir.path(), "test").unwrap();
        let loaded = load_parquet_to_ob(&path).unwrap();

        assert_eq!(loaded.len(), 1);
        let got = &loaded[0];

        // Grid ts (independent of the source ts).
        assert_eq!(
            got.orderbook_ts_us, GRID_TS,
            "orderbook_ts_us (grid) must survive"
        );

        // The three NEW timestamp-model fields must round-trip intact.
        assert_eq!(
            got.source_orderbook_ts_us, SOURCE_TS,
            "source_orderbook_ts_us must survive parquet round-trip"
        );
        assert_eq!(
            got.local_orderbook_ts_us, LOCAL_TS,
            "local_orderbook_ts_us must survive parquet round-trip"
        );
        assert_eq!(
            got.source_orderbook_rtt_us, SOURCE_RTT,
            "source_orderbook_rtt_us must survive parquet round-trip"
        );
    }
}
