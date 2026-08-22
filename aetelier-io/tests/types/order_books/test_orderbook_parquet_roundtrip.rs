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
    use aetelier_io::orderbooks::ob_parquet::{
        load_parquet_to_delta, load_parquet_to_ob, write_ob_delta_parquet,
        write_ob_parquet,
    };
    use aetelier_types::orderbooks::delta::NormalizedDelta;
    use aetelier_types::orderbooks::{Orderbook, OrderbookDelta, decimal_to_f64};
    use aetelier_types::trading_pair::TradingPair;
    use arrow::array::AsArray;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use rust_decimal::Decimal;
    use std::collections::BTreeMap;
    use std::fs::File;
    use std::path::{Path, PathBuf};
    use std::str::FromStr;
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

    const DELTA_FIELD_NAMES: [&str; 7] = [
        "timestamp_us",
        "symbol",
        "exchange",
        "side",
        "level",
        "price",
        "size",
    ];

    fn build_delta(
        exchange: &str,
        bids: &[(&str, &str)],
        asks: &[(&str, &str)],
    ) -> OrderbookDelta {
        fn to_map(levels: &[(&str, &str)]) -> BTreeMap<Decimal, Decimal> {
            levels
                .iter()
                .map(|(price, size)| {
                    (
                        Decimal::from_str(price).expect("price literal"),
                        Decimal::from_str(size).expect("size literal"),
                    )
                })
                .collect()
        }

        OrderbookDelta::from_maps(
            TradingPair::new("BTC", "USDT"),
            exchange,
            to_map(bids),
            to_map(asks),
        )
    }

    fn write_delta(dir: &Path, delta: &OrderbookDelta) -> PathBuf {
        let path = dir.join("delta.parquet");
        write_ob_delta_parquet(delta, &path).expect("delta parquet write");
        path
    }

    fn declared_field_names(path: &Path) -> Vec<String> {
        let file = File::open(path).expect("open delta parquet");
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(file).expect("parquet metadata");
        builder
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    }

    fn utf8_column_by_name(path: &Path, name: &str) -> Vec<String> {
        let file = File::open(path).expect("open delta parquet");
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(file).expect("parquet metadata");
        let reader = builder.build().expect("parquet reader");

        let mut values = Vec::new();
        for batch_result in reader {
            let batch = batch_result.expect("record batch");
            let index = batch
                .schema()
                .index_of(name)
                .unwrap_or_else(|_| panic!("column '{name}' not declared"));
            let column = batch.column(index).as_string::<i32>();
            for row in 0..batch.num_rows() {
                values.push(column.value(row).to_string());
            }
        }
        values
    }

    #[test]
    fn test_delta_parquet_named_columns_hold_their_own_values() {
        let dir = tempdir().unwrap();
        let delta = build_delta(
            "bybit",
            &[("21921.73", "0.063"), ("21921.00", "1.000")],
            &[("21922.00", "0.500"), ("21923.50", "2.000")],
        );
        let path = write_delta(dir.path(), &delta);

        assert_eq!(declared_field_names(&path), DELTA_FIELD_NAMES);
        assert_eq!(
            utf8_column_by_name(&path, "side"),
            vec!["bid", "bid", "ask", "ask"],
            "the column named 'side' must hold side values"
        );
        assert_eq!(
            utf8_column_by_name(&path, "exchange"),
            vec!["bybit"; 4],
            "the column named 'exchange' must hold exchange values"
        );
    }

    #[test]
    fn test_delta_parquet_empty_book_declares_named_columns() {
        let dir = tempdir().unwrap();
        let delta = build_delta("bybit", &[], &[]);
        let path = write_delta(dir.path(), &delta);

        assert_eq!(declared_field_names(&path), DELTA_FIELD_NAMES);
        assert!(utf8_column_by_name(&path, "side").is_empty());
        assert!(utf8_column_by_name(&path, "exchange").is_empty());
    }

    #[test]
    fn test_delta_parquet_single_level_one_sided_book() {
        let dir = tempdir().unwrap();
        let delta = build_delta("kraken", &[("21921.73", "0.063")], &[]);
        let path = write_delta(dir.path(), &delta);

        assert_eq!(utf8_column_by_name(&path, "side"), vec!["bid"]);
        assert_eq!(utf8_column_by_name(&path, "exchange"), vec!["kraken"]);
    }

    #[test]
    fn test_delta_parquet_exchange_named_like_a_side_stays_in_its_column() {
        let dir = tempdir().unwrap();
        let delta =
            build_delta("bid", &[("21921.73", "0.063")], &[("21922.00", "0.500")]);
        let path = write_delta(dir.path(), &delta);

        assert_eq!(utf8_column_by_name(&path, "side"), vec!["bid", "ask"]);
        assert_eq!(utf8_column_by_name(&path, "exchange"), vec!["bid", "bid"]);
    }

    #[test]
    fn test_delta_parquet_positional_reader_unaffected_by_field_rename() {
        let dir = tempdir().unwrap();
        let delta =
            build_delta("bybit", &[("21921.73", "0.063")], &[("21922.00", "0.500")]);
        let path = write_delta(dir.path(), &delta);

        let loaded = load_parquet_to_delta(&path).expect("delta parquet read");
        assert_eq!(loaded.exchange(), "bybit");
        assert_eq!(loaded.pair(), &TradingPair::new("BTC", "USDT"));
        assert_eq!(loaded.bid_depth(), 1);
        assert_eq!(loaded.ask_depth(), 1);
    }
}
