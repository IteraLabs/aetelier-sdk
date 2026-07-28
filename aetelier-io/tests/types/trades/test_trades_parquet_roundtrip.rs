//! Parquet round-trip tests for trade data across all three exchanges.
//!
//! Each test constructs `Trade` structs with known fields, writes them
//! to Parquet via `write_trades_parquet`, reads back via
//! `read_trades_parquet`, and asserts field equality.
//!
//! These tests require the `parquet` feature:
//!   cargo test --features parquet -p aetelier_types test_trades_parquet_roundtrip

#[cfg(test)]
#[cfg(feature = "parquet")]
mod tests {
    use aetelier_io::trades::trades_parquet::{
        read_trades_parquet, write_trades_parquet, write_trades_parquet_timestamped,
    };
    use aetelier_types::TradeSide;
    use aetelier_types::orderbooks::{decimal_to_f64, f64_to_decimal};
    use aetelier_types::trades::Trade;
    use aetelier_types::trading_pair::TradingPair;
    use tempfile::tempdir;

    // ── Helpers ──────────────────────────────────────────────────────────

    /// Build a pair of trades (buy + sell) for a given exchange/symbol.
    fn make_trades(exchange: &str) -> Vec<Trade> {
        let pair = TradingPair::new("BTC", "USDT");
        vec![
            Trade {
                source_trade_ts_us: 1672304484932,
                local_trade_ts_us: 0,
                source_trade_rtt_us: 0,
                pair: pair.clone(),
                side: TradeSide::Buy,
                amount: f64_to_decimal(0.001),
                price: f64_to_decimal(23536.30),
                exchange: exchange.to_string(),
                id: format!("{}_buy_1", exchange),
                origin: Default::default(),
            },
            Trade {
                source_trade_ts_us: 1672304484900,
                local_trade_ts_us: 0,
                source_trade_rtt_us: 0,
                pair: pair.clone(),
                side: TradeSide::Sell,
                amount: f64_to_decimal(0.500),
                price: f64_to_decimal(23535.00),
                exchange: exchange.to_string(),
                id: format!("{}_sell_1", exchange),
                origin: Default::default(),
            },
        ]
    }

    /// Assert a loaded trade matches expected values.
    fn assert_trade(
        trade: &Trade,
        _symbol: &str,
        exchange: &str,
        side: TradeSide,
        price: f64,
    ) {
        assert_eq!(trade.pair, TradingPair::new("BTC", "USDT"));
        assert_eq!(trade.exchange, exchange);
        assert_eq!(trade.side, side);
        assert!((decimal_to_f64(trade.price) - price).abs() < 0.01);
        assert!(trade.source_trade_ts_us > 0);
        assert!(!trade.id.is_empty());
    }

    // ── Per-exchange round-trip tests ────────────────────────────────────

    #[test]
    fn test_bybit_trades_parquet_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bybit_trades.parquet");
        let trades = make_trades("bybit");

        write_trades_parquet(&trades, &path).unwrap();
        let loaded = read_trades_parquet(&path).unwrap();

        assert_eq!(loaded.len(), 2);
        assert_trade(&loaded[0], "btcusdt", "bybit", TradeSide::Buy, 23536.30);
        assert_trade(&loaded[1], "btcusdt", "bybit", TradeSide::Sell, 23535.00);

        // Verify amounts
        assert!((decimal_to_f64(loaded[0].amount) - 0.001).abs() < 0.0001);
        assert!((decimal_to_f64(loaded[1].amount) - 0.500).abs() < 0.001);
    }

    #[test]
    fn test_coinbase_trades_parquet_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("coinbase_trades.parquet");
        let trades = make_trades("coinbase");

        write_trades_parquet(&trades, &path).unwrap();
        let loaded = read_trades_parquet(&path).unwrap();

        assert_eq!(loaded.len(), 2);
        assert_trade(&loaded[0], "btcusd", "coinbase", TradeSide::Buy, 23536.30);
        assert_trade(&loaded[1], "btcusd", "coinbase", TradeSide::Sell, 23535.00);
    }

    #[test]
    fn test_kraken_trades_parquet_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("kraken_trades.parquet");
        let trades = make_trades("kraken");

        write_trades_parquet(&trades, &path).unwrap();
        let loaded = read_trades_parquet(&path).unwrap();

        assert_eq!(loaded.len(), 2);
        assert_trade(&loaded[0], "btcusd", "kraken", TradeSide::Buy, 23536.30);
        assert_trade(&loaded[1], "btcusd", "kraken", TradeSide::Sell, 23535.00);
    }

    #[test]
    fn test_multi_exchange_trades_parquet_roundtrip() {
        let dir = tempdir().unwrap();

        let exchanges = [
            ("btcusdt", "bybit"),
            ("btcusd", "coinbase"),
            ("btcusd", "kraken"),
        ];

        for (symbol, exchange) in &exchanges {
            let path = dir.path().join(format!("{}_trades.parquet", exchange));
            let trades = make_trades(exchange);

            write_trades_parquet(&trades, &path).unwrap();
            let loaded = read_trades_parquet(&path).unwrap();

            assert_eq!(loaded.len(), 2);
            assert_trade(&loaded[0], symbol, exchange, TradeSide::Buy, 23536.30);
            assert_trade(&loaded[1], symbol, exchange, TradeSide::Sell, 23535.00);
        }
    }

    // ── Trade ID preservation ────────────────────────────────────────────

    #[test]
    fn test_trade_id_preserved() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("id_test.parquet");

        let trades = vec![Trade {
            source_trade_ts_us: 1672304484932,
            local_trade_ts_us: 0,
            source_trade_rtt_us: 0,
            pair: TradingPair::new("BTC", "USDT"),
            side: TradeSide::Buy,
            amount: f64_to_decimal(1.0),
            price: f64_to_decimal(100.0),
            exchange: "bybit".to_string(),
            id: "2100000000006930573".to_string(),
            origin: Default::default(),
        }];

        write_trades_parquet(&trades, &path).unwrap();
        let loaded = read_trades_parquet(&path).unwrap();

        assert_eq!(loaded[0].id, "2100000000006930573");
    }

    // ── Timestamp ordering ───────────────────────────────────────────────

    #[test]
    fn test_timestamps_preserved_in_order() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ts_test.parquet");

        let pair = TradingPair::new("BTC", "USDT");
        let trades = vec![
            Trade {
                source_trade_ts_us: 1000,
                local_trade_ts_us: 0,
                source_trade_rtt_us: 0,
                pair: pair.clone(),
                side: TradeSide::Buy,
                amount: f64_to_decimal(1.0),
                price: f64_to_decimal(100.0),
                exchange: "bybit".to_string(),
                id: "1".to_string(),
                origin: Default::default(),
            },
            Trade {
                source_trade_ts_us: 2000,
                local_trade_ts_us: 0,
                source_trade_rtt_us: 0,
                pair: pair.clone(),
                side: TradeSide::Sell,
                amount: f64_to_decimal(2.0),
                price: f64_to_decimal(200.0),
                exchange: "bybit".to_string(),
                id: "2".to_string(),
                origin: Default::default(),
            },
            Trade {
                source_trade_ts_us: 3000,
                local_trade_ts_us: 0,
                source_trade_rtt_us: 0,
                pair: pair.clone(),
                side: TradeSide::Buy,
                amount: f64_to_decimal(3.0),
                price: f64_to_decimal(300.0),
                exchange: "bybit".to_string(),
                id: "3".to_string(),
                origin: Default::default(),
            },
        ];

        write_trades_parquet(&trades, &path).unwrap();
        let loaded = read_trades_parquet(&path).unwrap();

        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0].source_trade_ts_us, 1000);
        assert_eq!(loaded[1].source_trade_ts_us, 2000);
        assert_eq!(loaded[2].source_trade_ts_us, 3000);
    }

    // ── Timestamp-model: source/local/rtt survive the round-trip ─────────

    #[test]
    fn test_trades_parquet_roundtrip_timestamps() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ts_model.parquet");

        let pair = TradingPair::new("BTC", "USDT");
        let trades = vec![
            Trade {
                source_trade_ts_us: 1_672_304_484_932,
                local_trade_ts_us: 1_700_000_000_000_000_000,
                source_trade_rtt_us: 4_200,
                pair: pair.clone(),
                side: TradeSide::Buy,
                amount: f64_to_decimal(0.25),
                price: f64_to_decimal(23_536.30),
                exchange: "bybit".to_string(),
                id: "ts_buy_1".to_string(),
                origin: Default::default(),
            },
            Trade {
                source_trade_ts_us: 1_672_304_484_955,
                local_trade_ts_us: 1_700_000_000_123_456_789,
                source_trade_rtt_us: 9_999,
                pair: pair.clone(),
                side: TradeSide::Sell,
                amount: f64_to_decimal(0.75),
                price: f64_to_decimal(23_535.00),
                exchange: "bybit".to_string(),
                id: "ts_sell_1".to_string(),
                origin: Default::default(),
            },
        ];

        write_trades_parquet(&trades, &path).unwrap();
        let loaded = read_trades_parquet(&path).unwrap();

        assert_eq!(loaded.len(), 2);

        // All three timestamp-model fields must survive byte-for-byte.
        assert_eq!(loaded[0].source_trade_ts_us, 1_672_304_484_932);
        assert_eq!(loaded[0].local_trade_ts_us, 1_700_000_000_000_000_000);
        assert_eq!(loaded[0].source_trade_rtt_us, 4_200);

        assert_eq!(loaded[1].source_trade_ts_us, 1_672_304_484_955);
        assert_eq!(loaded[1].local_trade_ts_us, 1_700_000_000_123_456_789);
        assert_eq!(loaded[1].source_trade_rtt_us, 9_999);
    }

    // ── Backward compatibility: legacy files without the new columns ─────

    /// Hand-write a Parquet file using the *legacy* 7-column trade schema
    /// (no `local_trade_ts_us` / `source_trade_rtt_us`) and assert the reader is
    /// robust: it returns 0 for the absent columns instead of failing.
    #[test]
    fn test_trades_parquet_backward_compat() {
        use arrow::{
            array::{Float64Array, StringArray, UInt64Array},
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        use parquet::{
            arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties,
        };
        use std::{fs::File, sync::Arc};

        let dir = tempdir().unwrap();
        let path = dir.path().join("legacy_trades.parquet");

        // Legacy schema: exactly the original 7 columns, in the original
        // order, WITHOUT local_trade_ts_us / source_trade_rtt_us.
        let schema = Schema::new(vec![
            Field::new("source_trade_ts_us", DataType::UInt64, false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("side", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("exchange", DataType::Utf8, false),
            Field::new("id", DataType::Utf8, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(UInt64Array::from(vec![1_672_304_484_932_u64])),
                Arc::new(StringArray::from(vec!["BTC/USDT"])),
                Arc::new(StringArray::from(vec!["Buy"])),
                Arc::new(Float64Array::from(vec![23_536.30_f64])),
                Arc::new(Float64Array::from(vec![0.001_f64])),
                Arc::new(StringArray::from(vec!["bybit"])),
                Arc::new(StringArray::from(vec!["legacy_1"])),
            ],
        )
        .unwrap();

        let file = File::create(&path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Reader must tolerate the missing columns.
        let loaded = read_trades_parquet(&path).unwrap();

        assert_eq!(loaded.len(), 1);

        // Existing columns round-trip as usual.
        assert_eq!(loaded[0].source_trade_ts_us, 1_672_304_484_932);
        assert_eq!(loaded[0].exchange, "bybit");
        assert_eq!(loaded[0].side, TradeSide::Buy);
        assert_eq!(loaded[0].id, "legacy_1");
        assert!((decimal_to_f64(loaded[0].price) - 23_536.30).abs() < 0.01);
        assert!((decimal_to_f64(loaded[0].amount) - 0.001).abs() < 0.0001);

        // Absent timestamp-model columns default to 0.
        assert_eq!(loaded[0].local_trade_ts_us, 0);
        assert_eq!(loaded[0].source_trade_rtt_us, 0);
    }

    // ── Timestamped writer — filename convention ─────────────────────────

    #[test]
    fn test_trades_timestamped_writer_filename_and_mode() {
        let dir = tempdir().unwrap();
        let trades = make_trades("bybit");

        // "sync" mode
        let path = write_trades_parquet_timestamped(&trades, dir.path(), "sync").unwrap();
        let fname = path.file_name().unwrap().to_str().unwrap();
        assert!(
            fname.starts_with("bybit_BTC-USDT_trades_sync_"),
            "expected filename starting with 'bybit_BTC-USDT_trades_sync_', got: {}",
            fname
        );
        assert!(fname.ends_with(".parquet"));

        // Roundtrip
        let loaded = read_trades_parquet(&path).unwrap();
        assert_eq!(loaded.len(), 2);

        // "raw" mode
        let path_raw =
            write_trades_parquet_timestamped(&trades, dir.path(), "raw").unwrap();
        let fname_raw = path_raw.file_name().unwrap().to_str().unwrap();
        assert!(
            fname_raw.starts_with("bybit_BTC-USDT_trades_raw_"),
            "expected filename starting with 'bybit_BTC-USDT_trades_raw_', got: {}",
            fname_raw
        );
    }

    // ── Symbol sanitization (slash → dash) ───────────────────────────────

    #[test]
    fn test_trades_slash_symbol_sanitized_in_filename() {
        let dir = tempdir().unwrap();
        let trades = make_trades("kraken");

        let path = write_trades_parquet_timestamped(&trades, dir.path(), "sync").unwrap();
        let fname = path.file_name().unwrap().to_str().unwrap();

        // Filename must use dash, not slash
        assert!(
            fname.starts_with("kraken_BTC-USDT_trades_sync_"),
            "expected slash sanitized to dash, got: {}",
            fname
        );
        assert!(!fname.contains('/'));

        // Data roundtrip preserves original pair
        let loaded = read_trades_parquet(&path).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].pair.to_canonical(), "BTC/USDT");
    }
}
