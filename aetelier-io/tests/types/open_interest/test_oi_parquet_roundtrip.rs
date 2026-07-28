//! Parquet round-trip tests for open interest data.
//!
//! Each test constructs `OpenInterest` structs with known fields, writes them
//! to Parquet via `write_oi_parquet`, reads back via `read_oi_parquet`,
//! and asserts field equality.
//!
//! These tests require the `parquet` feature:
//!   cargo test --features parquet -p aetelier_types test_oi_parquet_roundtrip

#[cfg(test)]
#[cfg(feature = "parquet")]
mod tests {
    use aetelier_io::open_interest::oi_parquet::{
        read_oi_parquet, write_oi_parquet, write_oi_parquet_timestamped,
    };
    use aetelier_types::open_interest::OpenInterest;
    use aetelier_types::trading_pair::TradingPair;
    use tempfile::tempdir;

    // ── Helpers ──────────────────────────────────────────────────────────

    /// Build a pair of open interest records for a given exchange/symbol.
    fn make_oi_records(exchange: &str) -> Vec<OpenInterest> {
        let pair = TradingPair::new("BTC", "USDT");
        vec![
            OpenInterest {
                open_interest_ts_us: 1672304484000,
                pair: pair.clone(),
                open_interest: 32000.5,
                open_interest_value: 752_000_000.0,
                exchange: exchange.to_string(),
            },
            OpenInterest {
                open_interest_ts_us: 1672304784000,
                pair: pair.clone(),
                open_interest: 32100.0,
                open_interest_value: 754_350_000.0,
                exchange: exchange.to_string(),
            },
        ]
    }

    /// Assert a loaded OI record matches expected values.
    fn assert_oi(
        oi: &OpenInterest,
        _symbol: &str,
        exchange: &str,
        interest: f64,
        value: f64,
    ) {
        assert_eq!(oi.pair, TradingPair::new("BTC", "USDT"));
        assert_eq!(oi.exchange, exchange);
        assert!(
            (oi.open_interest - interest).abs() < 1.0,
            "expected OI ~{}, got {}",
            interest,
            oi.open_interest,
        );
        assert!(
            (oi.open_interest_value - value).abs() < 100.0,
            "expected OI value ~{}, got {}",
            value,
            oi.open_interest_value,
        );
        assert!(oi.open_interest_ts_us > 0);
    }

    // ── Per-exchange round-trip tests ────────────────────────────────────

    #[test]
    fn test_bybit_oi_parquet_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bybit_oi.parquet");
        let records = make_oi_records("bybit");

        write_oi_parquet(&records, &path).unwrap();
        let loaded = read_oi_parquet(&path).unwrap();

        assert_eq!(loaded.len(), 2);
        assert_oi(&loaded[0], "btcusdt", "bybit", 32000.5, 752_000_000.0);
        assert_oi(&loaded[1], "btcusdt", "bybit", 32100.0, 754_350_000.0);
    }

    #[test]
    fn test_multi_exchange_oi_parquet_roundtrip() {
        let dir = tempdir().unwrap();

        let exchanges = [
            ("btcusdt", "bybit"),
            ("btcusd", "coinbase"),
            ("btcusd", "kraken"),
        ];

        for (symbol, exchange) in &exchanges {
            let path = dir.path().join(format!("{}_oi.parquet", exchange));
            let records = make_oi_records(exchange);

            write_oi_parquet(&records, &path).unwrap();
            let loaded = read_oi_parquet(&path).unwrap();

            assert_eq!(loaded.len(), 2);
            assert_oi(&loaded[0], symbol, exchange, 32000.5, 752_000_000.0);
            assert_oi(&loaded[1], symbol, exchange, 32100.0, 754_350_000.0);
        }
    }

    // ── Timestamp ordering ───────────────────────────────────────────────

    #[test]
    fn test_timestamps_preserved_in_order() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ts_test.parquet");

        let pair = TradingPair::new("BTC", "USDT");
        let records = vec![
            OpenInterest {
                open_interest_ts_us: 1000,
                pair: pair.clone(),
                open_interest: 100.0,
                open_interest_value: 1_000_000.0,
                exchange: "bybit".to_string(),
            },
            OpenInterest {
                open_interest_ts_us: 2000,
                pair: pair.clone(),
                open_interest: 200.0,
                open_interest_value: 2_000_000.0,
                exchange: "bybit".to_string(),
            },
            OpenInterest {
                open_interest_ts_us: 3000,
                pair: pair.clone(),
                open_interest: 300.0,
                open_interest_value: 3_000_000.0,
                exchange: "bybit".to_string(),
            },
        ];

        write_oi_parquet(&records, &path).unwrap();
        let loaded = read_oi_parquet(&path).unwrap();

        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0].open_interest_ts_us, 1000);
        assert_eq!(loaded[1].open_interest_ts_us, 2000);
        assert_eq!(loaded[2].open_interest_ts_us, 3000);
    }

    // ── Timestamped writer — filename convention ─────────────────────────

    #[test]
    fn test_timestamped_writer_filename_and_mode() {
        let dir = tempdir().unwrap();
        let records = make_oi_records("bybit");

        // Test with "sync" mode
        let path = write_oi_parquet_timestamped(&records, dir.path(), "sync").unwrap();
        let fname = path.file_name().unwrap().to_str().unwrap();

        assert!(
            fname.starts_with("bybit_BTC-USDT_oi_sync_"),
            "expected filename starting with 'bybit_BTC-USDT_oi_sync_', got: {}",
            fname
        );
        assert!(fname.ends_with(".parquet"));

        // Roundtrip
        let loaded = read_oi_parquet(&path).unwrap();
        assert_eq!(loaded.len(), 2);

        // Test with "raw" mode
        let path_raw = write_oi_parquet_timestamped(&records, dir.path(), "raw").unwrap();
        let fname_raw = path_raw.file_name().unwrap().to_str().unwrap();
        assert!(
            fname_raw.starts_with("bybit_BTC-USDT_oi_raw_"),
            "expected filename starting with 'bybit_BTC-USDT_oi_raw_', got: {}",
            fname_raw
        );
    }
}
