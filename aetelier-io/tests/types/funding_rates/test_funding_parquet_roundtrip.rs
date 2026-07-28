//! Parquet round-trip tests for funding rate data.
//!
//! Each test constructs `FundingRate` structs with known fields, writes them
//! to Parquet via `write_funding_parquet`, reads back via
//! `read_funding_parquet`, and asserts field equality.
//!
//! These tests require the `parquet` feature:
//!   cargo test --features parquet -p aetelier_types test_funding_parquet_roundtrip

#[cfg(test)]
#[cfg(feature = "parquet")]
mod tests {
    use aetelier_io::funding::funding_parquet::{
        read_funding_parquet, write_funding_parquet, write_funding_parquet_timestamped,
    };
    use aetelier_types::funding::FundingRate;
    use aetelier_types::trading_pair::TradingPair;
    use tempfile::tempdir;

    // ── Helpers ──────────────────────────────────────────────────────────

    /// Build a pair of funding rate records for a given exchange/symbol.
    fn make_funding_rates(exchange: &str) -> Vec<FundingRate> {
        let pair = TradingPair::new("BTC", "USDT");
        vec![
            FundingRate {
                funding_rate_ts_us: 1672304484000,
                pair: pair.clone(),
                funding_rate: 0.0001,
                next_funding_ts_us: 1672308000000,
                exchange: exchange.to_string(),
            },
            FundingRate {
                funding_rate_ts_us: 1672308000000,
                pair: pair.clone(),
                funding_rate: -0.00015,
                next_funding_ts_us: 1672311600000,
                exchange: exchange.to_string(),
            },
        ]
    }

    /// Assert a loaded funding rate matches expected values.
    fn assert_funding(
        fr: &FundingRate,
        _symbol: &str,
        exchange: &str,
        rate: f64,
        next_ts: u64,
    ) {
        assert_eq!(fr.pair, TradingPair::new("BTC", "USDT"));
        assert_eq!(fr.exchange, exchange);
        assert!(
            (fr.funding_rate - rate).abs() < 1e-8,
            "expected rate ~{}, got {}",
            rate,
            fr.funding_rate,
        );
        assert_eq!(fr.next_funding_ts_us, next_ts);
        assert!(fr.funding_rate_ts_us > 0);
    }

    // ── Per-exchange round-trip tests ────────────────────────────────────

    #[test]
    fn test_bybit_funding_parquet_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bybit_funding.parquet");
        let rates = make_funding_rates("bybit");

        write_funding_parquet(&rates, &path).unwrap();
        let loaded = read_funding_parquet(&path).unwrap();

        assert_eq!(loaded.len(), 2);
        assert_funding(&loaded[0], "btcusdt", "bybit", 0.0001, 1672308000000);
        assert_funding(&loaded[1], "btcusdt", "bybit", -0.00015, 1672311600000);
    }

    #[test]
    fn test_multi_exchange_funding_parquet_roundtrip() {
        let dir = tempdir().unwrap();

        let exchanges = [
            ("btcusdt", "bybit"),
            ("btcusd", "coinbase"),
            ("btcusd", "kraken"),
        ];

        for (symbol, exchange) in &exchanges {
            let path = dir.path().join(format!("{}_funding.parquet", exchange));
            let rates = make_funding_rates(exchange);

            write_funding_parquet(&rates, &path).unwrap();
            let loaded = read_funding_parquet(&path).unwrap();

            assert_eq!(loaded.len(), 2);
            assert_funding(&loaded[0], symbol, exchange, 0.0001, 1672308000000);
            assert_funding(&loaded[1], symbol, exchange, -0.00015, 1672311600000);
        }
    }

    // ── Negative funding rate precision ──────────────────────────────────

    #[test]
    fn test_negative_funding_rate_preserved() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("neg_rate.parquet");

        let rates = vec![FundingRate {
            funding_rate_ts_us: 1672304484000,
            pair: TradingPair::new("BTC", "USDT"),
            funding_rate: -0.000375,
            next_funding_ts_us: 0,
            exchange: "bybit".to_string(),
        }];

        write_funding_parquet(&rates, &path).unwrap();
        let loaded = read_funding_parquet(&path).unwrap();

        assert_eq!(loaded.len(), 1);
        assert!((loaded[0].funding_rate - (-0.000375)).abs() < 1e-8);
    }

    // ── Timestamp ordering ───────────────────────────────────────────────

    #[test]
    fn test_timestamps_preserved_in_order() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ts_test.parquet");

        let pair = TradingPair::new("BTC", "USDT");
        let rates = vec![
            FundingRate {
                funding_rate_ts_us: 1000,
                pair: pair.clone(),
                funding_rate: 0.0001,
                next_funding_ts_us: 2000,
                exchange: "bybit".to_string(),
            },
            FundingRate {
                funding_rate_ts_us: 2000,
                pair: pair.clone(),
                funding_rate: 0.0002,
                next_funding_ts_us: 3000,
                exchange: "bybit".to_string(),
            },
            FundingRate {
                funding_rate_ts_us: 3000,
                pair: pair.clone(),
                funding_rate: 0.0003,
                next_funding_ts_us: 4000,
                exchange: "bybit".to_string(),
            },
        ];

        write_funding_parquet(&rates, &path).unwrap();
        let loaded = read_funding_parquet(&path).unwrap();

        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0].funding_rate_ts_us, 1000);
        assert_eq!(loaded[1].funding_rate_ts_us, 2000);
        assert_eq!(loaded[2].funding_rate_ts_us, 3000);
    }

    // ── Timestamped writer — filename convention ─────────────────────────

    #[test]
    fn test_timestamped_writer_filename_and_mode() {
        let dir = tempdir().unwrap();
        let rates = make_funding_rates("bybit");

        // Test with "sync" mode
        let path = write_funding_parquet_timestamped(&rates, dir.path(), "sync").unwrap();
        let fname = path.file_name().unwrap().to_str().unwrap();

        assert!(
            fname.starts_with("bybit_BTC-USDT_funding_sync_"),
            "expected filename starting with 'bybit_BTC-USDT_funding_sync_', got: {}",
            fname
        );
        assert!(fname.ends_with(".parquet"));

        // Roundtrip
        let loaded = read_funding_parquet(&path).unwrap();
        assert_eq!(loaded.len(), 2);

        // Test with "raw" mode
        let path_raw =
            write_funding_parquet_timestamped(&rates, dir.path(), "raw").unwrap();
        let fname_raw = path_raw.file_name().unwrap().to_str().unwrap();
        assert!(
            fname_raw.starts_with("bybit_BTC-USDT_funding_raw_"),
            "expected filename starting with 'bybit_BTC-USDT_funding_raw_', got: {}",
            fname_raw
        );
    }
}
