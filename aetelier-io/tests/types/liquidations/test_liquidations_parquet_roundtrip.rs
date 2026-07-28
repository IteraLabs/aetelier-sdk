//! Parquet round-trip tests for liquidation data.
//!
//! Each test constructs `Liquidation` structs with known fields, writes them
//! to Parquet via `write_liquidations_parquet`, reads back via
//! `read_liquidations_parquet`, and asserts field equality.
//!
//! These tests require the `parquet` feature:
//!   cargo test --features parquet -p aetelier_types test_liquidations_parquet_roundtrip

#[cfg(test)]
#[cfg(feature = "parquet")]
mod tests {
    use aetelier_io::liquidations::liq_parquet::{
        read_liquidations_parquet, write_liquidations_parquet,
        write_liquidations_parquet_timestamped,
    };
    use aetelier_types::TradeSide;
    use aetelier_types::liquidations::Liquidation;
    use aetelier_types::orderbooks::{decimal_to_f64, f64_to_decimal};
    use aetelier_types::trading_pair::TradingPair;
    use tempfile::tempdir;

    // ── Helpers ──────────────────────────────────────────────────────────

    /// Build a pair of liquidations (buy + sell) for a given exchange/symbol.
    fn make_liquidations(exchange: &str) -> Vec<Liquidation> {
        let pair = TradingPair::new("BTC", "USDT");
        vec![
            Liquidation {
                liquidation_ts_us: 1672304484932,
                pair: pair.clone(),
                side: TradeSide::Buy,
                amount: f64_to_decimal(0.125),
                price: f64_to_decimal(23500.00),
                exchange: exchange.to_string(),
            },
            Liquidation {
                liquidation_ts_us: 1672304484900,
                pair: pair.clone(),
                side: TradeSide::Sell,
                amount: f64_to_decimal(1.500),
                price: f64_to_decimal(23480.50),
                exchange: exchange.to_string(),
            },
        ]
    }

    /// Assert a loaded liquidation matches expected values.
    fn assert_liq(
        liq: &Liquidation,
        _symbol: &str,
        exchange: &str,
        side: TradeSide,
        price: f64,
    ) {
        assert_eq!(liq.pair, TradingPair::new("BTC", "USDT"));
        assert_eq!(liq.exchange, exchange);
        assert_eq!(liq.side, side);
        assert!((decimal_to_f64(liq.price) - price).abs() < 0.01);
        assert!(liq.liquidation_ts_us > 0);
    }

    // ── Per-exchange round-trip tests ────────────────────────────────────

    #[test]
    fn test_bybit_liquidations_parquet_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bybit_liquidations.parquet");
        let liqs = make_liquidations("bybit");

        write_liquidations_parquet(&liqs, &path).unwrap();
        let loaded = read_liquidations_parquet(&path).unwrap();

        assert_eq!(loaded.len(), 2);
        assert_liq(&loaded[0], "btcusdt", "bybit", TradeSide::Buy, 23500.00);
        assert_liq(&loaded[1], "btcusdt", "bybit", TradeSide::Sell, 23480.50);

        // Verify amounts
        assert!((decimal_to_f64(loaded[0].amount) - 0.125).abs() < 0.001);
        assert!((decimal_to_f64(loaded[1].amount) - 1.500).abs() < 0.001);
    }

    #[test]
    fn test_multi_exchange_liquidations_parquet_roundtrip() {
        let dir = tempdir().unwrap();

        let exchanges = [
            ("btcusdt", "bybit"),
            ("btcusd", "coinbase"),
            ("btcusd", "kraken"),
        ];

        for (symbol, exchange) in &exchanges {
            let path = dir
                .path()
                .join(format!("{}_liquidations.parquet", exchange));
            let liqs = make_liquidations(exchange);

            write_liquidations_parquet(&liqs, &path).unwrap();
            let loaded = read_liquidations_parquet(&path).unwrap();

            assert_eq!(loaded.len(), 2);
            assert_liq(&loaded[0], symbol, exchange, TradeSide::Buy, 23500.00);
            assert_liq(&loaded[1], symbol, exchange, TradeSide::Sell, 23480.50);
        }
    }

    // ── Timestamp ordering ───────────────────────────────────────────────

    #[test]
    fn test_timestamps_preserved_in_order() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ts_test.parquet");

        let pair = TradingPair::new("BTC", "USDT");
        let liqs = vec![
            Liquidation {
                liquidation_ts_us: 1000,
                pair: pair.clone(),
                side: TradeSide::Buy,
                amount: f64_to_decimal(0.1),
                price: f64_to_decimal(100.0),
                exchange: "bybit".to_string(),
            },
            Liquidation {
                liquidation_ts_us: 2000,
                pair: pair.clone(),
                side: TradeSide::Sell,
                amount: f64_to_decimal(0.2),
                price: f64_to_decimal(200.0),
                exchange: "bybit".to_string(),
            },
            Liquidation {
                liquidation_ts_us: 3000,
                pair: pair.clone(),
                side: TradeSide::Buy,
                amount: f64_to_decimal(0.3),
                price: f64_to_decimal(300.0),
                exchange: "bybit".to_string(),
            },
        ];

        write_liquidations_parquet(&liqs, &path).unwrap();
        let loaded = read_liquidations_parquet(&path).unwrap();

        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0].liquidation_ts_us, 1000);
        assert_eq!(loaded[1].liquidation_ts_us, 2000);
        assert_eq!(loaded[2].liquidation_ts_us, 3000);
    }

    // ── Timestamped writer — filename convention ─────────────────────────

    #[test]
    fn test_timestamped_writer_filename_and_mode() {
        let dir = tempdir().unwrap();
        let liqs = make_liquidations("bybit");

        // Test with "sync" mode
        let path =
            write_liquidations_parquet_timestamped(&liqs, dir.path(), "sync").unwrap();
        let fname = path.file_name().unwrap().to_str().unwrap();

        assert!(
            fname.starts_with("bybit_BTC-USDT_liquidations_sync_"),
            "expected filename starting with 'bybit_BTC-USDT_liquidations_sync_', got: {}",
            fname
        );
        assert!(fname.ends_with(".parquet"));

        // Roundtrip
        let loaded = read_liquidations_parquet(&path).unwrap();
        assert_eq!(loaded.len(), 2);

        // Test with "raw" mode
        let path_raw =
            write_liquidations_parquet_timestamped(&liqs, dir.path(), "raw").unwrap();
        let fname_raw = path_raw.file_name().unwrap().to_str().unwrap();
        assert!(
            fname_raw.starts_with("bybit_BTC-USDT_liquidations_raw_"),
            "expected filename starting with 'bybit_BTC-USDT_liquidations_raw_', got: {}",
            fname_raw
        );
    }
}
