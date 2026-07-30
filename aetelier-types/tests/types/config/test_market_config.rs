#[cfg(test)]
mod tests {
    use aetelier_types::config::markets::market_config::{
        MarketSnapshotConfig, OrderbookConfig, SyncMode, TimeUnit,
    };
    use aetelier_types::synchronizers::ClockMode;

    // ─────────────────────────────────────────────────────────────────────────────
    // Test 1: Full TOML roundtrip
    // ─────────────────────────────────────────────────────────────────────────────

    #[test]
    fn test_full_toml_roundtrip() {
        let toml_str = r#"
[exchange]
name = "bybit"

[symbol]
name = "BTCUSDT"
sync_mode = "on_trade"

[update_frequency]
value = 100
unit = "Millis"

[pipeline]
flush_threshold = 36000

[datatypes.orderbook]
enabled = true
depth = 50

[datatypes.trades]
enabled = true

[datatypes.liquidations]
enabled = true

[datatypes.funding_rates]
enabled = true

[datatypes.open_interest]
enabled = true

[logs]
n_orderbooks = 100
n_trades = 10
n_liquidations = 1
n_fundings = 10
n_open_interests = 10

[output]
dir = "datasets/collected/bybit/market_snapshots"
"#;

        let config: MarketSnapshotConfig =
            toml::from_str(toml_str).expect("failed to deserialize complete TOML config");

        // Verify exchange
        assert_eq!(config.exchange.name, "bybit");

        // Verify symbol
        assert_eq!(config.symbol.name, "BTCUSDT");
        assert_eq!(config.symbol.sync_mode, SyncMode::OnTrade);

        // Verify update frequency
        assert_eq!(config.update_frequency.value, 100);
        assert_eq!(config.update_frequency.unit, TimeUnit::Millis);

        // Verify pipeline
        assert_eq!(config.pipeline.flush_threshold, 36000);

        // Verify datatypes
        assert!(config.datatypes.orderbook.enabled);
        assert_eq!(config.datatypes.orderbook.depth, 50);
        assert!(config.datatypes.trades.enabled);
        assert!(config.datatypes.liquidations.enabled);
        assert!(config.datatypes.funding_rates.enabled);
        assert!(config.datatypes.open_interest.enabled);

        // Verify logs
        assert_eq!(config.logs.n_orderbooks, 100);
        assert_eq!(config.logs.n_trades, 10);
        assert_eq!(config.logs.n_liquidations, 1);
        assert_eq!(config.logs.n_fundings, 10);
        assert_eq!(config.logs.n_open_interests, 10);

        // Verify output
        assert_eq!(
            config.output.dir,
            "datasets/collected/bybit/market_snapshots"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Test 2: period_us() conversion for all TimeUnit variants
    // ─────────────────────────────────────────────────────────────────────────────

    #[test]
    fn test_period_us_nanos() {
        let toml_str = r#"
[exchange]
name = "bybit"

[symbol]
name = "BTCUSDT"
sync_mode = "on_time"

[update_frequency]
value = 1000
unit = "Nanos"

[pipeline]
flush_threshold = 100

[output]
dir = "test"
"#;

        let config: MarketSnapshotConfig =
            toml::from_str(toml_str).expect("failed to deserialize");
        assert_eq!(config.period_us(), 1); // 1000 ns rounds down to 1 us
    }

    #[test]
    fn test_period_us_micros() {
        let toml_str = r#"
[exchange]
name = "bybit"

[symbol]
name = "BTCUSDT"
sync_mode = "on_time"

[update_frequency]
value = 100
unit = "Micros"

[pipeline]
flush_threshold = 100

[output]
dir = "test"
"#;

        let config: MarketSnapshotConfig =
            toml::from_str(toml_str).expect("failed to deserialize");
        assert_eq!(config.period_us(), 100); // 100 µs as-is
    }

    #[test]
    fn test_period_us_millis() {
        let toml_str = r#"
[exchange]
name = "bybit"

[symbol]
name = "BTCUSDT"
sync_mode = "on_time"

[update_frequency]
value = 100
unit = "Millis"

[pipeline]
flush_threshold = 100

[output]
dir = "test"
"#;

        let config: MarketSnapshotConfig =
            toml::from_str(toml_str).expect("failed to deserialize");
        assert_eq!(config.period_us(), 100_000); // 100 ms = 100_000 us
    }

    #[test]
    fn test_period_us_secs() {
        let toml_str = r#"
[exchange]
name = "bybit"

[symbol]
name = "BTCUSDT"
sync_mode = "on_time"

[update_frequency]
value = 1
unit = "Secs"

[pipeline]
flush_threshold = 100

[output]
dir = "test"
"#;

        let config: MarketSnapshotConfig =
            toml::from_str(toml_str).expect("failed to deserialize");
        assert_eq!(config.period_us(), 1_000_000); // 1 s = 1_000_000 us
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Test 3: flush_interval_us() with known values
    // ─────────────────────────────────────────────────────────────────────────────

    #[test]
    fn test_flush_interval_us_100ms_36000() {
        let toml_str = r#"
[exchange]
name = "bybit"

[symbol]
name = "BTCUSDT"
sync_mode = "on_time"

[update_frequency]
value = 100
unit = "Millis"

[pipeline]
flush_threshold = 36000

[output]
dir = "test"
"#;

        let config: MarketSnapshotConfig =
            toml::from_str(toml_str).expect("failed to deserialize");
        // 100 ms per period, 36000 periods = 3,600,000 ms = 1 hour = 3.6e12 ns
        assert_eq!(config.flush_interval_us(), 3_600_000_000);
    }

    #[test]
    fn test_flush_interval_us_1us_1000() {
        let toml_str = r#"
[exchange]
name = "bybit"

[symbol]
name = "BTCUSDT"
sync_mode = "on_time"

[update_frequency]
value = 1
unit = "Micros"

[pipeline]
flush_threshold = 1000

[output]
dir = "test"
"#;

        let config: MarketSnapshotConfig =
            toml::from_str(toml_str).expect("failed to deserialize");
        // 1 µs per period = 1000 ns, 1000 periods = 1_000_000 ns = 1 ms
        assert_eq!(config.flush_interval_us(), 1_000);
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Test 4: clock_mode() mapping for all SyncMode variants
    // ─────────────────────────────────────────────────────────────────────────────

    #[test]
    fn test_clock_mode_on_orderbook() {
        let toml_str = r#"
[exchange]
name = "bybit"

[symbol]
name = "BTCUSDT"
sync_mode = "on_orderbook"

[update_frequency]
value = 100
unit = "Millis"

[pipeline]
flush_threshold = 100

[output]
dir = "test"
"#;

        let config: MarketSnapshotConfig =
            toml::from_str(toml_str).expect("failed to deserialize");
        assert_eq!(config.clock_mode(), ClockMode::OrderbookDriven);
    }

    #[test]
    fn test_clock_mode_on_trade() {
        let toml_str = r#"
[exchange]
name = "bybit"

[symbol]
name = "BTCUSDT"
sync_mode = "on_trade"

[update_frequency]
value = 100
unit = "Millis"

[pipeline]
flush_threshold = 100

[output]
dir = "test"
"#;

        let config: MarketSnapshotConfig =
            toml::from_str(toml_str).expect("failed to deserialize");
        assert_eq!(config.clock_mode(), ClockMode::TradeDriven);
    }

    #[test]
    fn test_clock_mode_on_liquidation() {
        let toml_str = r#"
[exchange]
name = "bybit"

[symbol]
name = "BTCUSDT"
sync_mode = "on_liquidation"

[update_frequency]
value = 100
unit = "Millis"

[pipeline]
flush_threshold = 100

[output]
dir = "test"
"#;

        let config: MarketSnapshotConfig =
            toml::from_str(toml_str).expect("failed to deserialize");
        assert_eq!(config.clock_mode(), ClockMode::LiquidationDriven);
    }

    #[test]
    fn test_clock_mode_on_time() {
        let toml_str = r#"
[exchange]
name = "bybit"

[symbol]
name = "BTCUSDT"
sync_mode = "on_time"

[update_frequency]
value = 100
unit = "Millis"

[pipeline]
flush_threshold = 100

[output]
dir = "test"
"#;

        let config: MarketSnapshotConfig =
            toml::from_str(toml_str).expect("failed to deserialize");
        assert_eq!(config.clock_mode(), ClockMode::ExternalClock);
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Test 5: wss_streams() per exchange
    // ─────────────────────────────────────────────────────────────────────────────

    #[test]
    fn test_wss_streams_bybit_all_feeds() {
        let toml_str = r#"
[exchange]
name = "bybit"

[symbol]
name = "BTCUSDT"
sync_mode = "on_trade"

[update_frequency]
value = 100
unit = "Millis"

[pipeline]
flush_threshold = 100

[datatypes.orderbook]
enabled = true
depth = 50

[datatypes.trades]
enabled = true

[datatypes.liquidations]
enabled = true

[datatypes.funding_rates]
enabled = true

[datatypes.open_interest]
enabled = true

[output]
dir = "test"
"#;

        let config: MarketSnapshotConfig =
            toml::from_str(toml_str).expect("failed to deserialize");
        let streams = config.wss_streams();

        assert_eq!(streams.len(), 4); // orderbook, trades, liquidations, tickers
        assert!(streams.contains(&"orderbook.50.BTCUSDT".to_string()));
        assert!(streams.contains(&"publicTrade.BTCUSDT".to_string()));
        assert!(streams.contains(&"allLiquidation.BTCUSDT".to_string()));
        assert!(streams.contains(&"tickers.BTCUSDT".to_string()));
    }

    #[test]
    fn test_wss_streams_coinbase_ob_and_trades() {
        let toml_str = r#"
[exchange]
name = "coinbase"

[symbol]
name = "BTC-USD"
sync_mode = "on_trade"

[update_frequency]
value = 100
unit = "Millis"

[pipeline]
flush_threshold = 100

[datatypes.orderbook]
enabled = true
depth = 50

[datatypes.trades]
enabled = true

[datatypes.liquidations]
enabled = false

[datatypes.funding_rates]
enabled = false

[datatypes.open_interest]
enabled = false

[output]
dir = "test"
"#;

        let config: MarketSnapshotConfig =
            toml::from_str(toml_str).expect("failed to deserialize");
        let streams = config.wss_streams();

        assert_eq!(streams.len(), 2); // level2 and market_trades
        assert!(streams.contains(&"level2".to_string()));
        assert!(streams.contains(&"market_trades".to_string()));
    }

    #[test]
    fn test_wss_streams_kraken_ob_and_trades() {
        let toml_str = r#"
[exchange]
name = "kraken"

[symbol]
name = "XBT/USD"
sync_mode = "on_trade"

[update_frequency]
value = 100
unit = "Millis"

[pipeline]
flush_threshold = 100

[datatypes.orderbook]
enabled = true
depth = 25

[datatypes.trades]
enabled = true

[datatypes.liquidations]
enabled = false

[datatypes.funding_rates]
enabled = false

[datatypes.open_interest]
enabled = false

[output]
dir = "test"
"#;

        let config: MarketSnapshotConfig =
            toml::from_str(toml_str).expect("failed to deserialize");
        let streams = config.wss_streams();

        assert_eq!(streams.len(), 2); // book and trade
        assert!(streams.contains(&"book".to_string()));
        assert!(streams.contains(&"trade".to_string()));
    }

    #[test]
    fn test_wss_streams_binance_depth_and_trade() {
        let toml_str = r#"
[exchange]
name = "binance"

[symbol]
name = "BTCUSDT"
sync_mode = "on_trade"

[update_frequency]
value = 100
unit = "Millis"

[pipeline]
flush_threshold = 100

[datatypes.orderbook]
enabled = true
depth = 20

[datatypes.trades]
enabled = true

[datatypes.liquidations]
enabled = false

[datatypes.funding_rates]
enabled = false

[datatypes.open_interest]
enabled = false

[output]
dir = "test"
"#;

        let config: MarketSnapshotConfig =
            toml::from_str(toml_str).expect("failed to deserialize");
        let streams = config.wss_streams();

        assert_eq!(streams.len(), 2); // depth and trade
        assert!(streams.contains(&"btcusdt@depth@100ms".to_string()));
        assert!(streams.contains(&"btcusdt@trade".to_string()));
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Test 6: wss_streams() unknown exchange
    // ─────────────────────────────────────────────────────────────────────────────

    #[test]
    fn test_wss_streams_unknown_exchange() {
        let toml_str = r#"
[exchange]
name = "unknown_exchange"

[symbol]
name = "BTCUSDT"
sync_mode = "on_trade"

[update_frequency]
value = 100
unit = "Millis"

[pipeline]
flush_threshold = 100

[datatypes.orderbook]
enabled = true
depth = 50

[datatypes.trades]
enabled = true

[output]
dir = "test"
"#;

        let config: MarketSnapshotConfig =
            toml::from_str(toml_str).expect("failed to deserialize");
        let streams = config.wss_streams();

        assert!(streams.is_empty()); // Should return empty vec for unknown exchange
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Test 7: Default values for OrderbookConfig
    // ─────────────────────────────────────────────────────────────────────────────

    #[test]
    fn test_orderbook_config_default() {
        let ob_config = OrderbookConfig::default();

        assert!(!ob_config.enabled); // Should default to false
        assert_eq!(ob_config.depth, 50); // Should default to 50
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Test 8: Partial TOML with omitted feeds defaulting to disabled
    // ─────────────────────────────────────────────────────────────────────────────

    #[test]
    fn test_partial_toml_feeds_default_disabled() {
        let toml_str = r#"
[exchange]
name = "bybit"

[symbol]
name = "BTCUSDT"
sync_mode = "on_trade"

[update_frequency]
value = 100
unit = "Millis"

[pipeline]
flush_threshold = 100

[datatypes.orderbook]
enabled = true
depth = 25

[output]
dir = "test"
"#;

        let config: MarketSnapshotConfig =
            toml::from_str(toml_str).expect("failed to deserialize");

        // Only orderbook is explicitly set
        assert!(config.datatypes.orderbook.enabled);
        assert_eq!(config.datatypes.orderbook.depth, 25);

        // All other feeds should be disabled (default)
        assert!(!config.datatypes.trades.enabled);
        assert!(!config.datatypes.liquidations.enabled);
        assert!(!config.datatypes.funding_rates.enabled);
        assert!(!config.datatypes.open_interest.enabled);
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Test 9: SyncMode Display trait
    // ─────────────────────────────────────────────────────────────────────────────

    #[test]
    fn test_sync_mode_display() {
        assert_eq!(SyncMode::OnOrderbook.to_string(), "on_orderbook");
        assert_eq!(SyncMode::OnTrade.to_string(), "on_trade");
        assert_eq!(SyncMode::OnLiquidation.to_string(), "on_liquidation");
        assert_eq!(SyncMode::OnTime.to_string(), "on_time");
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Test 10: TimeUnit Display trait
    // ─────────────────────────────────────────────────────────────────────────────

    #[test]
    fn test_time_unit_display() {
        assert_eq!(TimeUnit::Nanos.to_string(), "ns");
        assert_eq!(TimeUnit::Micros.to_string(), "µs");
        assert_eq!(TimeUnit::Millis.to_string(), "ms");
        assert_eq!(TimeUnit::Secs.to_string(), "s");
    }
}
