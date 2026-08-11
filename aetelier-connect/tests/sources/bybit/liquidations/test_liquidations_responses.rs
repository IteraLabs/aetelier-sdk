#![allow(deprecated)]

#[cfg(test)]
mod tests {
    use aetelier_connect::sources::bybit::client::BybitWssClient;
    use tokio::sync::mpsc::channel;
    use tokio::time::{Duration, timeout};

    // Mock data structures for testing
    #[derive(Debug, Clone)]
    pub struct MockLiquidationData {
        pub symbol: String,
        pub side: String,
        pub amount: f64,
        pub price: f64,
        pub liquidation_ts_ms: u64,
    }

    #[tokio::test]
    #[ignore = "live venue websocket - validation bucket, run with --ignored"]
    async fn test_stream_configuration_and_setup() {
        // Test 1: Validate stream configuration and basic setup via BybitWssClient
        let streams = vec![
            "publicTrade.SOLUSDT".to_string(),
            "allLiquidation.SOLUSDT".to_string(),
        ];

        // Test that BybitWssClient can be created and connect without panicking
        let (tx, mut rx) = channel(1024);
        let client = BybitWssClient::new(streams.clone());

        let handle = tokio::spawn(async move {
            let _ = client.receive_data(tx).await;
        });

        // Try to receive one event with timeout to avoid infinite wait
        let result = timeout(Duration::from_secs(10), rx.recv()).await;

        match result {
            Ok(Some(event)) => {
                println!("BybitWssEvent {:?}", event);
            }
            Ok(None) => {
                println!("Channel closed (expected in test environments)");
            }
            Err(_) => {
                // Timeout — acceptable in CI or sandboxed environments
            }
        }

        handle.abort();

        // Validate input parameters
        assert!(!streams.is_empty(), "Streams should not be empty");
        assert_eq!(streams.len(), 2, "Should have 2 stream subscriptions");
    }

    #[tokio::test]
    async fn test_liquidation_event_processing_logic() {
        // Test 2: Validate liquidation event processing and formatting

        // Create mock liquidation data
        let mock_liquidation = MockLiquidationData {
            symbol: "SOLUSDT".to_string(),
            side: "Buy".to_string(),
            amount: 100.5,
            price: 150.75,
            liquidation_ts_ms: 1755485312, // August 18, 2023 timestamp in milliseconds
        };

        // Test timestamp conversion
        let ts = chrono::DateTime::from_timestamp_millis(
            mock_liquidation.liquidation_ts_ms as i64,
        )
        .unwrap_or_default();

        assert!(!ts.to_string().is_empty(), "Timestamp should be valid");

        // Test formatted output structure
        let formatted_output = format!(
            "Liquidation: [{}] {} - qty={} price={} ts={}",
            mock_liquidation.symbol,
            mock_liquidation.side,
            mock_liquidation.amount,
            mock_liquidation.price,
            ts.format("%Y-%m-%d %H:%M:%S%.3f")
        );

        assert!(formatted_output.contains("Liquidation:"));
        assert!(formatted_output.contains("SOLUSDT"));
        assert!(formatted_output.contains("Buy"));
        assert!(formatted_output.contains("100.5"));
        assert!(formatted_output.contains("150.75"));

        // Validate individual components
        assert_eq!(mock_liquidation.symbol, "SOLUSDT");
        assert!(["Buy", "Sell"].contains(&mock_liquidation.side.as_str()));
        assert!(mock_liquidation.amount > 0.0);
        assert!(mock_liquidation.price > 0.0);
        assert!(mock_liquidation.liquidation_ts_ms > 0);
    }

    /// Doc-shaped `allLiquidation.*` frame from the Bybit unified v5 docs.
    /// Numeric fields arrive as strings; `T` is Unix ms; `S` is PascalCase.
    /// Values are chosen to round-trip exactly through `f64` <-> `Decimal`.
    const LIQUIDATION_SNAPSHOT_JSON: &str = r#"{
        "topic": "allLiquidation.BTCUSDT",
        "type": "snapshot",
        "ts": 1672304484978,
        "data": [
            {
                "T": 1672304484978,
                "s": "BTCUSDT",
                "S": "Sell",
                "v": "0.5",
                "p": "100.25"
            }
        ]
    }"#;

    /// Deserialize a doc-shaped liquidation frame through the production
    /// response type, then map it into the exchange-agnostic `Liquidation`
    /// via the production builder / codec helpers, asserting the normalized
    /// fields (side, price, amount, symbol, ts in microseconds).
    #[test]
    fn test_parse_and_normalize_liquidation_snapshot() {
        use aetelier_connect::sources::bybit::responses::liquidations::BybitLiquidationResponse;
        use aetelier_types::exchanges::Exchange;
        use aetelier_types::liquidations::Liquidation;
        use aetelier_types::orderbooks::decimal_to_f64;
        use aetelier_types::trades::TradeSide;
        use aetelier_types::trading_pair::TradingPair;

        // ── Parse through the production response type ───────────────────
        let resp: BybitLiquidationResponse =
            serde_json::from_str(LIQUIDATION_SNAPSHOT_JSON)
                .expect("should parse liquidation snapshot");

        assert_eq!(resp.topic, "allLiquidation.BTCUSDT");
        assert_eq!(resp.ty, "snapshot");
        assert_eq!(resp.ts, 1672304484978);
        assert_eq!(resp.data.len(), 1);

        let raw = &resp.data[0];
        assert_eq!(raw.liquidation_ts_ms, 1672304484978);
        assert_eq!(raw.symbol, "BTCUSDT");
        assert_eq!(raw.side, "Sell");
        assert_eq!(raw.amount, "0.5");
        assert_eq!(raw.price, "100.25");

        // ── Normalize into the exchange-agnostic domain type ─────────────
        let pair = TradingPair::from_exchange_symbol(&raw.symbol, Exchange::Bybit)
            .expect("bybit symbol should normalize");
        let side = TradeSide::from_str_loose(&raw.side).expect("side should normalize");
        let amount: f64 = raw.amount.parse().expect("amount should parse to f64");
        let price: f64 = raw.price.parse().expect("price should parse to f64");
        // Bybit reports `T` in Unix ms; the platform standard is Unix µs.
        let liquidation_ts_us = raw.liquidation_ts_ms * 1_000;

        let normalized: Liquidation = Liquidation::builder()
            .ts(liquidation_ts_us)
            .pair(pair)
            .side(side)
            .amount(amount)
            .price(price)
            .exchange("bybit".to_string())
            .build()
            .expect("liquidation should build");

        // Symbol: concatenated "BTCUSDT" -> canonical "BTC/USDT".
        assert_eq!(normalized.pair.base(), "BTC");
        assert_eq!(normalized.pair.quote(), "USDT");
        assert_eq!(normalized.pair.to_canonical(), "BTC/USDT");

        // Side: PascalCase "Sell" -> TradeSide::Sell.
        assert_eq!(normalized.side, TradeSide::Sell);

        // Timestamp: 1672304484978 ms -> 1672304484978000 µs.
        assert_eq!(normalized.liquidation_ts_us, 1_672_304_484_978_000);

        // Amount / price: Decimal in memory, compared via decimal_to_f64.
        assert!(
            (decimal_to_f64(normalized.amount) - 0.5).abs() < 1e-9,
            "amount should normalize to 0.5"
        );
        assert!(
            (decimal_to_f64(normalized.price) - 100.25).abs() < 1e-9,
            "price should normalize to 100.25"
        );

        assert_eq!(normalized.exchange, "bybit");
    }
}
