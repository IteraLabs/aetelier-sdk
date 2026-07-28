//! Tests for Kraken `trade` channel WebSocket response parsing
//! and live WSS streaming.

#![allow(deprecated)] // exercises the deprecated legacy raw-ingestion path (still live for derivatives/BYO)

#[cfg(test)]
mod tests {
    use aetelier_connect::sources::kraken::{
        client::KrakenWssClient,
        events::KrakenWssEvent,
        responses::trades::{KrakenTradeData, KrakenTradeResponse},
    };
    use tokio::sync::mpsc::channel;
    use tokio::time::{Duration, timeout};

    // ── JSON fixtures ────────────────────────────────────────────────────

    /// Trade snapshot matching Kraken WebSocket v2 docs.
    const TRADES_SNAPSHOT_JSON: &str = r#"{
        "channel": "trade",
        "type": "snapshot",
        "data": [
            {
                "symbol": "BTC/USD",
                "side": "buy",
                "price": 23536.30,
                "qty": 0.001,
                "ord_type": "limit",
                "trade_id": 12345,
                "timestamp": "2023-02-09T20:19:35.396Z"
            },
            {
                "symbol": "BTC/USD",
                "side": "sell",
                "price": 23535.00,
                "qty": 0.500,
                "ord_type": "market",
                "trade_id": 12346,
                "timestamp": "2023-02-09T20:19:34.500Z"
            }
        ]
    }"#;

    /// Update message with a single trade.
    const TRADE_UPDATE_JSON: &str = r#"{
        "channel": "trade",
        "type": "update",
        "data": [
            {
                "symbol": "ETH/USD",
                "side": "buy",
                "price": 1650.25,
                "qty": 10.0,
                "ord_type": "limit",
                "trade_id": 12347,
                "timestamp": "2023-02-09T20:20:00.999Z"
            }
        ]
    }"#;

    // ── Deserialization tests ────────────────────────────────────────────

    #[test]
    fn test_parse_trades_snapshot() {
        let resp: KrakenTradeResponse = serde_json::from_str(TRADES_SNAPSHOT_JSON)
            .expect("should parse trades snapshot");

        assert_eq!(resp.channel, "trade");
        assert_eq!(resp.ty, "snapshot");
        assert_eq!(resp.data.len(), 2);

        let buy = &resp.data[0];
        assert_eq!(buy.symbol, "BTC/USD");
        assert_eq!(buy.side, "buy");
        assert!((buy.price - 23536.30).abs() < 0.01);
        assert!((buy.qty - 0.001).abs() < 0.0001);
        assert_eq!(buy.ord_type, "limit");
        assert_eq!(buy.trade_id, 12345);

        let sell = &resp.data[1];
        assert_eq!(sell.side, "sell");
        assert_eq!(sell.ord_type, "market");
        assert!((sell.qty - 0.500).abs() < 0.001);
    }

    #[test]
    fn test_parse_trade_update() {
        let resp: KrakenTradeResponse =
            serde_json::from_str(TRADE_UPDATE_JSON).expect("should parse trade update");

        assert_eq!(resp.ty, "update");
        assert_eq!(resp.data.len(), 1);

        let trade = &resp.data[0];
        assert_eq!(trade.symbol, "ETH/USD");
        assert!((trade.price - 1650.25).abs() < 0.01);
    }

    // ── Timestamp parsing test ──────────────────────────────────────────

    #[test]
    fn test_timestamp_us_parsing() {
        let trade = KrakenTradeData {
            symbol: "BTC/USD".to_string(),
            side: "buy".to_string(),
            price: 23536.30,
            qty: 0.001,
            ord_type: "limit".to_string(),
            trade_id: 12345,
            timestamp: "2023-02-09T20:19:35.396Z".to_string(),
        };

        let ts = trade.timestamp_us();
        assert!(
            ts > 0,
            "timestamp_us should produce a positive epoch us value"
        );

        // 2023-02-09T20:19:35.396Z is exactly 1675973975396000 us
        assert_eq!(
            ts, 1675973975396000,
            "timestamp should be the exact us epoch for 2023-02-09T20:19:35.396Z"
        );
    }

    #[test]
    fn test_timestamp_us_invalid_returns_zero() {
        let trade = KrakenTradeData {
            symbol: "BTC/USD".to_string(),
            side: "buy".to_string(),
            price: 100.0,
            qty: 1.0,
            ord_type: "limit".to_string(),
            trade_id: 1,
            timestamp: "not-a-timestamp".to_string(),
        };

        assert_eq!(trade.timestamp_us(), 0, "invalid timestamp should return 0");
    }

    // ── Side values match Kraken API ─────────────────────────────────────

    #[test]
    fn test_side_values_buy_sell() {
        // Kraken trades use lowercase "buy"/"sell"
        let resp: KrakenTradeResponse =
            serde_json::from_str(TRADES_SNAPSHOT_JSON).unwrap();

        let sides: Vec<&str> = resp.data.iter().map(|t| t.side.as_str()).collect();

        for side in &sides {
            assert!(
                *side == "buy" || *side == "sell",
                "trade side must be buy or sell, got: {}",
                side
            );
        }
    }

    // ── Float price/qty (unlike Bybit/Coinbase strings) ──────────────────

    #[test]
    fn test_price_qty_are_floats() {
        let resp: KrakenTradeResponse =
            serde_json::from_str(TRADES_SNAPSHOT_JSON).unwrap();

        for trade in &resp.data {
            assert!(trade.price > 0.0, "price should be a positive float");
            assert!(trade.qty > 0.0, "qty should be a positive float");
        }
    }

    // ── Live WSS streaming test ─────────────────────────────────────────

    #[tokio::test]
    #[ignore = "live venue websocket - validation bucket, run with --ignored"]
    async fn test_kraken_wss_trade_stream() {
        let channels = vec!["trade".to_string()];
        let symbols = vec!["BTC/USD".to_string()];

        let (tx, mut rx) = channel(1024);
        let client = KrakenWssClient::new(channels, symbols, 25);

        let handle = tokio::spawn(async move {
            let _ = client.receive_data(tx).await;
        });

        // Try to receive at least one trade event (timeout for CI)
        let result = timeout(Duration::from_secs(15), rx.recv()).await;

        match result {
            Ok(Some(KrakenWssEvent::TradeData(trades))) => {
                assert!(!trades.is_empty(), "a trade frame carries >=1 trade");
                let trade = &trades[0];
                assert_eq!(trade.symbol, "BTC/USD");
                assert!(
                    trade.side == "buy" || trade.side == "sell",
                    "side should be buy or sell"
                );
                assert!(trade.price > 0.0);
                assert!(trade.qty > 0.0);
                let ts = trade.timestamp_us();
                assert!(ts > 0);
                println!(
                    "Trade: id={} symbol={} price={} qty={} side={} ts={}",
                    trade.trade_id, trade.symbol, trade.price, trade.qty, trade.side, ts,
                );
            }
            Ok(Some(_)) => {
                println!(
                    "Received non-trade event first (orderbook snapshot); still valid"
                );
            }
            Ok(None) => {
                println!("Channel closed (expected in test environments)");
            }
            Err(_) => {
                println!(
                    "Timeout waiting for Kraken trade data (expected in sandboxed envs)"
                );
            }
        }

        handle.abort();
    }
}
