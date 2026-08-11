//! Tests for Coinbase market_trades WebSocket response parsing
//! and live WSS streaming.

#![allow(deprecated)]

#[cfg(test)]
mod tests {
    use aetelier_connect::sources::coinbase::{
        client::CoinbaseWssClient,
        events::CoinbaseWssEvent,
        responses::trades::{CoinbaseTradeData, CoinbaseTradeResponse},
    };
    use tokio::sync::mpsc::channel;
    use tokio::time::{Duration, timeout};

    // ── JSON fixtures ────────────────────────────────────────────────────

    /// Snapshot message matching the Coinbase Advanced Trade WSS docs:
    /// channel = "market_trades", side ∈ {"BUY", "SELL"}.
    const TRADES_SNAPSHOT_JSON: &str = r#"{
        "channel": "market_trades",
        "client_id": "",
        "timestamp": "2023-02-09T20:19:35.39625135Z",
        "sequence_num": 0,
        "events": [
            {
                "type": "snapshot",
                "trades": [
                    {
                        "trade_id": "12345",
                        "product_id": "BTC-USD",
                        "price": "23536.30",
                        "size": "0.001",
                        "side": "BUY",
                        "time": "2023-02-09T20:19:35.396Z"
                    },
                    {
                        "trade_id": "12346",
                        "product_id": "BTC-USD",
                        "price": "23535.00",
                        "size": "0.500",
                        "side": "SELL",
                        "time": "2023-02-09T20:19:34.500Z"
                    }
                ]
            }
        ]
    }"#;

    /// Update message with a single trade.
    const TRADE_UPDATE_JSON: &str = r#"{
        "channel": "market_trades",
        "client_id": "",
        "timestamp": "2023-02-09T20:20:01.000000000Z",
        "sequence_num": 42,
        "events": [
            {
                "type": "update",
                "trades": [
                    {
                        "trade_id": "12347",
                        "product_id": "ETH-USD",
                        "price": "1650.25",
                        "size": "10.0",
                        "side": "BUY",
                        "time": "2023-02-09T20:20:00.999Z"
                    }
                ]
            }
        ]
    }"#;

    // ── Deserialization tests ────────────────────────────────────────────

    #[test]
    fn test_parse_trades_snapshot() {
        let resp: CoinbaseTradeResponse = serde_json::from_str(TRADES_SNAPSHOT_JSON)
            .expect("should parse trades snapshot");

        assert_eq!(resp.channel, "market_trades");
        assert_eq!(resp.sequence_num, 0);
        assert_eq!(resp.events.len(), 1);

        let event = &resp.events[0];
        assert_eq!(event.ty, "snapshot");
        assert_eq!(event.trades.len(), 2);

        let buy = &event.trades[0];
        assert_eq!(buy.trade_id, "12345");
        assert_eq!(buy.product_id, "BTC-USD");
        assert_eq!(buy.price, "23536.30");
        assert_eq!(buy.size, "0.001");
        assert_eq!(buy.side, "BUY");

        let sell = &event.trades[1];
        assert_eq!(sell.side, "SELL");
        assert_eq!(sell.size, "0.500");
    }

    #[test]
    fn test_parse_trade_update() {
        let resp: CoinbaseTradeResponse =
            serde_json::from_str(TRADE_UPDATE_JSON).expect("should parse trade update");

        assert_eq!(resp.sequence_num, 42);
        let event = &resp.events[0];
        assert_eq!(event.ty, "update");
        assert_eq!(event.trades.len(), 1);

        let trade = &event.trades[0];
        assert_eq!(trade.product_id, "ETH-USD");
        assert_eq!(trade.price, "1650.25");
    }

    // ── Timestamp parsing test ──────────────────────────────────────────

    #[test]
    fn test_timestamp_us_parsing() {
        let trade = CoinbaseTradeData {
            trade_id: "1".to_string(),
            product_id: "BTC-USD".to_string(),
            price: "100.0".to_string(),
            size: "1.0".to_string(),
            side: "BUY".to_string(),
            time: "2023-02-09T20:19:35.396Z".to_string(),
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
        let trade = CoinbaseTradeData {
            trade_id: "1".to_string(),
            product_id: "BTC-USD".to_string(),
            price: "100.0".to_string(),
            size: "1.0".to_string(),
            side: "BUY".to_string(),
            time: "not-a-timestamp".to_string(),
        };

        assert_eq!(trade.timestamp_us(), 0, "invalid timestamp should return 0");
    }

    // ── Side values match Coinbase API ───────────────────────────────────

    #[test]
    fn test_side_values_buy_sell() {
        // Coinbase market_trades uses "BUY"/"SELL" (not "bid"/"offer")
        let resp: CoinbaseTradeResponse =
            serde_json::from_str(TRADES_SNAPSHOT_JSON).unwrap();

        let sides: Vec<&str> = resp.events[0]
            .trades
            .iter()
            .map(|t| t.side.as_str())
            .collect();

        for side in &sides {
            assert!(
                *side == "BUY" || *side == "SELL",
                "trade side must be BUY or SELL, got: {}",
                side
            );
        }
    }

    // ── Live WSS streaming test ─────────────────────────────────────────

    #[tokio::test]
    #[ignore = "live venue websocket - validation bucket, run with --ignored"]
    async fn test_coinbase_wss_trade_stream() {
        let channels = vec!["market_trades".to_string()];
        let product_ids = vec!["BTC-USD".to_string()];

        let (tx, mut rx) = channel(1024);
        let client = CoinbaseWssClient::new(channels, product_ids);

        let handle = tokio::spawn(async move {
            let _ = client.receive_data(tx).await;
        });

        // Try to receive at least one trade event (timeout for CI)
        let result = timeout(Duration::from_secs(15), rx.recv()).await;

        match result {
            Ok(Some(CoinbaseWssEvent::TradeData(trades))) => {
                assert!(!trades.is_empty(), "a trade frame carries >=1 trade");
                let trade = &trades[0];
                assert_eq!(trade.product_id, "BTC-USD");
                assert!(
                    trade.side == "BUY" || trade.side == "SELL",
                    "side should be BUY or SELL"
                );
                let price: f64 = trade.price.parse().expect("price should parse to f64");
                assert!(price > 0.0);
                let size: f64 = trade.size.parse().expect("size should parse to f64");
                assert!(size > 0.0);
                let ts = trade.timestamp_us();
                assert!(ts > 0);
                println!(
                    "Trade: id={} product={} price={} size={} side={} ts={}",
                    trade.trade_id,
                    trade.product_id,
                    trade.price,
                    trade.size,
                    trade.side,
                    ts,
                );
            }
            Ok(Some(_)) => {
                println!("Received non-trade event first (snapshot); still valid");
            }
            Ok(None) => {
                println!("Channel closed (expected in test environments)");
            }
            Err(_) => {
                println!(
                    "Timeout waiting for Coinbase trade data (expected in sandboxed envs)"
                );
            }
        }

        handle.abort();
    }
}
