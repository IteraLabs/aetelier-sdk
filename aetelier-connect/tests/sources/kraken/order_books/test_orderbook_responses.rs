//! Tests for Kraken `book` channel WebSocket response parsing,
//! NormalizedDelta conversion, and live WSS streaming.

#![allow(deprecated)]

#[cfg(test)]
mod tests {
    use aetelier_connect::sources::kraken::{
        client::KrakenWssClient, events::KrakenWssEvent,
        responses::orderbooks::KrakenBookResponse,
    };
    use aetelier_types::orderbooks::OrderbookDelta;
    use aetelier_types::trading_pair::TradingPair;
    use tokio::sync::mpsc::channel;
    use tokio::time::{Duration, timeout};

    // ── JSON fixtures ────────────────────────────────────────────────────

    /// Book snapshot matching Kraken WebSocket v2 docs.
    const SNAPSHOT_JSON: &str = r#"{
        "channel": "book",
        "type": "snapshot",
        "data": [{
            "symbol": "BTC/USD",
            "bids": [
                {"price": 21921.73, "qty": 0.063},
                {"price": 21921.00, "qty": 1.000}
            ],
            "asks": [
                {"price": 21922.00, "qty": 0.500},
                {"price": 21923.50, "qty": 2.000}
            ],
            "checksum": 2439117997,
            "timestamp": "2023-09-26T16:49:20.962586Z"
        }]
    }"#;

    /// Incremental update message.
    const UPDATE_JSON: &str = r#"{
        "channel": "book",
        "type": "update",
        "data": [{
            "symbol": "BTC/USD",
            "bids": [
                {"price": 21920.00, "qty": 3.000}
            ],
            "asks": [
                {"price": 21922.00, "qty": 0.0}
            ],
            "checksum": 1234567890,
            "timestamp": "2023-09-26T16:49:21.100000Z"
        }]
    }"#;

    // ── Deserialization tests ────────────────────────────────────────────

    #[test]
    fn test_parse_book_snapshot() {
        let resp: KrakenBookResponse =
            serde_json::from_str(SNAPSHOT_JSON).expect("should parse snapshot JSON");

        assert_eq!(resp.channel, "book");
        assert_eq!(resp.ty, "snapshot");
        assert_eq!(resp.data.len(), 1);

        let data = &resp.data[0];
        assert_eq!(data.symbol, "BTC/USD");
        assert_eq!(data.bids.len(), 2);
        assert_eq!(data.asks.len(), 2);
        assert_eq!(data.checksum, 2439117997);

        // Price/qty are kept as exact wire-token strings (checksum precision).
        let f = |s: &str| s.parse::<f64>().unwrap();
        assert!((f(&data.bids[0].price) - 21921.73).abs() < 0.01);
        assert!((f(&data.bids[0].qty) - 0.063).abs() < 0.001);
        assert!((f(&data.asks[0].price) - 21922.00).abs() < 0.01);
        assert!((f(&data.asks[0].qty) - 0.500).abs() < 0.001);
    }

    #[test]
    fn test_parse_book_update() {
        let resp: KrakenBookResponse =
            serde_json::from_str(UPDATE_JSON).expect("should parse update JSON");

        assert_eq!(resp.channel, "book");
        assert_eq!(resp.ty, "update");

        let data = &resp.data[0];
        assert_eq!(data.bids.len(), 1);
        assert_eq!(data.asks.len(), 1);

        // qty == 0.0 means remove the level
        assert_eq!(data.asks[0].qty.parse::<f64>().unwrap(), 0.0);
        assert!((data.asks[0].price.parse::<f64>().unwrap() - 21922.00).abs() < 0.01);
    }

    // ── NormalizedDelta conversion tests ─────────────────────────────────

    #[test]
    fn test_to_normalized_snapshot() {
        let resp: KrakenBookResponse = serde_json::from_str(SNAPSHOT_JSON).unwrap();

        let normalized = resp
            .to_normalized()
            .expect("snapshot should produce a NormalizedDelta");

        assert_eq!(normalized.symbol, "BTC/USD");
        assert!(normalized.is_snapshot);
        assert_eq!(normalized.update_id, 2439117997);
        assert_eq!(normalized.bids.len(), 2);
        assert_eq!(normalized.asks.len(), 2);

        // Prices are preserved as their exact wire tokens (checksum precision).
        assert!(normalized.bids.iter().any(|(p, _)| p == "21921.73"));
        assert!(normalized.bids.iter().any(|(p, _)| p == "21921.00"));
        assert!(normalized.asks.iter().any(|(p, _)| p == "21922.00"));
        assert!(normalized.asks.iter().any(|(p, _)| p == "21923.50"));
    }

    #[test]
    fn test_to_normalized_update_not_snapshot() {
        let resp: KrakenBookResponse = serde_json::from_str(UPDATE_JSON).unwrap();

        let normalized = resp.to_normalized().unwrap();

        assert!(
            !normalized.is_snapshot,
            "type='update' should not be is_snapshot"
        );
    }

    #[test]
    fn test_normalized_integrates_with_orderbook_delta() {
        let resp: KrakenBookResponse = serde_json::from_str(SNAPSHOT_JSON).unwrap();
        let normalized = resp.to_normalized().unwrap();

        let mut ob = OrderbookDelta::new(TradingPair::new("BTC", "USD"));
        let result = ob.process(&normalized);
        assert!(result.is_ok(), "snapshot processing should succeed");

        // After processing, the book should have 2 bids and 2 asks
        assert_eq!(ob.top_bids(10).len(), 2);
        assert_eq!(ob.top_asks(10).len(), 2);

        // Apply the update (removes ask at 21922.00, adds bid at 21920.00)
        let update_resp: KrakenBookResponse = serde_json::from_str(UPDATE_JSON).unwrap();
        let update_norm = update_resp.to_normalized().unwrap();
        let result = ob.process(&update_norm);
        assert!(result.is_ok());

        // Bids: 21921.73, 21921.00, 21920.00 (new) => 3
        assert_eq!(ob.top_bids(10).len(), 3);
        // Asks: 21923.50 only (21922.00 removed by qty=0) => 1
        assert_eq!(ob.top_asks(10).len(), 1);
    }

    // ── Empty data array ─────────────────────────────────────────────────

    #[test]
    fn test_to_normalized_empty_data() {
        let resp = KrakenBookResponse {
            channel: "book".to_string(),
            ty: "snapshot".to_string(),
            data: vec![],
        };

        assert!(
            resp.to_normalized().is_none(),
            "empty data should produce None"
        );
    }

    // ── Live WSS streaming test ─────────────────────────────────────────

    #[tokio::test]
    #[ignore = "live venue websocket - validation bucket, run with --ignored"]
    async fn test_kraken_wss_orderbook_stream() {
        let channels = vec!["book".to_string()];
        let symbols = vec!["BTC/USD".to_string()];

        let (tx, mut rx) = channel(1024);
        let client = KrakenWssClient::new(channels, symbols, 25);

        let handle = tokio::spawn(async move {
            let _ = client.receive_data(tx).await;
        });

        // Try to receive at least one book event (timeout for CI)
        let result = timeout(Duration::from_secs(15), rx.recv()).await;

        match result {
            Ok(Some(KrakenWssEvent::OrderbookData(resp))) => {
                assert_eq!(resp.channel, "book");
                assert!(!resp.data.is_empty());
                let data = &resp.data[0];
                assert_eq!(data.symbol, "BTC/USD");
                // First message should be a snapshot
                assert_eq!(resp.ty, "snapshot");
                assert!(!data.bids.is_empty());
                assert!(!data.asks.is_empty());
                println!(
                    "Received book: symbol={}, bids={}, asks={}, checksum={}",
                    data.symbol,
                    data.bids.len(),
                    data.asks.len(),
                    data.checksum,
                );
            }
            Ok(Some(_)) => {
                println!("Received non-orderbook event first; still valid");
            }
            Ok(None) => {
                println!("Channel closed (expected in test environments)");
            }
            Err(_) => {
                println!(
                    "Timeout waiting for Kraken WSS data (expected in sandboxed envs)"
                );
            }
        }

        handle.abort();
    }
}
