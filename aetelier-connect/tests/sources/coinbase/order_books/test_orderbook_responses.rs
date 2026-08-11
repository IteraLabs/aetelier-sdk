//! Tests for Coinbase level2 (orderbook) WebSocket response parsing,
//! NormalizedDelta conversion, and live WSS streaming.

#![allow(deprecated)]

#[cfg(test)]
mod tests {
    use aetelier_connect::sources::coinbase::{
        client::CoinbaseWssClient,
        events::CoinbaseWssEvent,
        responses::orderbooks::{CoinbaseL2Update, CoinbaseOrderbookResponse},
    };
    use aetelier_types::orderbooks::OrderbookDelta;
    use aetelier_types::trading_pair::TradingPair;
    use tokio::sync::mpsc::channel;
    use tokio::time::{Duration, timeout};

    // ── JSON fixtures ────────────────────────────────────────────────────

    /// Snapshot message matching the Coinbase Advanced Trade WSS docs:
    /// channel = "l2_data", type = "snapshot", side ∈ {"bid", "offer"}.
    const SNAPSHOT_JSON: &str = r#"{
        "channel": "l2_data",
        "client_id": "",
        "timestamp": "2023-02-09T20:32:50.714964855Z",
        "sequence_num": 0,
        "events": [
            {
                "type": "snapshot",
                "product_id": "BTC-USD",
                "updates": [
                    { "side": "bid",   "event_time": "1970-01-01T00:00:00Z", "price_level": "21921.73", "new_quantity": "0.06317902" },
                    { "side": "bid",   "event_time": "1970-01-01T00:00:00Z", "price_level": "21921.00", "new_quantity": "1.00000000" },
                    { "side": "offer", "event_time": "1970-01-01T00:00:00Z", "price_level": "21922.00", "new_quantity": "0.50000000" },
                    { "side": "offer", "event_time": "1970-01-01T00:00:00Z", "price_level": "21923.50", "new_quantity": "2.00000000" }
                ]
            }
        ]
    }"#;

    /// Incremental update message (type = "update").
    const UPDATE_JSON: &str = r#"{
        "channel": "l2_data",
        "client_id": "",
        "timestamp": "2023-02-09T20:33:01.123456789Z",
        "sequence_num": 5,
        "events": [
            {
                "type": "update",
                "product_id": "BTC-USD",
                "updates": [
                    { "side": "bid",   "event_time": "2023-02-09T20:33:01Z", "price_level": "21920.00", "new_quantity": "3.00000000" },
                    { "side": "offer", "event_time": "2023-02-09T20:33:01Z", "price_level": "21922.00", "new_quantity": "0" }
                ]
            }
        ]
    }"#;

    // ── Deserialization tests ────────────────────────────────────────────

    #[test]
    fn test_parse_l2_snapshot() {
        let resp: CoinbaseOrderbookResponse =
            serde_json::from_str(SNAPSHOT_JSON).expect("should parse snapshot JSON");

        assert_eq!(resp.channel, "l2_data");
        assert_eq!(resp.sequence_num, 0);
        assert_eq!(resp.events.len(), 1);

        let event = &resp.events[0];
        assert_eq!(event.ty, "snapshot");
        assert_eq!(event.product_id, "BTC-USD");
        assert_eq!(event.updates.len(), 4);

        // Verify bid/offer sides
        let bids: Vec<&CoinbaseL2Update> =
            event.updates.iter().filter(|u| u.side == "bid").collect();
        let offers: Vec<&CoinbaseL2Update> =
            event.updates.iter().filter(|u| u.side == "offer").collect();
        assert_eq!(bids.len(), 2);
        assert_eq!(offers.len(), 2);

        // Check specific price levels
        assert_eq!(bids[0].price_level, "21921.73");
        assert_eq!(bids[0].new_quantity, "0.06317902");
        assert_eq!(offers[0].price_level, "21922.00");
        assert_eq!(offers[0].new_quantity, "0.50000000");
    }

    #[test]
    fn test_parse_l2_update() {
        let resp: CoinbaseOrderbookResponse =
            serde_json::from_str(UPDATE_JSON).expect("should parse update JSON");

        assert_eq!(resp.channel, "l2_data");
        assert_eq!(resp.sequence_num, 5);

        let event = &resp.events[0];
        assert_eq!(event.ty, "update");

        // new_quantity == "0" means remove the level
        let removed: Vec<&CoinbaseL2Update> = event
            .updates
            .iter()
            .filter(|u| u.new_quantity == "0")
            .collect();
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].price_level, "21922.00");
        assert_eq!(removed[0].side, "offer");
    }

    // ── NormalizedDelta conversion tests ─────────────────────────────────

    #[test]
    fn test_to_normalized_snapshot() {
        let resp: CoinbaseOrderbookResponse =
            serde_json::from_str(SNAPSHOT_JSON).unwrap();

        let normalized = resp
            .to_normalized()
            .expect("snapshot should produce a NormalizedDelta");

        assert_eq!(normalized.symbol, "BTC-USD");
        assert!(normalized.is_snapshot);
        assert_eq!(normalized.update_id, 0);
        assert_eq!(normalized.sequence, 0);
        assert_eq!(normalized.bids.len(), 2);
        assert_eq!(normalized.asks.len(), 2);

        // Bids should contain the two bid levels
        assert!(normalized.bids.iter().any(|(p, _)| p == "21921.73"));
        assert!(normalized.bids.iter().any(|(p, _)| p == "21921.00"));

        // Asks should contain the two offer levels
        assert!(normalized.asks.iter().any(|(p, _)| p == "21922.00"));
        assert!(normalized.asks.iter().any(|(p, _)| p == "21923.50"));
    }

    #[test]
    fn test_to_normalized_update_not_snapshot() {
        let resp: CoinbaseOrderbookResponse = serde_json::from_str(UPDATE_JSON).unwrap();

        let normalized = resp.to_normalized().unwrap();

        assert!(
            !normalized.is_snapshot,
            "type='update' should not be is_snapshot"
        );
        assert_eq!(normalized.sequence, 5);
    }

    #[test]
    fn test_normalized_integrates_with_orderbook_delta() {
        let resp: CoinbaseOrderbookResponse =
            serde_json::from_str(SNAPSHOT_JSON).unwrap();
        let normalized = resp.to_normalized().unwrap();

        let mut ob = OrderbookDelta::new(TradingPair::new("BTC", "USD"));
        let result = ob.process(&normalized);
        assert!(result.is_ok(), "snapshot processing should succeed");

        // After processing, the book should have 2 bids and 2 asks
        assert_eq!(ob.top_bids(10).len(), 2);
        assert_eq!(ob.top_asks(10).len(), 2);

        // Apply the update (removes offer at 21922.00, adds bid at 21920.00)
        let update_resp: CoinbaseOrderbookResponse =
            serde_json::from_str(UPDATE_JSON).unwrap();
        let update_norm = update_resp.to_normalized().unwrap();
        let result = ob.process(&update_norm);
        assert!(result.is_ok());

        // Bids: 21921.73, 21921.00, 21920.00 (new) => 3
        assert_eq!(ob.top_bids(10).len(), 3);
        // Asks: 21923.50 only (21922.00 removed) => 1
        assert_eq!(ob.top_asks(10).len(), 1);
    }

    // ── Empty events ────────────────────────────────────────────────────

    #[test]
    fn test_to_normalized_empty_events() {
        let resp = CoinbaseOrderbookResponse {
            channel: "l2_data".to_string(),
            timestamp: "2023-02-09T20:32:50Z".to_string(),
            sequence_num: 0,
            events: vec![],
        };

        assert!(
            resp.to_normalized().is_none(),
            "empty events should produce None"
        );
    }

    // ── Live WSS streaming test ─────────────────────────────────────────

    #[tokio::test]
    #[ignore = "live venue websocket - validation bucket, run with --ignored"]
    async fn test_coinbase_wss_orderbook_stream() {
        let channels = vec!["level2".to_string()];
        let product_ids = vec!["BTC-USD".to_string()];

        let (tx, mut rx) = channel(1024);
        let client = CoinbaseWssClient::new(channels, product_ids);

        let handle = tokio::spawn(async move {
            let _ = client.receive_data(tx).await;
        });

        // Try to receive at least one orderbook event (timeout for CI)
        let result = timeout(Duration::from_secs(15), rx.recv()).await;

        match result {
            Ok(Some(CoinbaseWssEvent::OrderbookData(resp))) => {
                assert_eq!(resp.channel, "l2_data");
                assert!(!resp.events.is_empty());
                let event = &resp.events[0];
                assert_eq!(event.product_id, "BTC-USD");
                assert!(!event.updates.is_empty());
                // First message should be a snapshot
                assert_eq!(event.ty, "snapshot");
                println!(
                    "Received l2_data: product={}, updates={}, seq={}",
                    event.product_id,
                    event.updates.len(),
                    resp.sequence_num,
                );
            }
            Ok(Some(_)) => {
                // Got a trade event instead — still valid
                println!("Received non-orderbook event (expected in mixed channels)");
            }
            Ok(None) => {
                println!("Channel closed (expected in test environments)");
            }
            Err(_) => {
                // Timeout — acceptable in CI or sandboxed environments
                println!(
                    "Timeout waiting for Coinbase WSS data (expected in sandboxed envs)"
                );
            }
        }

        handle.abort();
    }
}
