//! Tests for Bybit orderbook WebSocket response parsing,
//! NormalizedDelta conversion, and OrderbookDelta integration.
//!
//! Mirrors the test structure of Coinbase and Kraken orderbook tests
//! for cross-exchange consistency.

#[cfg(test)]
mod tests {
    use aetelier_connect::sources::bybit::{
        responses::orderbooks::{BybitOrderbookResponse, BybitPriceLevel},
        tooling,
    };
    use aetelier_types::orderbooks::OrderbookDelta;
    use aetelier_types::trading_pair::TradingPair;

    // ── JSON fixtures ────────────────────────────────────────────────────

    /// Snapshot message matching the Bybit WebSocket v5 docs:
    /// topic = "orderbook.50.BTCUSDT", type = "snapshot".
    const SNAPSHOT_JSON: &str = r#"{
        "topic": "orderbook.50.BTCUSDT",
        "type": "snapshot",
        "ts": 1672304484978,
        "data": {
            "s": "BTCUSDT",
            "b": [
                ["21921.73", "0.063"],
                ["21921.00", "1.000"]
            ],
            "a": [
                ["21922.00", "0.500"],
                ["21923.50", "2.000"]
            ],
            "u": 18521288,
            "seq": 7961638724
        },
        "cts": 1672304484998
    }"#;

    /// Incremental delta message (type = "delta").
    const DELTA_JSON: &str = r#"{
        "topic": "orderbook.50.BTCUSDT",
        "type": "delta",
        "ts": 1672304485100,
        "data": {
            "s": "BTCUSDT",
            "b": [
                ["21920.00", "3.000"]
            ],
            "a": [
                ["21922.00", "0"]
            ],
            "u": 18521289,
            "seq": 7961638725
        },
        "cts": 1672304485120
    }"#;

    // ── Deserialization tests ────────────────────────────────────────────

    #[test]
    fn test_parse_ob_snapshot() {
        let resp: BybitOrderbookResponse =
            serde_json::from_str(SNAPSHOT_JSON).expect("should parse snapshot JSON");

        assert_eq!(resp.topic, "orderbook.50.BTCUSDT");
        assert_eq!(resp.ty, "snapshot");
        assert_eq!(resp.orderbook_ts_ms, 1672304484978);
        assert_eq!(resp.cts, Some(1672304484998));

        let data = &resp.data;
        assert_eq!(data.symbol, "BTCUSDT");
        assert_eq!(data.bids.len(), 2);
        assert_eq!(data.asks.len(), 2);
        assert_eq!(data.update_id, 18521288);
        assert_eq!(data.sequence, 7961638724);

        // Check specific price levels
        assert_eq!(data.bids[0], BybitPriceLevel::new("21921.73", "0.063"));
        assert_eq!(data.bids[1], BybitPriceLevel::new("21921.00", "1.000"));
        assert_eq!(data.asks[0], BybitPriceLevel::new("21922.00", "0.500"));
        assert_eq!(data.asks[1], BybitPriceLevel::new("21923.50", "2.000"));
    }

    #[test]
    fn test_parse_ob_delta() {
        let resp: BybitOrderbookResponse =
            serde_json::from_str(DELTA_JSON).expect("should parse delta JSON");

        assert_eq!(resp.topic, "orderbook.50.BTCUSDT");
        assert_eq!(resp.ty, "delta");
        assert_eq!(resp.orderbook_ts_ms, 1672304485100);

        let data = &resp.data;
        assert_eq!(data.bids.len(), 1);
        assert_eq!(data.asks.len(), 1);
        assert_eq!(data.update_id, 18521289);
        assert_eq!(data.sequence, 7961638725);

        // size == "0" means remove the level
        assert!(data.asks[0].is_deletion());
        assert_eq!(data.asks[0].price_str(), "21922.00");
    }

    // ── NormalizedDelta conversion tests ─────────────────────────────────

    #[test]
    fn test_to_normalized_snapshot() {
        let resp: BybitOrderbookResponse = serde_json::from_str(SNAPSHOT_JSON).unwrap();

        let normalized = resp
            .to_normalized()
            .expect("snapshot should produce a NormalizedDelta");

        assert_eq!(normalized.symbol, "BTCUSDT");
        assert!(normalized.is_snapshot);
        assert_eq!(normalized.update_id, 18521288);
        assert_eq!(normalized.sequence, 7961638724);
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
    fn test_to_normalized_delta_not_snapshot() {
        let resp: BybitOrderbookResponse = serde_json::from_str(DELTA_JSON).unwrap();

        let normalized = resp.to_normalized().unwrap();

        assert!(
            !normalized.is_snapshot,
            "type='delta' should not be is_snapshot"
        );
        assert_eq!(normalized.sequence, 7961638725);
    }

    #[test]
    fn test_normalized_integrates_with_orderbook_delta() {
        let resp: BybitOrderbookResponse = serde_json::from_str(SNAPSHOT_JSON).unwrap();
        let normalized = resp.to_normalized().unwrap();

        let mut ob = OrderbookDelta::new(TradingPair::new("BTC", "USDT"));
        let result = ob.process(&normalized);
        assert!(result.is_ok(), "snapshot processing should succeed");

        // After processing, the book should have 2 bids and 2 asks
        assert_eq!(ob.top_bids(10).len(), 2);
        assert_eq!(ob.top_asks(10).len(), 2);

        // Apply the delta (removes ask at 21922.00, adds bid at 21920.00)
        let delta_resp: BybitOrderbookResponse =
            serde_json::from_str(DELTA_JSON).unwrap();
        let delta_norm = delta_resp.to_normalized().unwrap();
        let result = ob.process(&delta_norm);
        assert!(result.is_ok());

        // Bids: 21921.73, 21921.00, 21920.00 (new) => 3
        assert_eq!(ob.top_bids(10).len(), 3);
        // Asks: 21923.50 only (21922.00 removed by size="0") => 1
        assert_eq!(ob.top_asks(10).len(), 1);
    }

    // ── Empty data ───────────────────────────────────────────────────────

    #[test]
    fn test_to_normalized_empty_data() {
        let resp = BybitOrderbookResponse {
            topic: "orderbook.50.BTCUSDT".to_string(),
            ty: "snapshot".to_string(),
            orderbook_ts_ms: 0,
            data: aetelier_connect::sources::bybit::responses::orderbooks::BybitOrderbookData {
                symbol: String::new(),
                bids: vec![],
                asks: vec![],
                update_id: 0,
                sequence: 0,
            },
            cts: None,
        };

        assert!(
            resp.to_normalized().is_none(),
            "empty bids+asks+symbol should produce None"
        );
    }

    // ── Stream topic construction (kept from original) ───────────────────

    #[test]
    fn test_create_streams_topics() {
        let topic = "orderbook".to_string();
        let symbols = ["SOLUSDT".to_string()];
        let depths = [50];
        let streams = tooling::create_streams_topics(&topic, &symbols, &depths);

        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0], "orderbook.50.SOLUSDT");
    }
}
