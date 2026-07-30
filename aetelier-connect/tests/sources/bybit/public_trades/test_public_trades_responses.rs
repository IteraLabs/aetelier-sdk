//! Tests for Bybit `publicTrade` WebSocket response parsing.
//!
//! Mirrors the test structure of Coinbase and Kraken trade tests
//! for cross-exchange consistency.

#[cfg(test)]
mod tests {
    use aetelier_connect::sources::bybit::responses::trades::BybitTradeResponse;

    // ── JSON fixtures ────────────────────────────────────────────────────

    /// Snapshot message matching the Bybit WebSocket v5 docs:
    /// topic = "publicTrade.BTCUSDT", type = "snapshot".
    const TRADES_SNAPSHOT_JSON: &str = r#"{
        "topic": "publicTrade.BTCUSDT",
        "type": "snapshot",
        "ts": 1672304484978,
        "data": [
            {
                "T": 1672304484932,
                "s": "BTCUSDT",
                "S": "Buy",
                "v": "0.001",
                "p": "23536.30",
                "L": "PlusTick",
                "i": "2100000000006930573",
                "BT": false,
                "RPI": false,
                "seq": 7961638724
            },
            {
                "T": 1672304484900,
                "s": "BTCUSDT",
                "S": "Sell",
                "v": "0.500",
                "p": "23535.00",
                "L": "MinusTick",
                "i": "2100000000006930574",
                "BT": false,
                "RPI": false,
                "seq": 7961638725
            }
        ]
    }"#;

    /// Update message with a single trade.
    const TRADE_UPDATE_JSON: &str = r#"{
        "topic": "publicTrade.ETHUSDT",
        "type": "snapshot",
        "ts": 1672304485100,
        "data": [
            {
                "T": 1672304485050,
                "s": "ETHUSDT",
                "S": "Buy",
                "v": "10.0",
                "p": "1650.25",
                "L": "PlusTick",
                "i": "2100000000006930575",
                "BT": false,
                "RPI": false,
                "seq": 7961638726
            }
        ]
    }"#;

    // ── Deserialization tests ────────────────────────────────────────────

    #[test]
    fn test_parse_trades_snapshot() {
        let resp: BybitTradeResponse = serde_json::from_str(TRADES_SNAPSHOT_JSON)
            .expect("should parse trades snapshot");

        assert_eq!(resp.topic, "publicTrade.BTCUSDT");
        assert_eq!(resp.ty, "snapshot");
        assert_eq!(resp.ts, 1672304484978);
        assert_eq!(resp.data.len(), 2);

        let buy = &resp.data[0];
        assert_eq!(buy.trade_ts, 1672304484932);
        assert_eq!(buy.symbol, "BTCUSDT");
        assert_eq!(buy.side, "Buy");
        assert_eq!(buy.amount, "0.001");
        assert_eq!(buy.price, "23536.30");
        assert_eq!(buy.trade_id, "2100000000006930573");

        let sell = &resp.data[1];
        assert_eq!(sell.side, "Sell");
        assert_eq!(sell.amount, "0.500");
        assert_eq!(sell.price, "23535.00");
    }

    #[test]
    fn test_parse_trade_update() {
        let resp: BybitTradeResponse =
            serde_json::from_str(TRADE_UPDATE_JSON).expect("should parse trade update");

        assert_eq!(resp.data.len(), 1);

        let trade = &resp.data[0];
        assert_eq!(trade.symbol, "ETHUSDT");
        assert_eq!(trade.price, "1650.25");
        assert_eq!(trade.amount, "10.0");
    }

    // ── Field validation ─────────────────────────────────────────────────

    #[test]
    fn test_trade_fields() {
        let resp: BybitTradeResponse =
            serde_json::from_str(TRADES_SNAPSHOT_JSON).unwrap();

        let trade = &resp.data[0];

        // Exchange-reported timestamp is the exact epoch-ms from the fixture.
        assert_eq!(trade.trade_ts, 1672304484932, "trade_ts (epoch ms)");
        // Same instant expressed as the platform-standard epoch microseconds.
        assert_eq!(
            trade.trade_ts * 1_000,
            1672304484932000,
            "trade_ts converted to epoch us"
        );

        // Symbol is the exact pair from the fixture.
        assert_eq!(trade.symbol, "BTCUSDT");

        // Side is the exact Bybit PascalCase taker side.
        assert_eq!(trade.side, "Buy");

        // Price and amount are the exact string values, and parse to the exact
        // f64 the doc-shaped JSON encodes.
        assert_eq!(trade.price, "23536.30");
        assert_eq!(trade.amount, "0.001");
        let price: f64 = trade.price.parse().expect("price should parse to f64");
        assert_eq!(price, 23536.30, "price parsed to f64");
        let amount: f64 = trade.amount.parse().expect("amount should parse to f64");
        assert_eq!(amount, 0.001, "amount parsed to f64");

        // Exchange-assigned identifiers match the fixture exactly.
        assert_eq!(trade.trade_id, "2100000000006930573");
        assert_eq!(trade.sequence, 7961638724);
    }

    // ── Side values match Bybit API ──────────────────────────────────────

    #[test]
    fn test_side_values_buy_sell() {
        // Bybit publicTrade uses "Buy"/"Sell" (PascalCase)
        let resp: BybitTradeResponse =
            serde_json::from_str(TRADES_SNAPSHOT_JSON).unwrap();

        let sides: Vec<&str> = resp.data.iter().map(|t| t.side.as_str()).collect();

        for side in &sides {
            assert!(
                *side == "Buy" || *side == "Sell",
                "trade side must be Buy or Sell, got: {}",
                side
            );
        }
    }
}
