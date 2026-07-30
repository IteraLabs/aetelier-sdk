//! Tests for OKX `trades` WebSocket response parsing and decoding.

#[cfg(test)]
mod tests {
    use aetelier_connect::clients::wss::WssDecoder;
    use aetelier_connect::sources::okx::decoder::OkxDecoder;
    use aetelier_connect::sources::okx::events::OkxWssEvent;
    use aetelier_connect::sources::okx::responses::OkxTradeResponse;

    const TRADE_JSON: &str = r#"{
        "arg": { "channel": "trades", "instId": "BTC-USDT" },
        "data": [{
            "instId": "BTC-USDT",
            "tradeId": "216970876",
            "px": "31684.5",
            "sz": "0.00001186",
            "side": "buy",
            "ts": "1626531038288"
        }]
    }"#;

    #[test]
    fn test_parse_trade() {
        let resp: OkxTradeResponse =
            serde_json::from_str(TRADE_JSON).expect("should parse trade JSON");

        assert_eq!(resp.arg.channel, "trades");
        assert_eq!(resp.data.len(), 1);

        let t = &resp.data[0];
        assert_eq!(t.inst_id, "BTC-USDT");
        assert_eq!(t.trade_id, "216970876");
        assert_eq!(t.px, "31684.5");
        assert_eq!(t.sz, "0.00001186");
        assert_eq!(t.side, "buy");
        assert_eq!(t.ts_ms(), 1626531038288);
    }

    #[test]
    fn test_taker_side_parses_to_tradeside() {
        let resp: OkxTradeResponse = serde_json::from_str(TRADE_JSON).unwrap();
        let side: aetelier_types::TradeSide = resp.data[0].side.parse().unwrap();
        assert_eq!(side, aetelier_types::TradeSide::Buy);
    }

    #[test]
    fn test_decoder_routes_trade() {
        let decoded = OkxDecoder::decode(TRADE_JSON).expect("decode ok");
        match decoded {
            Some(OkxWssEvent::TradeData(trades)) => {
                assert_eq!(trades.len(), 1);
                assert_eq!(trades[0].trade_id, "216970876");
                assert_eq!(trades[0].px, "31684.5");
            }
            other => panic!("expected TradeData, got {:?}", other),
        }
    }

    #[test]
    fn test_trade_precision_preserved() {
        let json = r#"{
            "arg": { "channel": "trades", "instId": "BTC-USDT" },
            "data": [{
                "instId": "BTC-USDT", "tradeId": "1", "px": "99999.99999999",
                "sz": "0.00000001", "side": "sell", "ts": "1626531038288"
            }]
        }"#;
        let resp: OkxTradeResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.data[0].px, "99999.99999999");
        assert_eq!(resp.data[0].sz, "0.00000001");
    }
}
