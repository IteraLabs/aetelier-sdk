//! Tests for Gateio.io `spot.trades` response parsing and decoding.

#[cfg(test)]
mod tests {
    use aetelier_connect::clients::wss::WssDecoder;
    use aetelier_connect::sources::gateio::decoder::GateioDecoder;
    use aetelier_connect::sources::gateio::events::GateioWssEvent;
    use aetelier_connect::sources::gateio::responses::GateioTradeResponse;

    const TRADE_JSON: &str = r#"{
        "time": 1606292218,
        "time_ms": 1606292218231,
        "channel": "spot.trades",
        "event": "update",
        "result": {
            "id": 309143071,
            "create_time": 1606292218,
            "create_time_ms": "1606292218213.4578",
            "side": "sell",
            "currency_pair": "BTC_USDT",
            "amount": "16.4700000000",
            "price": "0.4705000000"
        }
    }"#;

    #[test]
    fn test_parse_trade() {
        let resp: GateioTradeResponse =
            serde_json::from_str(TRADE_JSON).expect("should parse trade JSON");

        assert_eq!(resp.channel, "spot.trades");
        assert_eq!(resp.event, "update");

        let t = &resp.result;
        assert_eq!(t.id, 309143071);
        assert_eq!(t.currency_pair, "BTC_USDT");
        assert_eq!(t.side, "sell");
        assert_eq!(t.amount, "16.4700000000");
        assert_eq!(t.price, "0.4705000000");
    }

    #[test]
    fn test_fractional_ms_timestamp_truncates() {
        let resp: GateioTradeResponse = serde_json::from_str(TRADE_JSON).unwrap();
        // "1606292218213.4578" -> 1606292218213 (sub-ms part dropped).
        assert_eq!(resp.result.ts_ms(), 1606292218213);
    }

    #[test]
    fn test_taker_side_parses_to_tradeside() {
        let resp: GateioTradeResponse = serde_json::from_str(TRADE_JSON).unwrap();
        let side: aetelier_types::TradeSide = resp.result.side.parse().unwrap();
        assert_eq!(side, aetelier_types::TradeSide::Sell);
    }

    #[test]
    fn test_decoder_routes_trade() {
        let decoded = GateioDecoder::decode(TRADE_JSON).expect("decode ok");
        match decoded {
            Some(GateioWssEvent::TradeData(t)) => {
                assert_eq!(t.id, 309143071);
                assert_eq!(t.price, "0.4705000000");
            }
            other => panic!("expected TradeData, got {:?}", other),
        }
    }
}
