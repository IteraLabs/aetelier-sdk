//! Tests for Gateio.io `spot.order_book` response parsing and decoding.

#[cfg(test)]
mod tests {
    use aetelier_connect::clients::wss::WssDecoder;
    use aetelier_connect::sources::gateio::decoder::GateioDecoder;
    use aetelier_connect::sources::gateio::events::GateioWssEvent;
    use aetelier_connect::sources::gateio::responses::GateioOrderbookResponse;

    const ORDER_BOOK_JSON: &str = r#"{
        "time": 1606295412,
        "time_ms": 1606295412213,
        "channel": "spot.order_book",
        "event": "update",
        "result": {
            "t": 1606295412123,
            "lastUpdateId": 48791820,
            "s": "BTC_USDT",
            "bids": [["19079.55","0.0195"],["19079.07","0.7341"]],
            "asks": [["19080.24","0.1638"],["19080.91","0.8513"]]
        }
    }"#;

    #[test]
    fn test_parse_order_book_snapshot() {
        let resp: GateioOrderbookResponse =
            serde_json::from_str(ORDER_BOOK_JSON).expect("should parse order_book JSON");

        assert_eq!(resp.channel, "spot.order_book");
        assert_eq!(resp.event, "update");
        assert_eq!(resp.time_ms, Some(1606295412213));

        let book = &resp.result;
        assert_eq!(book.symbol, "BTC_USDT");
        assert_eq!(book.ts_ms, 1606295412123);
        assert_eq!(book.last_update_id, 48791820);
        assert_eq!(book.bids.len(), 2);
        assert_eq!(book.asks.len(), 2);

        assert_eq!(book.bids[0].price_str(), "19079.55");
        assert_eq!(book.bids[0].size_str(), "0.0195");
        assert_eq!(book.asks[0].price(), 19080.24);
        assert_eq!(book.asks[0].size(), 0.1638);
    }

    #[test]
    fn test_decoder_routes_orderbook() {
        let decoded = GateioDecoder::decode(ORDER_BOOK_JSON).expect("decode ok");
        match decoded {
            Some(GateioWssEvent::OrderbookData(resp)) => {
                assert_eq!(resp.result.symbol, "BTC_USDT");
                assert_eq!(resp.result.asks.len(), 2);
            }
            other => panic!("expected OrderbookData, got {:?}", other),
        }
    }

    #[test]
    fn test_decoder_skips_subscription_ack() {
        let ack = r#"{"time":1606295412,"time_ms":1606295412123,"id":1,"channel":"spot.order_book","event":"subscribe","error":null,"result":{"status":"success"}}"#;
        let decoded = GateioDecoder::decode(ack).expect("decode ok");
        assert!(decoded.is_none(), "subscription ack should yield None");
    }

    #[test]
    fn test_decoder_skips_pong() {
        let pong = r#"{"time":1545404023,"channel":"spot.pong","event":"","error":null,"result":null}"#;
        let decoded = GateioDecoder::decode(pong).expect("decode ok");
        assert!(decoded.is_none(), "spot.pong should yield None");
    }
}
