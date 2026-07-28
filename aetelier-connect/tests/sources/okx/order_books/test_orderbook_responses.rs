//! Tests for OKX `books5` order-book WebSocket response parsing and decoding.

#[cfg(test)]
mod tests {
    use aetelier_connect::clients::wss::WssDecoder;
    use aetelier_connect::sources::okx::decoder::OkxDecoder;
    use aetelier_connect::sources::okx::events::OkxWssEvent;
    use aetelier_connect::sources::okx::responses::OkxOrderbookResponse;

    // Real `books5` frame shape (full top-N snapshot, no `action`).
    const BOOKS5_JSON: &str = r#"{
        "arg": { "channel": "books5", "instId": "BTC-USDT" },
        "data": [{
            "asks": [["31685.1","0.0001","0","1"],["31685.2","0.5","0","3"]],
            "bids": [["31684.9","0.01","0","1"],["31684.5","2.0","0","7"]],
            "instId": "BTC-USDT",
            "ts": "1626537446491",
            "seqId": 1234567
        }]
    }"#;

    // Incremental `books` frame (has `action`, prevSeqId, checksum).
    const BOOKS_UPDATE_JSON: &str = r#"{
        "arg": { "channel": "books", "instId": "BTC-USDT" },
        "action": "update",
        "data": [{
            "asks": [["31657.7","0","0","0"]],
            "bids": [["31642.9","0.50296385","0","4"]],
            "ts": "1626535709008",
            "checksum": 830931827,
            "seqId": 123457,
            "prevSeqId": 123456
        }]
    }"#;

    #[test]
    fn test_parse_books5_snapshot() {
        let resp: OkxOrderbookResponse =
            serde_json::from_str(BOOKS5_JSON).expect("should parse books5 JSON");

        assert_eq!(resp.arg.channel, "books5");
        assert_eq!(resp.arg.inst_id, "BTC-USDT");
        assert!(resp.action.is_none(), "books5 has no action field");
        assert!(resp.is_snapshot(), "books5 frames are full snapshots");

        let book = &resp.data[0];
        assert_eq!(book.bids.len(), 2);
        assert_eq!(book.asks.len(), 2);
        assert_eq!(book.ts_ms(), 1626537446491);
        assert_eq!(book.seq_id, 1234567);

        // String prices preserved; accessors parse correctly.
        assert_eq!(book.bids[0].price_str(), "31684.9");
        assert_eq!(book.bids[0].size_str(), "0.01");
        assert_eq!(book.asks[0].price(), 31685.1);
        assert_eq!(book.asks[0].size(), 0.0001);
    }

    #[test]
    fn test_parse_books_update() {
        let resp: OkxOrderbookResponse =
            serde_json::from_str(BOOKS_UPDATE_JSON).expect("should parse books update");

        assert_eq!(resp.arg.channel, "books");
        assert_eq!(resp.action.as_deref(), Some("update"));
        assert!(!resp.is_snapshot(), "action=update is not a snapshot");

        let book = &resp.data[0];
        assert_eq!(book.seq_id, 123457);
        assert_eq!(book.prev_seq_id, Some(123456));
        assert_eq!(book.checksum, Some(830931827));
        // size "0" = level deletion on the incremental channel.
        assert_eq!(book.asks[0].size_str(), "0");
    }

    #[test]
    fn test_decoder_routes_orderbook() {
        let decoded = OkxDecoder::decode(BOOKS5_JSON).expect("decode ok");
        match decoded {
            Some(OkxWssEvent::OrderbookData(resp)) => {
                assert_eq!(resp.arg.channel, "books5");
                assert_eq!(resp.data[0].bids.len(), 2);
            }
            other => panic!("expected OrderbookData, got {:?}", other),
        }
    }

    #[test]
    fn test_decoder_skips_subscription_ack() {
        let ack = r#"{"event":"subscribe","arg":{"channel":"books5","instId":"BTC-USDT"},"connId":"abc123"}"#;
        let decoded = OkxDecoder::decode(ack).expect("decode ok");
        assert!(decoded.is_none(), "subscription ack should yield None");
    }

    #[test]
    fn test_decoder_skips_pong() {
        let decoded = OkxDecoder::decode("pong").expect("decode ok");
        assert!(decoded.is_none(), "literal pong should yield None");
    }
}
