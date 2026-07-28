//! Unit tests for exchange-specific WebSocket decoders (Test 21).
//!
//! For each exchange decoder ([`BybitDecoder`], [`CoinbaseDecoder`],
//! [`KrakenDecoder`]): feed a real-shaped JSON frame, assert `decode()`
//! returns the expected typed event.  Feed a subscription ack, assert
//! `Ok(None)`.  Feed garbage, assert `Err(...)`.
//!
//! **Pure unit tests — no networking involved.**

#[cfg(test)]
mod tests {
    use aetelier_connect::clients::wss::WssDecoder;

    // ══════════════════════════════════════════════════════════════════
    // Bybit
    // ══════════════════════════════════════════════════════════════════

    mod bybit {
        use super::*;
        use aetelier_connect::sources::bybit::decoder::BybitDecoder;
        use aetelier_connect::sources::bybit::events::BybitWssEvent;

        // ── Data frames ───────────────────────────────────────────────

        #[test]
        fn test_bybit_decode_trade_frame() {
            let json = r#"{
                "topic": "publicTrade.BTCUSDT",
                "type": "snapshot",
                "ts": 1672304484978,
                "data": [{
                    "T": 1672304484978,
                    "s": "BTCUSDT",
                    "S": "Buy",
                    "v": "0.001",
                    "p": "16578.50",
                    "L": "PlusTick",
                    "i": "2100000000007764767",
                    "BT": false,
                    "RPI": false,
                    "seq": 7961638724
                }]
            }"#;

            let result = BybitDecoder::decode(json);
            let Ok(Some(event)) = result else {
                panic!("expected Ok(Some(TradeData)), got {:?}", result.is_ok());
            };

            assert!(
                matches!(&event, BybitWssEvent::TradeData(t) if t.first().is_some_and(|x| x.symbol == "BTCUSDT")),
                "should decode to TradeData with symbol BTCUSDT, got: {event:?}",
            );

            // Verify specific field values from the JSON fixture (single fill).
            if let BybitWssEvent::TradeData(t) = &event {
                let t = &t[0];
                assert_eq!(t.price, "16578.50");
                assert_eq!(t.amount, "0.001");
                assert_eq!(t.side, "Buy");
                assert_eq!(t.trade_id, "2100000000007764767");
                assert!(!t.block_trade);
            }
        }

        #[test]
        fn test_bybit_decode_orderbook_frame() {
            let json = r#"{
                "topic": "orderbook.50.BTCUSDT",
                "type": "snapshot",
                "ts": 1672304484978,
                "data": {
                    "s": "BTCUSDT",
                    "b": [["16493.50", "0.006"]],
                    "a": [["16493.75", "0.100"]],
                    "u": 18521288,
                    "seq": 7961638724
                },
                "cts": 1672304484998
            }"#;

            let result = BybitDecoder::decode(json);
            let Ok(Some(event)) = result else {
                panic!("expected Ok(Some(OrderbookData))");
            };

            assert!(
                matches!(&event, BybitWssEvent::OrderbookData(ob) if ob.data.symbol == "BTCUSDT"),
                "should decode to OrderbookData, got: {event:?}",
            );
        }

        #[test]
        fn test_bybit_decode_liquidation_frame() {
            let json = r#"{
                "topic": "allLiquidation.BTCUSDT",
                "type": "snapshot",
                "ts": 1672304484978,
                "data": [{
                    "T": 1672304484978,
                    "s": "BTCUSDT",
                    "S": "Sell",
                    "v": "0.500",
                    "p": "16500.00"
                }]
            }"#;

            let result = BybitDecoder::decode(json);
            let Ok(Some(event)) = result else {
                panic!("expected Ok(Some(LiquidationData))");
            };

            assert!(
                matches!(&event, BybitWssEvent::LiquidationData(l) if l.symbol == "BTCUSDT"),
                "should decode to LiquidationData, got: {event:?}",
            );
        }

        #[test]
        fn test_bybit_decode_ticker_frame() {
            let json = r#"{
                "topic": "tickers.BTCUSDT",
                "type": "snapshot",
                "cs": 12345,
                "ts": 1672304484978,
                "data": {
                    "symbol": "BTCUSDT",
                    "lastPrice": "16578.50"
                }
            }"#;

            let result = BybitDecoder::decode(json);
            let Ok(Some(event)) = result else {
                panic!("expected Ok(Some(TickerData))");
            };

            assert!(
                matches!(&event, BybitWssEvent::TickerData(t) if t.symbol == "BTCUSDT"),
                "should decode to TickerData, got: {event:?}",
            );
        }

        // ── Control frames (Ok(None)) ─────────────────────────────────

        #[test]
        fn test_bybit_decode_subscription_ack_returns_none() {
            let json = r#"{
                "success": true,
                "ret_msg": "subscribe",
                "conn_id": "abc123",
                "op": "subscribe"
            }"#;

            let result = BybitDecoder::decode(json);
            let Ok(None) = result else {
                panic!("subscription ack should produce Ok(None)");
            };
        }

        #[test]
        fn test_bybit_decode_pong_returns_none() {
            let json = r#"{"op": "pong", "args": ["1234567890"], "req_id": "100001"}"#;

            let result = BybitDecoder::decode(json);
            let Ok(None) = result else {
                panic!("pong message should produce Ok(None)");
            };
        }

        #[test]
        fn test_bybit_decode_unknown_topic_returns_none() {
            let json = r#"{
                "topic": "unknown.thing",
                "type": "snapshot",
                "ts": 0,
                "data": {}
            }"#;

            let result = BybitDecoder::decode(json);
            let Ok(None) = result else {
                panic!("unknown topic should produce Ok(None)");
            };
        }

        // ── Error path ────────────────────────────────────────────────

        #[test]
        fn test_bybit_decode_garbage_returns_err() {
            let result = BybitDecoder::decode("{not valid json at all");
            assert!(result.is_err(), "garbage JSON should return Err");
        }
    }

    // ══════════════════════════════════════════════════════════════════
    // Coinbase
    // ══════════════════════════════════════════════════════════════════

    mod coinbase {
        use super::*;
        use aetelier_connect::sources::coinbase::decoder::CoinbaseDecoder;
        use aetelier_connect::sources::coinbase::events::CoinbaseWssEvent;

        // ── Data frames ───────────────────────────────────────────────

        #[test]
        fn test_coinbase_decode_trade_frame() {
            let json = r#"{
                "channel": "market_trades",
                "timestamp": "2023-02-09T20:19:35.39625135Z",
                "sequence_num": 42,
                "events": [{
                    "type": "snapshot",
                    "trades": [{
                        "trade_id": "789",
                        "product_id": "BTC-USD",
                        "price": "23536.30",
                        "size": "0.001",
                        "side": "BUY",
                        "time": "2023-02-09T20:19:35.396Z"
                    }]
                }]
            }"#;

            let result = CoinbaseDecoder::decode(json);
            let Ok(Some(event)) = result else {
                panic!("expected Ok(Some(TradeData))");
            };

            assert!(
                matches!(&event, CoinbaseWssEvent::TradeData(t) if t[0].product_id == "BTC-USD"),
                "should decode to TradeData, got: {event:?}",
            );

            if let CoinbaseWssEvent::TradeData(t) = &event {
                assert_eq!(t.len(), 1);
                assert_eq!(t[0].trade_id, "789");
                assert_eq!(t[0].price, "23536.30");
                assert_eq!(t[0].size, "0.001");
                assert_eq!(t[0].side, "BUY");
            }
        }

        #[test]
        fn test_coinbase_decode_orderbook_frame() {
            let json = r#"{
                "channel": "l2_data",
                "timestamp": "2023-02-09T20:32:50.714964855Z",
                "sequence_num": 0,
                "events": [{
                    "type": "snapshot",
                    "product_id": "BTC-USD",
                    "updates": [
                        {"side": "bid", "event_time": "2023-02-09T20:32:50Z", "price_level": "23000.00", "new_quantity": "1.5"},
                        {"side": "offer", "event_time": "2023-02-09T20:32:50Z", "price_level": "23001.00", "new_quantity": "0.8"}
                    ]
                }]
            }"#;

            let result = CoinbaseDecoder::decode(json);
            let Ok(Some(event)) = result else {
                panic!("expected Ok(Some(OrderbookData))");
            };

            assert!(
                matches!(&event, CoinbaseWssEvent::OrderbookData(_)),
                "should decode to OrderbookData, got: {event:?}",
            );

            if let CoinbaseWssEvent::OrderbookData(ob) = &event {
                assert_eq!(ob.channel, "l2_data");
                assert_eq!(ob.sequence_num, 0);
                assert_eq!(ob.events.len(), 1);

                let ev = &ob.events[0];
                assert_eq!(ev.ty, "snapshot");
                assert_eq!(ev.product_id, "BTC-USD");
                assert_eq!(ev.updates.len(), 2);

                let bid = &ev.updates[0];
                assert_eq!(bid.side, "bid");
                assert_eq!(bid.price_level, "23000.00");
                assert_eq!(bid.new_quantity, "1.5");

                let ask = &ev.updates[1];
                assert_eq!(ask.side, "offer");
                assert_eq!(ask.price_level, "23001.00");
                assert_eq!(ask.new_quantity, "0.8");
            }
        }

        // ── Control frames (Ok(None)) ─────────────────────────────────

        #[test]
        fn test_coinbase_decode_subscription_ack_returns_none() {
            let json = r#"{"type": "subscriptions", "channels": []}"#;

            let result = CoinbaseDecoder::decode(json);
            let Ok(None) = result else {
                panic!("subscription ack should produce Ok(None)");
            };
        }

        #[test]
        fn test_coinbase_decode_error_message_returns_none() {
            let json = r#"{"type": "error", "message": "bad request"}"#;

            let result = CoinbaseDecoder::decode(json);
            let Ok(None) = result else {
                panic!("error type message should produce Ok(None)");
            };
        }

        #[test]
        fn test_coinbase_decode_heartbeat_returns_none() {
            let json = r#"{"channel": "heartbeat"}"#;

            let result = CoinbaseDecoder::decode(json);
            let Ok(None) = result else {
                panic!("heartbeat channel should produce Ok(None)");
            };
        }

        #[test]
        fn test_coinbase_decode_unknown_channel_returns_none() {
            let json = r#"{"channel": "status"}"#;

            let result = CoinbaseDecoder::decode(json);
            let Ok(None) = result else {
                panic!("unknown channel should produce Ok(None)");
            };
        }

        // ── Error path ────────────────────────────────────────────────

        #[test]
        fn test_coinbase_decode_garbage_returns_err() {
            let result = CoinbaseDecoder::decode("{{{{garbage}}}");
            assert!(result.is_err(), "garbage JSON should return Err");
        }
    }

    // ══════════════════════════════════════════════════════════════════
    // Kraken
    // ══════════════════════════════════════════════════════════════════

    mod kraken {
        use super::*;
        use aetelier_connect::sources::kraken::decoder::KrakenDecoder;
        use aetelier_connect::sources::kraken::events::KrakenWssEvent;

        // ── Data frames ───────────────────────────────────────────────

        #[test]
        fn test_kraken_decode_trade_frame() {
            let json = r#"{
                "channel": "trade",
                "type": "update",
                "data": [{
                    "symbol": "BTC/USD",
                    "side": "buy",
                    "price": 23536.30,
                    "qty": 0.001,
                    "ord_type": "limit",
                    "trade_id": 12345,
                    "timestamp": "2023-02-09T20:19:35.396Z"
                }]
            }"#;

            let result = KrakenDecoder::decode(json);
            let Ok(Some(event)) = result else {
                panic!("expected Ok(Some(TradeData))");
            };

            assert!(
                matches!(&event, KrakenWssEvent::TradeData(t) if t[0].symbol == "BTC/USD"),
                "should decode to TradeData, got: {event:?}",
            );

            if let KrakenWssEvent::TradeData(t) = &event {
                assert_eq!(t.len(), 1);
                assert_eq!(t[0].trade_id, 12345);
                assert!((t[0].price - 23536.30).abs() < f64::EPSILON);
                assert!((t[0].qty - 0.001).abs() < f64::EPSILON);
                assert_eq!(t[0].side, "buy");
            }
        }

        #[test]
        fn test_kraken_decode_book_frame() {
            let json = r#"{
                "channel": "book",
                "type": "snapshot",
                "data": [{
                    "symbol": "BTC/USD",
                    "bids": [{"price": 21921.73, "qty": 0.063}],
                    "asks": [{"price": 21922.00, "qty": 0.500}],
                    "checksum": 2439117997,
                    "timestamp": "2023-09-26T16:49:20.962586Z"
                }]
            }"#;

            let result = KrakenDecoder::decode(json);
            let Ok(Some(event)) = result else {
                panic!("expected Ok(Some(OrderbookData))");
            };

            assert!(
                matches!(&event, KrakenWssEvent::OrderbookData(_)),
                "should decode to OrderbookData, got: {event:?}",
            );

            if let KrakenWssEvent::OrderbookData(ob) = &event {
                assert_eq!(ob.channel, "book");
                assert_eq!(ob.ty, "snapshot");
                assert_eq!(ob.data.len(), 1);

                let d = &ob.data[0];
                assert_eq!(d.symbol, "BTC/USD");
                assert_eq!(d.checksum, 2439117997);
                assert_eq!(d.bids.len(), 1);
                assert_eq!(d.asks.len(), 1);

                // `price`/`qty` are preserved as their exact JSON wire tokens,
                // so trailing zeros (`21922.00`, `0.500`) survive verbatim.
                assert_eq!(d.bids[0].price, "21921.73");
                assert_eq!(d.bids[0].qty, "0.063");
                assert_eq!(d.asks[0].price, "21922.00");
                assert_eq!(d.asks[0].qty, "0.500");
            }
        }

        // ── Control frames (Ok(None)) ─────────────────────────────────

        #[test]
        fn test_kraken_decode_subscription_ack_returns_none() {
            let json = r#"{"method": "subscribe", "success": true}"#;

            let result = KrakenDecoder::decode(json);
            let Ok(None) = result else {
                panic!("subscription ack should produce Ok(None)");
            };
        }

        #[test]
        fn test_kraken_decode_subscription_error_returns_none() {
            let json =
                r#"{"method": "subscribe", "success": false, "error": "bad channel"}"#;

            let result = KrakenDecoder::decode(json);
            let Ok(None) = result else {
                panic!("subscription error should produce Ok(None) (logged, not fatal)");
            };
        }

        #[test]
        fn test_kraken_decode_heartbeat_returns_none() {
            let json = r#"{"channel": "heartbeat"}"#;

            let result = KrakenDecoder::decode(json);
            let Ok(None) = result else {
                panic!("heartbeat should produce Ok(None)");
            };
        }

        #[test]
        fn test_kraken_decode_unknown_channel_returns_none() {
            let json = r#"{"channel": "status", "type": "update", "data": []}"#;

            let result = KrakenDecoder::decode(json);
            let Ok(None) = result else {
                panic!("unknown channel should produce Ok(None)");
            };
        }

        // ── Error path ────────────────────────────────────────────────

        #[test]
        fn test_kraken_decode_garbage_returns_err() {
            let result = KrakenDecoder::decode("not json at all");
            assert!(result.is_err(), "garbage should return Err");
        }
    }
}
