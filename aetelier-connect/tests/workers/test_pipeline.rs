//! Tests for the EventPipeline middleware.
//!
//! Validates:
//! - PassthroughPipeline identity behaviour
//! - BookInitializer state machine (buffering, reconciliation, synced)
//! - Trade passthrough during buffering phase
//! - Stale delta discarding after snapshot

#![allow(deprecated)] // exercises the deprecated legacy raw-ingestion path (still live for derivatives/BYO)

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::{mpsc, watch};

    use aetelier_connect::sources::ExchangeEvent;
    use aetelier_connect::sources::binance::events::BinanceWssEvent;
    use aetelier_connect::sources::binance::responses::orderbooks::BinanceDepthUpdate;
    use aetelier_connect::sources::binance::responses::trades::BinanceTradeData;
    use aetelier_connect::workers::TopicMessage;
    use aetelier_connect::workers::pipeline::{EventPipeline, PassthroughPipeline};

    // ── Helpers ──────────────────────────────────────────────────────────

    fn make_depth_update_msg(first_id: u64, last_id: u64, topic: &str) -> TopicMessage {
        let upd = BinanceDepthUpdate {
            event_type: "depthUpdate".to_string(),
            event_time: 1672304484978,
            symbol: "BTCUSDT".to_string(),
            first_update_id: first_id,
            last_update_id: last_id,
            bids: vec![["21921.73".to_string(), "0.063".to_string()]],
            asks: vec![["21922.00".to_string(), "0.500".to_string()]],
        };
        TopicMessage {
            topic: topic.to_string(),
            received_at_us: 0,
            exchange: "binance".to_string(),
            payload: ExchangeEvent::Binance(BinanceWssEvent::DepthUpdate(upd)),
        }
    }

    fn make_trade_msg(topic: &str) -> TopicMessage {
        let trade = BinanceTradeData {
            event_type: "trade".to_string(),
            event_time: 1672304484978,
            symbol: "BTCUSDT".to_string(),
            trade_id: 12345,
            price: "21921.73".to_string(),
            quantity: "0.063".to_string(),
            trade_time: 1672304484975,
            is_buyer_maker: true,
        };
        TopicMessage {
            topic: topic.to_string(),
            received_at_us: 0,
            exchange: "binance".to_string(),
            payload: ExchangeEvent::Binance(BinanceWssEvent::TradeData(trade)),
        }
    }

    // ── PassthroughPipeline tests ────────────────────────────────────────

    #[tokio::test]
    async fn test_passthrough_forwards_all_events() {
        let (in_tx, in_rx) = mpsc::channel(16);
        let (out_tx, mut out_rx) = mpsc::channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let pipeline = Box::new(PassthroughPipeline);
        let handle = tokio::spawn(pipeline.run(in_rx, out_tx, shutdown_rx));

        // Send 5 events
        for i in 0..5 {
            let msg = make_depth_update_msg(i, i + 1, "orderbook.50.BTCUSDT");
            in_tx.send(msg).await.unwrap();
        }

        // Drop sender to close the pipeline
        drop(in_tx);

        // Collect all outputs
        let mut count = 0;
        while let Some(_msg) = out_rx.recv().await {
            count += 1;
        }

        assert_eq!(count, 5, "all 5 events should be forwarded");
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_passthrough_closes_on_input_drop() {
        let (in_tx, in_rx) = mpsc::channel(16);
        let (out_tx, _out_rx) = mpsc::channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let pipeline = Box::new(PassthroughPipeline);
        let handle = tokio::spawn(pipeline.run(in_rx, out_tx, shutdown_rx));

        // Drop the input sender immediately
        drop(in_tx);

        // Pipeline should terminate
        let result = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(
            result.is_ok(),
            "pipeline should terminate when input closes"
        );
    }

    // ── BookInitializer tests ────────────────────────────────────────────
    //
    // These tests use a local mock HTTP server to provide the REST snapshot.

    /// Start a mock HTTP server that returns a fixed depth snapshot response.
    async fn start_mock_server(
        last_update_id: u64,
    ) -> (tokio::task::JoinHandle<()>, String) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{}", addr);

        let body = format!(
            r#"{{"lastUpdateId":{},"bids":[["50000.00","1.000"]],"asks":[["50001.00","2.000"]]}}"#,
            last_update_id
        );

        let handle = tokio::spawn(async move {
            // Accept one connection and serve the snapshot.
            loop {
                let (mut stream, _) = match listener.accept().await {
                    Ok(s) => s,
                    Err(_) => break,
                };
                let body_clone = body.clone();
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};

                    // Read request (we don't parse it fully — just drain)
                    let mut buf = [0u8; 4096];
                    let _ = stream.read(&mut buf).await;

                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                        body_clone.len(),
                        body_clone
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
        });

        (handle, base_url)
    }

    #[tokio::test]
    async fn test_trades_forwarded_during_buffering() {
        use aetelier_connect::sources::binance::rest::BinanceRestClient;
        use aetelier_connect::workers::pipeline::book_initializer::BookInitializer;

        let (server_handle, base_url) = start_mock_server(100).await;

        let rest = BinanceRestClient::new(base_url);
        let initializer = BookInitializer::new(
            rest,
            "BTCUSDT".to_string(),
            "orderbook.50.BTCUSDT".to_string(),
        );

        let (in_tx, in_rx) = mpsc::channel(64);
        let (out_tx, mut out_rx) = mpsc::channel(64);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let handle = tokio::spawn(Box::new(initializer).run(in_rx, out_tx, shutdown_rx));

        // Send a trade event (should be forwarded immediately)
        let trade = make_trade_msg("trade.all.BTCUSDT");
        in_tx.send(trade).await.unwrap();

        // The trade should arrive quickly (before snapshot)
        let result = tokio::time::timeout(Duration::from_secs(3), out_rx.recv()).await;
        assert!(result.is_ok(), "trade should arrive during buffering");
        let msg = result.unwrap().unwrap();
        assert_eq!(msg.topic, "trade.all.BTCUSDT");

        drop(in_tx);
        let _ = handle.await;
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_deltas_buffered_until_snapshot() {
        use aetelier_connect::sources::binance::rest::BinanceRestClient;
        use aetelier_connect::workers::pipeline::book_initializer::BookInitializer;

        // Snapshot lastUpdateId = 100
        let (server_handle, base_url) = start_mock_server(100).await;

        let rest = BinanceRestClient::new(base_url);
        let initializer = BookInitializer::new(
            rest,
            "BTCUSDT".to_string(),
            "orderbook.50.BTCUSDT".to_string(),
        );

        let (in_tx, in_rx) = mpsc::channel(64);
        let (out_tx, mut out_rx) = mpsc::channel(64);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let handle = tokio::spawn(Box::new(initializer).run(in_rx, out_tx, shutdown_rx));

        // Send depth updates (some stale, some valid)
        // Stale: last_update_id <= 100
        in_tx
            .send(make_depth_update_msg(90, 95, "orderbook.50.BTCUSDT"))
            .await
            .unwrap();
        in_tx
            .send(make_depth_update_msg(96, 100, "orderbook.50.BTCUSDT"))
            .await
            .unwrap();
        // Valid: last_update_id > 100
        in_tx
            .send(make_depth_update_msg(101, 105, "orderbook.50.BTCUSDT"))
            .await
            .unwrap();
        in_tx
            .send(make_depth_update_msg(106, 110, "orderbook.50.BTCUSDT"))
            .await
            .unwrap();

        // Wait for the snapshot + reconciled events to arrive
        let mut received = Vec::new();
        for _ in 0..10 {
            match tokio::time::timeout(Duration::from_secs(3), out_rx.recv()).await {
                Ok(Some(msg)) => received.push(msg),
                _ => break,
            }
        }

        // First message should be the snapshot (DepthSnapshot)
        assert!(
            !received.is_empty(),
            "should have received at least the snapshot"
        );
        assert!(
            matches!(
                &received[0].payload,
                ExchangeEvent::Binance(BinanceWssEvent::DepthSnapshot(_))
            ),
            "first output should be the synthesised snapshot"
        );

        // After snapshot, the stale deltas (u <= 100) should be discarded.
        // Only the valid deltas (u=105, u=110) should follow.
        let delta_count = received
            .iter()
            .skip(1)
            .filter(|m| {
                matches!(
                    &m.payload,
                    ExchangeEvent::Binance(BinanceWssEvent::DepthUpdate(_))
                )
            })
            .count();
        assert_eq!(delta_count, 2, "only 2 valid deltas should be forwarded");

        drop(in_tx);
        let _ = handle.await;
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_stale_deltas_discarded() {
        use aetelier_connect::sources::binance::rest::BinanceRestClient;
        use aetelier_connect::workers::pipeline::book_initializer::BookInitializer;

        // Snapshot lastUpdateId = 200
        let (server_handle, base_url) = start_mock_server(200).await;

        let rest = BinanceRestClient::new(base_url);
        let initializer = BookInitializer::new(
            rest,
            "BTCUSDT".to_string(),
            "orderbook.50.BTCUSDT".to_string(),
        );

        let (in_tx, in_rx) = mpsc::channel(64);
        let (out_tx, mut out_rx) = mpsc::channel(64);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let handle = tokio::spawn(Box::new(initializer).run(in_rx, out_tx, shutdown_rx));

        // Send only stale deltas (all u <= 200)
        in_tx
            .send(make_depth_update_msg(180, 190, "orderbook.50.BTCUSDT"))
            .await
            .unwrap();
        in_tx
            .send(make_depth_update_msg(191, 200, "orderbook.50.BTCUSDT"))
            .await
            .unwrap();

        // Wait for snapshot
        let mut received = Vec::new();
        for _ in 0..5 {
            match tokio::time::timeout(Duration::from_secs(3), out_rx.recv()).await {
                Ok(Some(msg)) => received.push(msg),
                _ => break,
            }
        }

        // Only the snapshot should come through — all deltas were stale
        assert_eq!(received.len(), 1, "only snapshot, no stale deltas");
        assert!(
            matches!(
                &received[0].payload,
                ExchangeEvent::Binance(BinanceWssEvent::DepthSnapshot(_))
            ),
            "only output should be the snapshot"
        );

        drop(in_tx);
        let _ = handle.await;
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_synced_state_forwards_directly() {
        use aetelier_connect::sources::binance::rest::BinanceRestClient;
        use aetelier_connect::workers::pipeline::book_initializer::BookInitializer;

        let (server_handle, base_url) = start_mock_server(50).await;

        let rest = BinanceRestClient::new(base_url);
        let initializer = BookInitializer::new(
            rest,
            "BTCUSDT".to_string(),
            "orderbook.50.BTCUSDT".to_string(),
        );

        let (in_tx, in_rx) = mpsc::channel(64);
        let (out_tx, mut out_rx) = mpsc::channel(64);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let handle = tokio::spawn(Box::new(initializer).run(in_rx, out_tx, shutdown_rx));

        // Await the snapshot with a bounded poll instead of a fixed sleep:
        // retry a capped number of short-timeout receives so the test tolerates
        // a slow snapshot fetch without racing a hardcoded delay. The snapshot
        // is the first output the initializer emits once the fetch completes.
        let mut snap = None;
        for _ in 0..30 {
            match tokio::time::timeout(Duration::from_millis(100), out_rx.recv()).await {
                Ok(Some(msg)) => {
                    snap = Some(msg);
                    break;
                }
                Ok(None) => break,  // channel closed unexpectedly
                Err(_) => continue, // timed out this round; poll again
            }
        }
        let snap = snap.expect("should receive snapshot within bounded polling window");
        assert!(matches!(
            &snap.payload,
            ExchangeEvent::Binance(BinanceWssEvent::DepthSnapshot(_))
        ));

        // Now send post-reconciliation deltas — they should forward immediately
        let delta1 = make_depth_update_msg(51, 55, "orderbook.50.BTCUSDT");
        in_tx.send(delta1).await.unwrap();

        let delta2 = make_depth_update_msg(56, 60, "orderbook.50.BTCUSDT");
        in_tx.send(delta2).await.unwrap();

        // Both should arrive quickly
        let msg1 = tokio::time::timeout(Duration::from_secs(2), out_rx.recv())
            .await
            .expect("delta1 should arrive")
            .unwrap();
        let msg2 = tokio::time::timeout(Duration::from_secs(2), out_rx.recv())
            .await
            .expect("delta2 should arrive")
            .unwrap();

        assert!(matches!(
            &msg1.payload,
            ExchangeEvent::Binance(BinanceWssEvent::DepthUpdate(_))
        ));
        assert!(matches!(
            &msg2.payload,
            ExchangeEvent::Binance(BinanceWssEvent::DepthUpdate(_))
        ));

        drop(in_tx);
        let _ = handle.await;
        server_handle.abort();
    }
}
