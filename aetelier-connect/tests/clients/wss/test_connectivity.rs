//! Integration tests for [`WssClient`] WebSocket connectivity.
//!
//! Each test spins up a local `TcpListener` that upgrades to WebSocket,
//! acting as a mock exchange server.  A purpose-built [`WssDecoder`]
//! impl controls how frames are interpreted, letting each test exercise
//! a different branch of the `WssClient::run()` event loop.
//!
//! **No real exchange traffic is generated** — all connections stay on
//! `127.0.0.1`.

#[cfg(test)]
mod tests {
    use aetelier_connect::clients::wss::wss_client::{WssClientBuilder, WssDecoder};
    use aetelier_connect::errors::ExchangeError;
    use futures_util::{SinkExt, StreamExt};
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio_tungstenite::tungstenite::protocol::Message;

    // ── Test decoders ─────────────────────────────────────────────────

    /// Passthrough decoder: yields every text frame as a `String` event.
    #[derive(Clone)]
    struct PassthroughDecoder;

    #[async_trait::async_trait]
    impl WssDecoder for PassthroughDecoder {
        type Event = String;

        fn decode(text: &str) -> Result<Option<Self::Event>, Box<ExchangeError>> {
            Ok(Some(text.to_string()))
        }
    }

    /// Filtering decoder: returns `Ok(None)` for frames prefixed with
    /// `"ignore:"`, forwarding everything else as `Ok(Some(text))`.
    #[derive(Clone)]
    struct FilteringDecoder;

    #[async_trait::async_trait]
    impl WssDecoder for FilteringDecoder {
        type Event = String;

        fn decode(text: &str) -> Result<Option<Self::Event>, Box<ExchangeError>> {
            if text.starts_with("ignore:") {
                Ok(None)
            } else {
                Ok(Some(text.to_string()))
            }
        }
    }

    /// Faulty decoder: returns `Err(...)` for frames prefixed with
    /// `"error:"`, forwarding everything else normally.
    #[derive(Clone)]
    struct FaultyDecoder;

    #[async_trait::async_trait]
    impl WssDecoder for FaultyDecoder {
        type Event = String;

        fn decode(text: &str) -> Result<Option<Self::Event>, Box<ExchangeError>> {
            if text.starts_with("error:") {
                // Use a simple variant — the specific error type doesn't
                // matter here; we only need the Err path.
                Err(Box::new(ExchangeError::ChannelSendError))
            } else {
                Ok(Some(text.to_string()))
            }
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────

    /// Collect all events from the receiver until the channel closes or
    /// the timeout fires.  The timeout guards against tests hanging if
    /// something goes wrong; under normal operation the channel closes
    /// well before the deadline.
    async fn collect_events(
        rx: &mut mpsc::Receiver<String>,
        timeout: Duration,
    ) -> Vec<String> {
        let mut events = Vec::new();
        while let Ok(Some(event)) = tokio::time::timeout(timeout, rx.recv()).await {
            events.push(event);
        }
        events
    }

    // ── Test 1: basic text-frame pump ─────────────────────────────────

    /// Spin up a local WS server that sends 3 text frames then closes.
    /// Assert the `mpsc::Receiver` yields exactly 3 events in order.
    #[tokio::test]
    async fn test_wss_client_receives_text_frames() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // ── Mock server ──
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

            for i in 0..3 {
                ws.send(Message::Text(format!("frame-{i}").into()))
                    .await
                    .unwrap();
            }
            ws.send(Message::Close(None)).await.ok();
        });

        // ── Client under test ──
        let client = WssClientBuilder::new()
            .streams(vec!["test".into()])
            .base_url(format!("ws://{addr}"))
            .decoder(PassthroughDecoder)
            .build()
            .expect("builder should succeed");

        let (tx, mut rx) = mpsc::channel(16);
        tokio::spawn(async move { client.run(tx).await });

        let received = collect_events(&mut rx, Duration::from_secs(3)).await;
        assert_eq!(received, vec!["frame-0", "frame-1", "frame-2"]);
    }

    // ── Test 2: Ok(None) filtering ────────────────────────────────────

    /// Server sends 5 frames; decoder returns `Ok(None)` for 2 of them.
    /// Assert the receiver gets exactly 3 events.
    #[tokio::test]
    async fn test_wss_client_ignores_filtered_frames() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

            ws.send(Message::Text("data-1".to_string().into()))
                .await
                .unwrap();
            ws.send(Message::Text("ignore:skip-this".to_string().into()))
                .await
                .unwrap();
            ws.send(Message::Text("data-2".to_string().into()))
                .await
                .unwrap();
            ws.send(Message::Text("ignore:skip-this-too".to_string().into()))
                .await
                .unwrap();
            ws.send(Message::Text("data-3".to_string().into()))
                .await
                .unwrap();
            ws.send(Message::Close(None)).await.ok();
        });

        let client = WssClientBuilder::new()
            .streams(vec!["test".into()])
            .base_url(format!("ws://{addr}"))
            .decoder(FilteringDecoder)
            .build()
            .expect("builder should succeed");

        let (tx, mut rx) = mpsc::channel(16);
        tokio::spawn(async move { client.run(tx).await });

        let received = collect_events(&mut rx, Duration::from_secs(3)).await;
        assert_eq!(received.len(), 3, "only non-ignored frames should arrive");
        assert_eq!(received, vec!["data-1", "data-2", "data-3"]);
    }

    // ── Test 3: non-fatal decode errors ───────────────────────────────

    /// Decoder returns `Err(...)` for some frames.  Assert the client
    /// continues processing subsequent frames without panicking.
    #[tokio::test]
    async fn test_wss_client_handles_decode_errors_gracefully() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

            ws.send(Message::Text("good-1".to_string().into()))
                .await
                .unwrap();
            ws.send(Message::Text("error:bad-frame".to_string().into()))
                .await
                .unwrap();
            ws.send(Message::Text("good-2".to_string().into()))
                .await
                .unwrap();
            ws.send(Message::Text("error:also-bad".to_string().into()))
                .await
                .unwrap();
            ws.send(Message::Text("good-3".to_string().into()))
                .await
                .unwrap();
            ws.send(Message::Close(None)).await.ok();
        });

        let client = WssClientBuilder::new()
            .streams(vec!["test".into()])
            .base_url(format!("ws://{addr}"))
            .decoder(FaultyDecoder)
            .build()
            .expect("builder should succeed");

        let (tx, mut rx) = mpsc::channel(16);
        tokio::spawn(async move { client.run(tx).await });

        let received = collect_events(&mut rx, Duration::from_secs(3)).await;
        assert_eq!(
            received,
            vec!["good-1", "good-2", "good-3"],
            "decode errors should be non-fatal; valid frames still arrive",
        );
    }

    // ── Test 4: receiver-drop shutdown path ───────────────────────────

    /// Create the channel, drop the receiver, then call `run()`.
    /// Assert it returns `Ok(())` promptly without hanging.
    #[tokio::test]
    async fn test_wss_client_exits_on_receiver_drop() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Server: accept, send one frame, then hold the connection open.
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

            // Send a frame so the client has something to try to forward.
            ws.send(Message::Text("hello".to_string().into()))
                .await
                .unwrap();

            // Hold the connection — the client should exit on its own
            // because the receiver is dropped.
            tokio::time::sleep(Duration::from_secs(10)).await;
            let _ = ws;
        });

        let client = WssClientBuilder::new()
            .streams(vec!["test".into()])
            .base_url(format!("ws://{addr}"))
            .decoder(PassthroughDecoder)
            .build()
            .expect("builder should succeed");

        let (tx, rx) = mpsc::channel::<String>(16);
        drop(rx); // ← Drop the receiver *before* run()

        // run() should complete promptly — the first tx.send() will fail,
        // triggering the "Receiver dropped" shutdown branch.
        let result = tokio::time::timeout(Duration::from_secs(5), client.run(tx)).await;

        assert!(result.is_ok(), "run() should exit promptly, not time out");
        let inner = result.unwrap();
        assert!(inner.is_ok(), "run() returns Ok(()) on receiver-drop exit");
    }

    // ── Test 5: Ping ➜ Pong round-trip ────────────────────────────────

    /// Server sends a Ping frame, then waits for the Pong reply.
    /// Assert the server receives a Pong with the same payload.
    #[tokio::test]
    async fn test_wss_client_responds_to_ping_with_pong() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (pong_tx, mut pong_rx) = mpsc::channel::<Vec<u8>>(1);

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

            // Send a Ping with a recognisable payload.
            ws.send(Message::Ping(b"heartbeat-42".to_vec().into()))
                .await
                .unwrap();

            // Read messages until we see the Pong.
            while let Some(Ok(msg)) = ws.next().await {
                if let Message::Pong(data) = msg {
                    pong_tx.send(data.to_vec()).await.ok();
                    break;
                }
            }

            // Cleanly shut down so the client's run() returns.
            ws.send(Message::Close(None)).await.ok();
        });

        let client = WssClientBuilder::new()
            .streams(vec!["test".into()])
            .base_url(format!("ws://{addr}"))
            .decoder(PassthroughDecoder)
            .build()
            .expect("builder should succeed");

        // Keep the receiver alive so the event loop doesn't bail out
        // before processing the Ping.
        let (tx, _rx) = mpsc::channel::<String>(16);
        tokio::spawn(async move { client.run(tx).await });

        let pong_data = tokio::time::timeout(Duration::from_secs(3), pong_rx.recv())
            .await
            .expect("pong should arrive within timeout")
            .expect("pong channel should not be closed prematurely");

        assert_eq!(pong_data, b"heartbeat-42", "Pong payload must echo Ping");
    }

    // ── Test 6: server-initiated close ────────────────────────────────

    /// Server sends a Close frame immediately after handshake.
    /// Assert `run()` returns `Ok(())`.
    #[tokio::test]
    async fn test_wss_client_exits_on_server_close() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

            // Close immediately.
            ws.send(Message::Close(None)).await.ok();
        });

        let client = WssClientBuilder::new()
            .streams(vec!["test".into()])
            .base_url(format!("ws://{addr}"))
            .decoder(PassthroughDecoder)
            .build()
            .expect("builder should succeed");

        let (tx, _rx) = mpsc::channel::<String>(16);

        let result = tokio::time::timeout(Duration::from_secs(3), client.run(tx)).await;

        assert!(result.is_ok(), "run() should exit promptly on server Close");
        let inner = result.unwrap();
        assert!(inner.is_ok(), "run() returns Ok(()) on graceful close");
    }
}
