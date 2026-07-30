//! Bybit-specific WebSocket client.
//!
//! [`BybitWssClient`] is a thin, exchange-aware wrapper that handles
//! Bybit's connection, subscription framing, heartbeat protocol, and
//! delegates all message decoding to [`BybitDecoder`] via the [`WssDecoder`]
//! trait.
//!
//! This is the single entry point for Bybit WSS I/O. All other code
//! (workers, examples) should use this client — never raw `WssClient<D>`.

#![allow(deprecated)] // this module defines the deprecated legacy WssClient; its own impls/tests reference it

use crate::{
    clients::{disconnect::WssExitReason, wss::WssDecoder},
    sources::bybit::decoder::BybitDecoder,
};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::{
    sync::{Mutex, mpsc},
    time::{Duration, sleep},
};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info, warn};
use url::Url;

use crate::sources::bybit::events::BybitWssEvent;

/// Default Bybit linear-perpetual public WebSocket endpoint.
const BYBIT_WSS_URL: &str = "wss://stream.bybit.com/v5/public/linear";

/// Heartbeat interval matching Bybit's 30-second timeout with margin.
const HEARTBEAT_INTERVAL_SECS: u64 = 25;

/// Bybit WebSocket client.
///
/// Connects to Bybit's public linear WSS endpoint, subscribes to the
/// requested streams, maintains a heartbeat, and decodes incoming
/// messages through [`BybitDecoder`] (implementing [`WssDecoder`]).
#[deprecated(
    since = "0.0.11",
    note = "legacy raw-ingestion API superseded by the framework engine; use MarketWorker or DataWorker with framework_ingest = true."
)]
pub struct BybitWssClient {
    base_url: String,
    streams: Vec<String>,
}

impl BybitWssClient {
    /// Create a new client for the given stream topics.
    ///
    /// Each stream should be a fully-qualified Bybit topic, e.g.
    /// `"orderbook.50.BTCUSDT"`, `"publicTrade.ETHUSDT"`.
    pub fn new(streams: Vec<String>) -> Self {
        Self {
            base_url: BYBIT_WSS_URL.to_string(),
            streams,
        }
    }

    /// Create a new client with a custom base URL (useful for testnet).
    pub fn with_url(base_url: impl Into<String>, streams: Vec<String>) -> Self {
        Self {
            base_url: base_url.into(),
            streams,
        }
    }

    /// Connect, subscribe, and pump decoded events into `tx`.
    ///
    /// Returns a [`WssExitReason`] describing *why* the message loop
    /// terminated — the caller (typically [`DataWorker`](crate::workers::DataWorker)) converts this
    /// into a [`DisconnectReason`](crate::clients::disconnect::DisconnectReason) for the reconnection policy.
    pub async fn receive_data(&self, tx: mpsc::Sender<BybitWssEvent>) -> WssExitReason {
        let url = match Url::parse(&self.base_url) {
            Ok(u) => u,
            Err(e) => return WssExitReason::ConnectionFailed(format!("URL parse: {e}")),
        };
        let (ws_stream, _) = match connect_async(url.as_str()).await {
            Ok(s) => s,
            Err(e) => return WssExitReason::Transport(e),
        };
        info!("WebSocket connected to Bybit");

        let (writer_half, mut reader_half) = ws_stream.split();
        let writer = Arc::new(Mutex::new(writer_half));

        // ── Subscribe ────────────────────────────────────────────────
        let sub_msg = serde_json::json!({
            "op": "subscribe",
            "args": self.streams
        })
        .to_string();

        if let Err(e) = writer
            .lock()
            .await
            .send(Message::Text(sub_msg.into()))
            .await
        {
            return WssExitReason::Transport(e);
        }

        // ── Heartbeat task ───────────────────────────────────────────
        let hb_writer = writer.clone();
        let hb_handle = tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(HEARTBEAT_INTERVAL_SECS)).await;
                if hb_writer
                    .lock()
                    .await
                    .send(Message::Text(r#"{"op":"ping"}"#.into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        // ── Message loop ─────────────────────────────────────────────
        let pong_writer = writer.clone();
        let mut exit_reason = WssExitReason::StreamEnded;

        while let Some(msg) = reader_half.next().await {
            match msg {
                Ok(Message::Text(txt)) => {
                    // Respond to server-initiated pings
                    if txt.contains(r#""op":"ping""#) {
                        let _ = pong_writer
                            .lock()
                            .await
                            .send(Message::Text(r#"{"op":"pong"}"#.into()))
                            .await;
                        continue;
                    }

                    // Unified decode via WssDecoder trait
                    match BybitDecoder::decode(&txt) {
                        Ok(Some(event)) => {
                            if tx.send(event).await.is_err() {
                                exit_reason = WssExitReason::ReceiverDropped;
                                break;
                            }
                        }
                        Ok(None) => {} // non-data frame (subscription ack, pong, etc.)
                        Err(e) => warn!("Decode error: {}", e),
                    }
                }
                Ok(Message::Ping(p)) => {
                    let _ = pong_writer.lock().await.send(Message::Pong(p)).await;
                }
                Ok(Message::Close(f)) => {
                    info!("Bybit server closed connection: {:?}", f);
                    exit_reason = WssExitReason::ServerClose(f);
                    break;
                }
                Err(e) => {
                    error!("Bybit ws error: {}", e);
                    exit_reason = WssExitReason::Transport(e);
                    break;
                }
                _ => {}
            }
        }

        hb_handle.abort();
        exit_reason
    }
}
