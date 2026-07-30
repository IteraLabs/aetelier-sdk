//! Gate.io V4 public WebSocket client.
//!
//! [`GateioWssClient`] handles Gate.io-specific connection, per-channel
//! subscription framing, and the application-level `spot.ping` heartbeat.
//! Message decoding is delegated to [`GateioDecoder`] via the [`WssDecoder`]
//! trait.
//!
//! Public market-data channels (`spot.order_book`, `spot.trades`) require
//! **no authentication**.

#![allow(deprecated)] // this module defines the deprecated legacy WssClient; its own impls/tests reference it

use crate::{
    clients::{disconnect::WssExitReason, wss::WssDecoder},
    sources::gateio::{decoder::GateioDecoder, tooling::snap_level},
};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::{
    sync::{Mutex, mpsc},
    time::{Duration, sleep},
};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info, warn};
use url::Url;

use crate::sources::gateio::events::GateioWssEvent;

/// Default Gate.io V4 public WebSocket endpoint (no auth).
const GATEIO_WSS_URL: &str = "wss://api.gateio.ws/ws/v4/";

/// Application-level ping interval (Gate.io's own SDKs use 5–10 s).
const PING_INTERVAL_SECS: u64 = 10;

/// Update interval for the `spot.order_book` snapshot channel.
const ORDERBOOK_INTERVAL: &str = "100ms";

/// Current Unix time in **seconds** (Gate.io's `time` envelope field).
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Gate.io V4 public WebSocket client.
///
/// Subscribes to the given channels for a single currency pair and decodes
/// incoming messages through [`GateioDecoder`].
#[deprecated(
    since = "0.0.11",
    note = "legacy raw-ingestion API superseded by the framework engine; use MarketWorker or DataWorker with framework_ingest = true."
)]
pub struct GateioWssClient {
    base_url: String,
    /// Channel names (e.g. `["spot.order_book", "spot.trades"]`).
    channels: Vec<String>,
    /// Currency pair in Gate.io format (e.g. `"BTC_USDT"`).
    pair: String,
    /// Order-book depth, snapped to a Gate.io-accepted level at subscribe time.
    ob_level: usize,
}

impl GateioWssClient {
    /// Create a new client for the given channels, pair, and book depth.
    pub fn new(channels: Vec<String>, pair: String, ob_level: usize) -> Self {
        Self {
            base_url: GATEIO_WSS_URL.to_string(),
            channels,
            pair,
            ob_level,
        }
    }

    /// Create a new client with a custom base URL (useful for a mock
    /// server).
    pub fn with_url(
        base_url: impl Into<String>,
        channels: Vec<String>,
        pair: String,
        ob_level: usize,
    ) -> Self {
        Self {
            base_url: base_url.into(),
            channels,
            pair,
            ob_level,
        }
    }

    /// Connect, subscribe, and pump decoded events into `tx`.
    ///
    /// Returns a [`WssExitReason`] describing *why* the message loop
    /// terminated.
    pub async fn receive_data(&self, tx: mpsc::Sender<GateioWssEvent>) -> WssExitReason {
        let url = match Url::parse(&self.base_url) {
            Ok(u) => u,
            Err(e) => return WssExitReason::ConnectionFailed(format!("URL parse: {e}")),
        };
        let (ws_stream, _) = match connect_async(url.as_str()).await {
            Ok(s) => s,
            Err(e) => return WssExitReason::Transport(e),
        };
        info!("WebSocket connected to Gate.io");

        let (writer_half, mut reader_half) = ws_stream.split();
        let writer = Arc::new(Mutex::new(writer_half));

        // ── Subscribe to each channel (payload arity differs per channel) ─
        let level = snap_level(self.ob_level).to_string();
        for channel in &self.channels {
            let payload = match channel.as_str() {
                "spot.order_book" => {
                    serde_json::json!([self.pair, level, ORDERBOOK_INTERVAL])
                }
                // spot.trades and any other channel: just the pair.
                _ => serde_json::json!([self.pair]),
            };
            let sub_msg = serde_json::json!({
                "time": now_secs(),
                "channel": channel,
                "event": "subscribe",
                "payload": payload,
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
        }

        // ── Heartbeat task: app-level spot.ping ──────────────────────────
        let hb_writer = writer.clone();
        let hb_handle = tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(PING_INTERVAL_SECS)).await;
                let ping = serde_json::json!({
                    "time": now_secs(),
                    "channel": "spot.ping",
                })
                .to_string();
                if hb_writer
                    .lock()
                    .await
                    .send(Message::Text(ping.into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        // ── Message loop ─────────────────────────────────────────────────
        let pong_writer = writer.clone();
        let mut exit_reason = WssExitReason::StreamEnded;

        while let Some(msg) = reader_half.next().await {
            match msg {
                Ok(Message::Text(txt)) => match GateioDecoder::decode(&txt) {
                    Ok(Some(event)) => {
                        if tx.send(event).await.is_err() {
                            exit_reason = WssExitReason::ReceiverDropped;
                            break;
                        }
                    }
                    Ok(None) => {} // ack / pong / ignored channel
                    Err(e) => warn!("Gate.io decode error: {}", e),
                },
                Ok(Message::Ping(p)) => {
                    let _ = pong_writer.lock().await.send(Message::Pong(p)).await;
                }
                Ok(Message::Close(f)) => {
                    info!("Gate.io server closed connection: {:?}", f);
                    exit_reason = WssExitReason::ServerClose(f);
                    break;
                }
                Err(e) => {
                    error!("Gate.io ws error: {}", e);
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
