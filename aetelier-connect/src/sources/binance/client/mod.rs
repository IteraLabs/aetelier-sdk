//! Binance spot public WebSocket client.
//!
//! [`BinanceWssClient`] handles Binance-specific connection, subscription
//! framing, and heartbeat protocol. Message decoding is delegated to
//! [`BinanceDecoder`] via the [`WssDecoder`] trait.
//!
//! Public market data streams do **not** require authentication.

#![allow(deprecated)] // this module defines the deprecated legacy WssClient; its own impls/tests reference it

use crate::{
    clients::disconnect::WssExitReason, clients::wss::WssDecoder,
    sources::binance::decoder::BinanceDecoder, sources::binance::events::BinanceWssEvent,
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

/// Base endpoint for Binance spot public WebSocket streams.
const BINANCE_WSS_URL: &str = "wss://stream.binance.com:9443/ws";

/// Binance sends ping every 20s; we must pong within 60s.
/// We also send unsolicited pings as keep-alive.
const KEEPALIVE_INTERVAL_SECS: u64 = 15;

/// Binance spot public WebSocket client.
///
/// Subscribes to the specified streams for a single symbol and
/// decodes incoming messages through [`BinanceDecoder`].
#[deprecated(
    since = "0.0.11",
    note = "legacy raw-ingestion API superseded by the framework engine; use MarketWorker or DataWorker with framework_ingest = true."
)]
pub struct BinanceWssClient {
    base_url: String,
    /// Stream names to subscribe to (e.g. `["btcusdt@depth@100ms", "btcusdt@trade"]`).
    streams: Vec<String>,
}

impl BinanceWssClient {
    /// Create a new client for the given stream names.
    pub fn new(streams: Vec<String>) -> Self {
        Self {
            base_url: BINANCE_WSS_URL.to_string(),
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
    /// terminated.
    pub async fn receive_data(&self, tx: mpsc::Sender<BinanceWssEvent>) -> WssExitReason {
        let url = match Url::parse(&self.base_url) {
            Ok(u) => u,
            Err(e) => return WssExitReason::ConnectionFailed(format!("URL parse: {e}")),
        };
        let (ws_stream, _) = match connect_async(url.as_str()).await {
            Ok(s) => s,
            Err(e) => return WssExitReason::Transport(e),
        };
        info!("WebSocket connected to Binance");

        let (writer_half, mut reader_half) = ws_stream.split();
        let writer = Arc::new(Mutex::new(writer_half));

        // ── Subscribe via SUBSCRIBE method ────────────────────────────
        let sub_msg = serde_json::json!({
            "method": "SUBSCRIBE",
            "params": self.streams,
            "id": 1
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

        // ── Keep-alive task ───────────────────────────────────────────
        let ka_writer = writer.clone();
        let ka_handle = tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(KEEPALIVE_INTERVAL_SECS)).await;
                if ka_writer
                    .lock()
                    .await
                    .send(Message::Pong(vec![].into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        // ── Message loop ──────────────────────────────────────────────
        let pong_writer = writer.clone();
        let mut exit_reason = WssExitReason::StreamEnded;

        while let Some(msg) = reader_half.next().await {
            match msg {
                Ok(Message::Text(txt)) => match BinanceDecoder::decode(&txt) {
                    Ok(Some(event)) => {
                        if tx.send(event).await.is_err() {
                            exit_reason = WssExitReason::ReceiverDropped;
                            break;
                        }
                    }
                    Ok(None) => {}
                    Err(e) => warn!("Binance decode error: {}", e),
                },
                Ok(Message::Ping(p)) => {
                    let _ = pong_writer.lock().await.send(Message::Pong(p)).await;
                }
                Ok(Message::Close(f)) => {
                    info!("Binance server closed connection: {:?}", f);
                    exit_reason = WssExitReason::ServerClose(f);
                    break;
                }
                Err(e) => {
                    error!("Binance ws error: {}", e);
                    exit_reason = WssExitReason::Transport(e);
                    break;
                }
                _ => {}
            }
        }

        ka_handle.abort();
        exit_reason
    }
}
