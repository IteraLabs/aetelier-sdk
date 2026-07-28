//! Coinbase Advanced Trade WebSocket client.
//!
//! [`CoinbaseWssClient`] handles Coinbase-specific connection, subscription
//! framing, and heartbeat protocol. Message decoding is delegated to
//! [`CoinbaseDecoder`] via the [`WssDecoder`] trait.
//!
//! Public market data channels (`level2`, `market_trades`) do **not**
//! require authentication on Coinbase Advanced Trade (beta).

#![allow(deprecated)] // this module defines the deprecated legacy WssClient; its own impls/tests reference it

use crate::{
    clients::{disconnect::WssExitReason, wss::WssDecoder},
    sources::coinbase::decoder::CoinbaseDecoder,
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

use crate::sources::coinbase::events::CoinbaseWssEvent;

/// Default Coinbase Advanced Trade public WebSocket endpoint.
const COINBASE_WSS_URL: &str = "wss://advanced-trade-ws.coinbase.com";

/// Heartbeat interval for Coinbase (keeps connection alive).
const HEARTBEAT_INTERVAL_SECS: u64 = 25;

/// Coinbase Advanced Trade WebSocket client.
///
/// Subscribes to the specified channels for the given product IDs
/// and decodes incoming messages through [`CoinbaseDecoder`].
#[deprecated(
    since = "0.0.11",
    note = "legacy raw-ingestion API superseded by the framework engine; use MarketWorker or DataWorker with framework_ingest = true."
)]
pub struct CoinbaseWssClient {
    base_url: String,
    /// Channel names to subscribe to (e.g. `["level2", "market_trades"]`).
    channels: Vec<String>,
    /// Product IDs (e.g. `["BTC-USD", "ETH-USD"]`).
    product_ids: Vec<String>,
}

impl CoinbaseWssClient {
    /// Create a new client for the given channels and product IDs.
    pub fn new(channels: Vec<String>, product_ids: Vec<String>) -> Self {
        Self {
            base_url: COINBASE_WSS_URL.to_string(),
            channels,
            product_ids,
        }
    }

    /// Create a new client with a custom base URL (useful for sandbox).
    pub fn with_url(
        base_url: impl Into<String>,
        channels: Vec<String>,
        product_ids: Vec<String>,
    ) -> Self {
        Self {
            base_url: base_url.into(),
            channels,
            product_ids,
        }
    }

    /// Connect, subscribe, and pump decoded events into `tx`.
    ///
    /// Returns a [`WssExitReason`] describing *why* the message loop
    /// terminated — the caller (typically [`DataWorker`](crate::workers::DataWorker)) converts this
    /// into a [`DisconnectReason`](crate::clients::disconnect::DisconnectReason) for the reconnection policy.
    pub async fn receive_data(
        &self,
        tx: mpsc::Sender<CoinbaseWssEvent>,
    ) -> WssExitReason {
        let url = match Url::parse(&self.base_url) {
            Ok(u) => u,
            Err(e) => return WssExitReason::ConnectionFailed(format!("URL parse: {e}")),
        };
        let (ws_stream, _) = match connect_async(url.as_str()).await {
            Ok(s) => s,
            Err(e) => return WssExitReason::Transport(e),
        };
        info!("WebSocket connected to Coinbase");

        let (writer_half, mut reader_half) = ws_stream.split();
        let writer = Arc::new(Mutex::new(writer_half));

        // ── Subscribe to each channel ────────────────────────────────
        for channel in &self.channels {
            let sub_msg = serde_json::json!({
                "type": "subscribe",
                "product_ids": self.product_ids,
                "channel": channel,
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

        // ── Heartbeat task ───────────────────────────────────────────
        // Coinbase uses standard WebSocket pings; we send periodic
        // subscribe to heartbeats channel to keep connection alive.
        let hb_writer = writer.clone();
        let hb_products = self.product_ids.clone();
        let hb_handle = tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(HEARTBEAT_INTERVAL_SECS)).await;
                let hb_msg = serde_json::json!({
                    "type": "subscribe",
                    "product_ids": hb_products,
                    "channel": "heartbeats",
                })
                .to_string();
                if hb_writer
                    .lock()
                    .await
                    .send(Message::Text(hb_msg.into()))
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
                Ok(Message::Text(txt)) => match CoinbaseDecoder::decode(&txt) {
                    Ok(Some(event)) => {
                        if tx.send(event).await.is_err() {
                            exit_reason = WssExitReason::ReceiverDropped;
                            break;
                        }
                    }
                    Ok(None) => {}
                    Err(e) => warn!("Coinbase decode error: {}", e),
                },
                Ok(Message::Ping(p)) => {
                    let _ = pong_writer.lock().await.send(Message::Pong(p)).await;
                }
                Ok(Message::Close(f)) => {
                    info!("Coinbase server closed connection: {:?}", f);
                    exit_reason = WssExitReason::ServerClose(f);
                    break;
                }
                Err(e) => {
                    error!("Coinbase ws error: {}", e);
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
