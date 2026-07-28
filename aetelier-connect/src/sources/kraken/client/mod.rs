//! Kraken WebSocket v2 client.
//!
//! [`KrakenWssClient`] handles Kraken-specific connection, subscription
//! framing, and heartbeat handling.  Message decoding is delegated to
//! [`KrakenDecoder`] via the [`WssDecoder`] trait.
//!
//! Public market data channels (`book`, `trade`) do **not** require
//! authentication on Kraken WebSocket v2.
//!
//! Kraken sends automatic heartbeats (~1/s) so no explicit heartbeat
//! subscription is needed (unlike Coinbase).

#![allow(deprecated)] // this module defines the deprecated legacy WssClient; its own impls/tests reference it

use crate::{
    clients::{disconnect::WssExitReason, wss::WssDecoder},
    sources::kraken::decoder::KrakenDecoder,
};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    sync::mpsc,
    time::{Duration, interval},
};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info, warn};
use url::Url;

use crate::sources::kraken::events::KrakenWssEvent;

/// Default Kraken WebSocket v2 public endpoint.
const KRAKEN_WSS_URL: &str = "wss://ws.kraken.com/v2";

/// Ping interval to keep the connection alive (Kraken recommends < 60s).
const PING_INTERVAL_SECS: u64 = 30;

/// Kraken WebSocket v2 client.
///
/// Subscribes to the specified channels for the given symbols
/// and decodes incoming messages through [`KrakenDecoder`].
#[deprecated(
    since = "0.0.11",
    note = "legacy raw-ingestion API superseded by the framework engine; use MarketWorker or DataWorker with framework_ingest = true."
)]
pub struct KrakenWssClient {
    base_url: String,
    /// Channel names to subscribe to (e.g. `["book", "trade"]`).
    channels: Vec<String>,
    /// Symbols (e.g. `["BTC/USD", "ETH/USD"]`).
    symbols: Vec<String>,
    /// Orderbook depth (only relevant for the `book` channel).
    book_depth: usize,
}

impl KrakenWssClient {
    /// Create a new client for the given channels and symbols.
    pub fn new(channels: Vec<String>, symbols: Vec<String>, book_depth: usize) -> Self {
        Self {
            base_url: KRAKEN_WSS_URL.to_string(),
            channels,
            symbols,
            book_depth,
        }
    }

    /// Create a new client with a custom base URL (useful for sandbox).
    pub fn with_url(
        base_url: impl Into<String>,
        channels: Vec<String>,
        symbols: Vec<String>,
        book_depth: usize,
    ) -> Self {
        Self {
            base_url: base_url.into(),
            channels,
            symbols,
            book_depth,
        }
    }

    /// Connect, subscribe, and pump decoded events into `tx`.
    ///
    /// Returns a [`WssExitReason`] describing *why* the message loop
    /// terminated — the caller (typically [`DataWorker`](crate::workers::DataWorker)) converts this
    /// into a [`DisconnectReason`](crate::clients::disconnect::DisconnectReason) for the reconnection policy.
    pub async fn receive_data(&self, tx: mpsc::Sender<KrakenWssEvent>) -> WssExitReason {
        let url = match Url::parse(&self.base_url) {
            Ok(u) => u,
            Err(e) => return WssExitReason::ConnectionFailed(format!("URL parse: {e}")),
        };
        let (ws_stream, _) = match connect_async(url.as_str()).await {
            Ok(s) => s,
            Err(e) => return WssExitReason::Transport(e),
        };
        info!("WebSocket connected to Kraken");

        let (mut writer, mut reader) = ws_stream.split();

        // ── Subscribe to each channel ────────────────────────────────
        for channel in &self.channels {
            let mut params = serde_json::json!({
                "channel": channel,
                "symbol": self.symbols,
            });

            // Add depth parameter for book channel
            if channel == "book" {
                params["depth"] = serde_json::json!(self.book_depth);
            }

            let sub_msg = serde_json::json!({
                "method": "subscribe",
                "params": params,
            })
            .to_string();

            if let Err(e) = writer.send(Message::Text(sub_msg.into())).await {
                return WssExitReason::Transport(e);
            }
        }

        // ── Ping task ────────────────────────────────────────────────
        // Kraken sends automatic heartbeats but we also send periodic
        // application-level pings to be safe.
        let (ping_tx, mut ping_rx) = mpsc::channel::<()>(1);
        let ping_handle = tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(PING_INTERVAL_SECS));
            loop {
                ticker.tick().await;
                if ping_tx.send(()).await.is_err() {
                    break;
                }
            }
        });

        // ── Message loop ─────────────────────────────────────────────
        let mut exit_reason = WssExitReason::StreamEnded;

        loop {
            tokio::select! {
                msg = reader.next() => {
                    match msg {
                        Some(Ok(Message::Text(txt))) => {
                            match KrakenDecoder::decode(&txt) {
                                Ok(Some(event)) => {
                                    if tx.send(event).await.is_err() {
                                        exit_reason = WssExitReason::ReceiverDropped;
                                        break;
                                    }
                                }
                                Ok(None) => {}
                                Err(e) => warn!("Kraken decode error: {}", e),
                            }
                        }
                        Some(Ok(Message::Ping(p))) => {
                            let _ = writer.send(Message::Pong(p)).await;
                        }
                        Some(Ok(Message::Close(f))) => {
                            info!("Kraken server closed connection: {:?}", f);
                            exit_reason = WssExitReason::ServerClose(f);
                            break;
                        }
                        Some(Err(e)) => {
                            error!("Kraken ws error: {}", e);
                            exit_reason = WssExitReason::Transport(e);
                            break;
                        }
                        None => break, // stream ended
                        _ => {}
                    }
                }

                _ = ping_rx.recv() => {
                    let ping_msg = serde_json::json!({
                        "method": "ping",
                    }).to_string();
                    match writer.send(Message::Text(ping_msg.into())).await {
                        Ok(()) => {}
                        Err(e) => {
                            exit_reason = WssExitReason::HeartbeatWriteFailed(e);
                            break;
                        }
                    }
                }
            }
        }

        ping_handle.abort();
        exit_reason
    }
}
