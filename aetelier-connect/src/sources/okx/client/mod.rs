//! OKX V5 public WebSocket client.
//!
//! [`OkxWssClient`] handles OKX-specific connection, subscription framing,
//! and the application-level `ping`/`pong` heartbeat. Message decoding is
//! delegated to [`OkxDecoder`] via the [`WssDecoder`] trait.
//!
//! Public market-data channels (`books5`, `trades`) require **no
//! authentication**.
//!
//! Heartbeat: OKX closes the connection after 30 s of client silence and
//! expects the client to send the **literal text** `"ping"` (not a
//! WebSocket protocol ping); the server answers with literal `"pong"`.

#![allow(deprecated)] // this module defines the deprecated legacy WssClient; its own impls/tests reference it

use crate::{
    clients::{disconnect::WssExitReason, wss::WssDecoder},
    sources::okx::decoder::OkxDecoder,
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

use crate::sources::okx::events::OkxWssEvent;

/// Default OKX V5 public WebSocket endpoint (production, no auth).
const OKX_WSS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";

/// Heartbeat interval. OKX disconnects after 30 s of silence; 20 s leaves
/// margin.
const HEARTBEAT_INTERVAL_SECS: u64 = 20;

/// OKX V5 public WebSocket client.
///
/// Subscribes to the given channels for the given instrument ids and
/// decodes incoming messages through [`OkxDecoder`].
#[deprecated(
    since = "0.0.11",
    note = "legacy raw-ingestion API superseded by the framework engine; use MarketWorker or DataWorker with framework_ingest = true."
)]
pub struct OkxWssClient {
    base_url: String,
    /// Channel names to subscribe to (e.g. `["books5", "trades"]`).
    channels: Vec<String>,
    /// Instrument ids (e.g. `["BTC-USDT"]`).
    inst_ids: Vec<String>,
}

impl OkxWssClient {
    /// Create a new client for the given channels and instrument ids.
    pub fn new(channels: Vec<String>, inst_ids: Vec<String>) -> Self {
        Self {
            base_url: OKX_WSS_URL.to_string(),
            channels,
            inst_ids,
        }
    }

    /// Create a new client with a custom base URL (useful for the AWS
    /// endpoint or a mock server).
    pub fn with_url(
        base_url: impl Into<String>,
        channels: Vec<String>,
        inst_ids: Vec<String>,
    ) -> Self {
        Self {
            base_url: base_url.into(),
            channels,
            inst_ids,
        }
    }

    /// Connect, subscribe, and pump decoded events into `tx`.
    ///
    /// Returns a [`WssExitReason`] describing *why* the message loop
    /// terminated.
    pub async fn receive_data(&self, tx: mpsc::Sender<OkxWssEvent>) -> WssExitReason {
        let url = match Url::parse(&self.base_url) {
            Ok(u) => u,
            Err(e) => return WssExitReason::ConnectionFailed(format!("URL parse: {e}")),
        };
        let (ws_stream, _) = match connect_async(url.as_str()).await {
            Ok(s) => s,
            Err(e) => return WssExitReason::Transport(e),
        };
        info!("WebSocket connected to OKX");

        let (writer_half, mut reader_half) = ws_stream.split();
        let writer = Arc::new(Mutex::new(writer_half));

        // ── Subscribe (single frame, one arg per channel × instId) ───────
        let args: Vec<serde_json::Value> = self
            .channels
            .iter()
            .flat_map(|channel| {
                self.inst_ids.iter().map(move |inst_id| {
                    serde_json::json!({ "channel": channel, "instId": inst_id })
                })
            })
            .collect();

        let sub_msg = serde_json::json!({ "op": "subscribe", "args": args }).to_string();

        if let Err(e) = writer
            .lock()
            .await
            .send(Message::Text(sub_msg.into()))
            .await
        {
            return WssExitReason::Transport(e);
        }

        // ── Heartbeat task: literal text "ping" every 20s ────────────────
        let hb_writer = writer.clone();
        let hb_handle = tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(HEARTBEAT_INTERVAL_SECS)).await;
                if hb_writer
                    .lock()
                    .await
                    .send(Message::Text("ping".to_string().into()))
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
                Ok(Message::Text(txt)) => match OkxDecoder::decode(&txt) {
                    Ok(Some(event)) => {
                        if tx.send(event).await.is_err() {
                            exit_reason = WssExitReason::ReceiverDropped;
                            break;
                        }
                    }
                    Ok(None) => {} // control frame / pong / ignored channel
                    Err(e) => warn!("OKX decode error: {}", e),
                },
                Ok(Message::Ping(p)) => {
                    let _ = pong_writer.lock().await.send(Message::Pong(p)).await;
                }
                Ok(Message::Close(f)) => {
                    info!("OKX server closed connection: {:?}", f);
                    exit_reason = WssExitReason::ServerClose(f);
                    break;
                }
                Err(e) => {
                    error!("OKX ws error: {}", e);
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
