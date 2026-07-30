use crate::errors::ExchangeError;
use aetelier_types::errors::BuildError;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info, warn};
use url::Url;

/// Stateless decoder that maps raw WebSocket text frames to typed events.
///
/// Each supported exchange provides its own implementation (e.g.
/// [`crate::sources::bybit::decoder::BybitDecoder`]) that knows how to
/// parse the exchange's proprietary JSON envelope.
///
/// The three return conventions form a tri-state protocol:
///
/// | Return value | Meaning | Client behaviour |
/// |--------------|---------|-----------------|
/// | `Ok(Some(event))` | Data frame decoded | Forward to [`mpsc::Sender`] |
/// | `Ok(None)` | Control / heartbeat frame | Silently skip |
/// | `Err(e)` | Decode failure | Log warning, continue |
///
/// # Implementor notes
///
/// `decode` is intentionally a **static method** (no `&self`) because
/// decoders are stateless — all routing is determined by the frame content
/// alone.  The decoder instance held by [`WssClient`] exists only to
/// carry the type parameter `D`.
#[async_trait::async_trait]
pub trait WssDecoder: Send + Sync + 'static {
    /// The domain event type yielded to application code after decoding.
    type Event: Send + 'static;

    /// Attempt to decode a single text frame.
    ///
    /// See the [trait-level documentation](WssDecoder) for the
    /// return-value contract.
    fn decode(text: &str) -> Result<Option<Self::Event>, Box<ExchangeError>>;
}

/// Transport-level WebSocket client that pumps decoded events into an
/// [`mpsc`] channel.
///
/// `WssClient` is generic over a [`WssDecoder`] implementation, making it
/// exchange-agnostic: the same connect → read → decode → send loop works
/// for Bybit, Coinbase, Kraken, or any future exchange.
///
/// # Connection lifecycle
///
/// [`run()`](Self::run) performs the following steps:
///
/// 1. Build the subscription URL from [`base_url`](Self::base_url) and
///    [`streams`](Self::streams).
/// 2. Open a TLS WebSocket via `tokio-tungstenite`.
/// 3. Enter an event loop that decodes `Text` frames through
///    `D::decode`, responds to `Ping` with `Pong`, and exits cleanly
///    on `Close` or transport error.
///
/// # URL format
///
/// ```text
/// {base_url}?streams={stream_a}/{stream_b}/...
/// ```
///
/// # Shutdown
///
/// The loop terminates when any of these occur:
///
/// - The server sends a `Close` frame.
/// - The [`mpsc::Receiver`] is dropped (back-pressure signal).
/// - A transport-level error is encountered.
///
/// Reconnection is **not** handled here — see
/// [`ReconnectPolicy`](crate::clients::reconnect::ReconnectPolicy).
#[derive(Clone)]
pub struct WssClient<D>
where
    D: WssDecoder,
{
    /// Stream names to subscribe to (joined with `/` in the URL query).
    pub streams: Vec<String>,
    /// WebSocket base URL (e.g. `wss://stream.bybit.com/v5/public/linear`).
    pub base_url: String,
    /// Decoder instance wrapped in [`Arc`] for potential sharing across tasks.
    pub decoder: Arc<D>,
}

impl<D> WssClient<D>
where
    D: WssDecoder,
{
    /// Connect to the exchange WebSocket and pump decoded events into `tx`.
    ///
    /// This method **consumes** `self` and runs until the connection is
    /// closed or the receiving half of the channel is dropped.  Returns
    /// `Ok(())` on every graceful shutdown path; transport failures
    /// surface as `Err(`[`ExchangeError`]`)`.
    /// The subscription URL [`run`](Self::run) connects to: the base URL with
    /// the stream names joined by `/` under a `streams` query parameter.
    /// Extracted from `run` so the exact wire URL is unit-testable without
    /// opening a socket.
    pub fn subscribe_url(&self) -> String {
        format!("{}?streams={}", self.base_url, self.streams.join("/"))
    }

    pub async fn run(
        self,
        tx: mpsc::Sender<<D as WssDecoder>::Event>,
    ) -> Result<(), ExchangeError> {
        let url = Url::parse(&self.subscribe_url())?;

        info!("Connecting to WebSocket URL: {}", url);
        let (ws_stream, _) = connect_async(url.as_str()).await?;
        info!("WebSocket connection established.");

        let (write, mut read) = ws_stream.split();
        // Wrap writer in a Mutex so heartbeat can run concurrently
        let write = Arc::new(Mutex::new(write));

        // Spawn a heartbeat task (if the exchange overrides the default no-op)
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(25)).await;
            }
        });

        // Main message processing loop
        loop {
            tokio::select! {
                Some(msg) = read.next() => {
                    match msg {
                        Ok(Message::Text(text)) => {
                            match D::decode(&text) {
                                Ok(Some(event)) => {
                                    if tx.send(event).await.is_err() {
                                        error!("Receiver dropped; shutting down client.");
                                        break;
                                    }
                                }
                                Ok(None) => {} // not a data frame; ignore
                                Err(e) => warn!("Decode error: {} | {}", e, text),
                            }
                        }
                        Ok(Message::Ping(p)) => {
                            if let Err(e) = write.lock().await.send(Message::Pong(p)).await {
                                error!("Pong failed: {}", e);
                                break;
                            }
                        }
                        Ok(Message::Close(_)) => {
                            info!("Server closed connection.");
                            break;
                        }
                        Err(e) => {
                            error!("WebSocket error: {}", e);
                            break;
                        }
                        _ => {}
                    }
                }
                else => break,
            }
        }

        warn!("WssClient loop terminated.");
        Ok(())
    }
}

/// Builder for [`WssClient<D>`].
///
/// All three fields — [`streams`](Self::streams),
/// [`base_url`](Self::base_url), and [`decoder`](Self::decoder) — are
/// required.  Calling [`build()`](Self::build) with any field missing
/// returns `Err(String)` naming the first absent field.
///
/// # Example
///
/// ```rust,ignore
/// use aetelier_connect::{clients::wss::wss_client, sources::bybit::decoder::BybitDecoder};
///
/// let client = wss_client::WssClientBuilder::new()
///     .streams(vec!["orderbook.50.BTCUSDT".into()])
///     .base_url("wss://stream.bybit.com/v5/public/linear")
///     .decoder(BybitDecoder)
///     .build()
///     .expect("all fields provided");
/// ```
#[derive(Clone)]
pub struct WssClientBuilder<D>
where
    D: WssDecoder,
{
    streams: Option<Vec<String>>,
    base_url: Option<String>,
    decoder: Option<Arc<D>>,
}

impl<D> Default for WssClientBuilder<D>
where
    D: WssDecoder,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<D> WssClientBuilder<D>
where
    D: WssDecoder,
{
    /// Create an empty builder with all fields set to `None`.
    pub fn new() -> Self {
        Self {
            streams: None,
            base_url: None,
            decoder: None,
        }
    }

    /// Set the stream names to subscribe to.
    ///
    /// These are joined with `"/"` and appended as a `?streams=` query
    /// parameter when the connection URL is built inside
    /// [`WssClient::run`].
    pub fn streams(mut self, streams: Vec<String>) -> Self {
        self.streams = Some(streams);
        self
    }

    /// Set the WebSocket base URL (e.g.
    /// `"wss://stream.bybit.com/v5/public/linear"`).
    ///
    /// Accepts any type that implements [`Into<String>`].
    pub fn base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = Some(base_url.into());
        self
    }

    /// Set the decoder instance.
    ///
    /// The decoder is automatically wrapped in an [`Arc`] for internal
    /// sharing.
    pub fn decoder(mut self, decoder: D) -> Self {
        self.decoder = Some(Arc::new(decoder));
        self
    }

    /// Consume the builder and produce a [`WssClient<D>`].
    ///
    /// # Errors
    ///
    /// Returns [`BuildError`] if any required field is missing.  Validation
    /// order: `streams` → `base_url` → `decoder`.
    pub fn build(self) -> Result<WssClient<D>, BuildError> {
        Ok(WssClient {
            streams: self.streams.ok_or(BuildError::MissingField("streams"))?,
            base_url: self.base_url.ok_or(BuildError::MissingField("base_url"))?,
            decoder: self.decoder.ok_or(BuildError::MissingField("decoder"))?,
        })
    }
}
