//! WSS protocol seam: the per-venue behavior that varies across exchanges.
//!
//! `ProtocolHooks` captures subscribe-framing, heartbeat, control-frame echo,
//! frame encoding, and pre-connect bootstrap so the transport loop
//! (`super::transport::WssTransport`) can be shared across venues.

use crate::errors::ExchangeError;
pub use aetelier_types::config::markets::market_config::{DeclaredDatatype, DeclaredSet};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::protocol::Message;

/// Heartbeat / keep-alive strategy for a venue's public WSS connection.
///
/// Covers client timer (`WsPong`/`Text`), dynamic-timestamp pings (`Text` with
/// a closure), channel re-subscribe (`Resubscribe`, Coinbase), and passive
/// server-heartbeat (`None`, Bitfinex/Kraken — the reply side is a no-op).
#[derive(Clone)]
pub enum Heartbeat {
    /// No client-initiated heartbeat.
    None,
    /// Emit a WebSocket-protocol `Pong` on a timer (Binance).
    WsPong { every: Duration },
    /// Send an application-level text frame on a timer. The payload is
    /// computed per tick, covering dynamic-timestamp pings (Gate.io/HTX).
    Text {
        every: Duration,
        payload: Arc<dyn Fn() -> String + Send + Sync>,
    },
    /// Re-send a fixed subscription frame on a timer (Coinbase heartbeats).
    Resubscribe { every: Duration, frame: Message },
}

/// What the transport should do with an inbound control frame.
pub enum ControlAction {
    /// Not a control frame we handle — fall through to the decoder.
    Ignore,
    /// Reply with this frame (server-driven echo pings, e.g. HTX `{ping:int}`).
    Reply(Message),
}

/// Classification of an inbound frame as a subscription acknowledgement.
///
/// Venues with an explicit ack envelope implement
/// [`ProtocolHooks::classify_ack`] so a rejected subscription forces a
/// reconnect instead of presenting as a healthy-but-silent stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AckOutcome {
    /// The frame acknowledges a successful subscription.
    Accepted,
    /// The venue rejected the subscription (or reported a stream error).
    Rejected(String),
    /// Not an ack frame — fall through to control/decode handling.
    NotAck,
}

/// Transport frame encoding for a venue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameCodec {
    /// Plain UTF-8 JSON text frames.
    PlainText,
    /// GZIP-compressed binary frames (HTX).
    Gzip,
}

/// A raw inbound WSS frame payload, prior to inflation.
pub enum FramePayload<'a> {
    Text(&'a str),
    Binary(&'a [u8]),
}

impl FrameCodec {
    /// Inflate a raw inbound frame to text.
    ///
    /// `Gzip` decompresses HTX/Huobi binary frames; a control `{"ping":…}` (sent
    /// gzip-framed like any other) inflates here, then
    /// [`ProtocolHooks::on_inbound_control`] replies. A stray gzip *text* frame
    /// is treated as already-inflated (HTX only ever gzips binary frames).
    pub fn inflate(&self, payload: FramePayload<'_>) -> Result<String, ExchangeError> {
        match (self, payload) {
            (FrameCodec::PlainText, FramePayload::Text(t)) => Ok(t.to_string()),
            (FrameCodec::PlainText, FramePayload::Binary(b)) => {
                String::from_utf8(b.to_vec()).map_err(invalid_data)
            }
            (FrameCodec::Gzip, FramePayload::Binary(b)) => gunzip(b),
            (FrameCodec::Gzip, FramePayload::Text(t)) => Ok(t.to_string()),
        }
    }
}

/// GZIP-inflate `bytes` to a UTF-8 string (HTX frames are gzip-compressed JSON).
fn gunzip(bytes: &[u8]) -> Result<String, ExchangeError> {
    use std::io::Read;
    let mut out = String::new();
    flate2::read::GzDecoder::new(bytes)
        .read_to_string(&mut out)
        .map_err(|e| {
            ExchangeError::IoError(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e,
            ))
        })?;
    Ok(out)
}

fn invalid_data<E>(e: E) -> ExchangeError
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    ExchangeError::IoError(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod codec_tests {
    use super::*;

    #[test]
    fn gzip_round_trips_a_json_frame() {
        use std::io::Write;
        let json =
            r#"{"ch":"market.btcusdt.mbp.150","tick":{"seqNum":2,"prevSeqNum":1}}"#;
        let mut enc =
            flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(json.as_bytes()).unwrap();
        let compressed = enc.finish().unwrap();

        let inflated = FrameCodec::Gzip
            .inflate(FramePayload::Binary(&compressed))
            .expect("gzip inflate");
        assert_eq!(inflated, json);
    }

    #[test]
    fn plaintext_binary_is_utf8_decoded() {
        let out = FrameCodec::PlainText
            .inflate(FramePayload::Binary(b"hello"))
            .unwrap();
        assert_eq!(out, "hello");
    }
}

pub const DEFAULT_STALE_AFTER: Duration = Duration::from_secs(60);

/// Result of a venue's pre-connect bootstrap (`ProtocolHooks::prepare`).
#[derive(Default)]
pub struct Prepared {
    /// Replaces the static endpoint (KuCoin dynamic URL).
    pub endpoint_override: Option<String>,
    /// Replaces the heartbeat (KuCoin server-provided cadence).
    pub heartbeat_override: Option<Heartbeat>,
    /// Frames to send immediately after the subscribe frames (HTX REQ seed).
    pub extra_frames: Vec<Message>,
    pub extra_frames_delay: Option<Duration>,
}

/// The per-venue WSS protocol behavior. Implementing this + a `WssDecoder`
/// is (almost) all a new public-LOB+Trades venue needs to write.
#[async_trait::async_trait]
pub trait ProtocolHooks: Send + Sync + 'static {
    /// Base WSS endpoint. May be overridden at runtime by [`prepare`](Self::prepare).
    fn endpoint(&self) -> String;

    /// Frames to subscribe a SET of symbols on one socket. Returning a `Vec`
    /// lets venues that need one-frame-per-topic (HTX/Bitfinex) emit many.
    fn subscribe_frames(
        &self,
        symbols: &[String],
        declared: &DeclaredSet,
    ) -> Vec<Message>;

    fn channel_map(&self) -> &'static [(DeclaredDatatype, &'static str)] {
        &[]
    }

    /// Heartbeat strategy. Default: passive (no client heartbeat).
    fn heartbeat(&self) -> Heartbeat {
        Heartbeat::None
    }

    /// React to an inbound control frame (already inflated to text). Default:
    /// ignore (fall through to the decoder).
    fn on_inbound_control(&self, _text: &str) -> ControlAction {
        ControlAction::Ignore
    }

    /// Classify a frame as a subscription ack. Venues with an explicit ack
    /// envelope (Bybit `success`, OKX `event`, Binance `result`/`error`)
    /// override this so a rejection surfaces as a connection failure. Default:
    /// not an ack (no behavior change for venues without an envelope).
    fn classify_ack(&self, _text: &str) -> AckOutcome {
        AckOutcome::NotAck
    }

    fn subscribe_ack_deadline(&self) -> Option<Duration> {
        None
    }

    fn unsubscribe_frames(
        &self,
        _symbols: &[String],
        _declared: &DeclaredSet,
    ) -> Vec<Message> {
        Vec::new()
    }

    fn stale_after(&self) -> Duration {
        DEFAULT_STALE_AFTER
    }

    /// Transport frame encoding. Default: plain text JSON.
    fn frame_codec(&self) -> FrameCodec {
        FrameCodec::PlainText
    }

    /// Optional pre-subscribe bootstrap (token fetch, login, dynamic URL).
    /// Default: no-op for the public-open majority.
    async fn prepare(&self) -> Result<Prepared, ExchangeError> {
        Ok(Prepared::default())
    }
}
