use std::fmt;
use std::time::Duration;
use tokio_tungstenite::tungstenite;

// ─────────────────────────────────────────────────────────────────────────────
// Disconnect classification
// ─────────────────────────────────────────────────────────────────────────────

/// Why a WebSocket connection terminated.
///
/// Returned by `WssClient::run()` (and exchange-specific `receive_data()`)
/// so the caller can make an informed retry decision via [`ReconnectPolicy`](crate::clients::reconnect::ReconnectPolicy).
#[derive(Debug)]
pub enum DisconnectReason {
    /// Server sent Close code 1000 — normal shutdown.
    ///
    /// Typically safe to retry immediately (the server closed gracefully).
    CleanClose,

    /// Server sent Close code 1001 — going away ().
    ///
    /// Retry with standard backoff; typically a maintenance, or deploy
    /// is happening, the server is expected to return.
    GoingAway { reason: String },

    /// Server explicitly rejected the connection: Close code 1008 (policy),
    /// 1003 (unsupported data), or a genuine 1002 (protocol error such as
    /// invalid frames or unsupported extensions).
    ///
    /// **Do not retry** unless the root cause is fixed (e.g. bad subscription
    /// payload, authentication failure).
    ///
    /// Note: a *connection reset without closing handshake* (often surfaced as
    /// tungstenite `Protocol` error with code 1002) is **not** a real protocol
    /// rejection — it is reclassified as [`TransportError`](Self::TransportError)
    /// by [`classify_tungstenite_error`] / [`classify_close_frame`].
    ProtocolRejection { code: u16, reason: String },

    /// Network-level failure: TCP reset, DNS resolution error, TLS handshake
    /// failure, or unexpected stream termination.
    ///
    /// Retry with backoff.
    TransportError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// No message (Text, Ping, or Pong) received within the configured
    /// staleness timeout.
    ///
    /// The connection is likely dead but the server never sent a Close frame.
    /// Retry with backoff.
    StaleConnection { silence_duration: Duration },

    /// The `mpsc::Sender` failed because the receiver was dropped — this is
    /// an intentional shutdown signal from the caller.
    ///
    /// **Do not retry.**
    ReceiverDropped,

    /// Writing a Pong (or application-level heartbeat) to the WebSocket failed.
    ///
    /// The write half is broken; retry with backoff.
    HeartbeatFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl DisconnectReason {
    /// Whether this disconnection reason warrants a reconnection attempt.
    ///
    /// Returns `false` for [`ProtocolRejection`](Self::ProtocolRejection) and
    /// [`ReceiverDropped`](Self::ReceiverDropped) — these indicate either a
    /// server-side rejection or an intentional local shutdown.
    pub fn is_retryable(&self) -> bool {
        !matches!(self, Self::ProtocolRejection { .. } | Self::ReceiverDropped)
    }

    /// Multiplier applied to the base backoff delay for this reason.
    ///
    /// - `0.0` — retry immediately (no delay).
    /// - `1.0` — standard exponential backoff.
    /// - `2.0` — more aggressive backoff (transport errors deserve patience).
    pub fn suggested_delay_factor(&self) -> f64 {
        match self {
            Self::CleanClose => 0.0,
            Self::GoingAway { .. } => 1.0,
            Self::ProtocolRejection { .. } => 0.0, // won't be used (not retryable)
            Self::TransportError { .. } => 2.0,
            Self::StaleConnection { .. } => 1.0,
            Self::ReceiverDropped => 0.0, // won't be used (not retryable)
            Self::HeartbeatFailed { .. } => 1.0,
        }
    }
}

impl fmt::Display for DisconnectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CleanClose => write!(f, "server closed normally (1000)"),
            Self::GoingAway { reason } => write!(f, "server going away (1001): {reason}"),
            Self::ProtocolRejection { code, reason } => {
                write!(f, "protocol rejection ({code}): {reason}")
            }
            Self::TransportError { source } => write!(f, "transport error: {source}"),
            Self::StaleConnection {
                silence_duration: d,
            } => {
                write!(f, "stale connection (no data for {d:?})")
            }
            Self::ReceiverDropped => write!(f, "receiver dropped (intentional shutdown)"),
            Self::HeartbeatFailed { source } => {
                write!(f, "heartbeat write failed: {source}")
            }
        }
    }
}

// ── Close-frame classification ───────────────────────────────────────────────

/// Classify a WebSocket `CloseFrame` into a [`DisconnectReason`].
///
/// The `frame` is `Option<CloseFrame>` because the tungstenite
/// `Message::Close` variant carries an optional payload.
///
/// # Close-code mapping
///
/// | Code | Name | Action |
/// |------|------|--------|
/// | 1000 | Normal | `CleanClose` — retry immediately |
/// | 1001 | Going Away | `GoingAway` — backoff + retry |
/// | 1002 | Protocol Error | `TransportError` if reason contains "reset" / "Connection reset"; `ProtocolRejection` otherwise |
/// | 1003 | Unsupported Data | `ProtocolRejection` — do not retry |
/// | 1008 | Policy Violation | `ProtocolRejection` — do not retry |
/// | other | — | `TransportError` — backoff + retry |
///
pub fn classify_close_frame(
    frame: Option<tokio_tungstenite::tungstenite::protocol::CloseFrame>,
) -> DisconnectReason {
    let Some(frame) = frame else {
        // No close frame at all — treat as unexpected transport loss.
        return DisconnectReason::TransportError {
            source: "server closed without a close frame".into(),
        };
    };

    let code = frame.code;
    let reason = frame.reason.to_string();

    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    match code {
        CloseCode::Normal => DisconnectReason::CleanClose,
        CloseCode::Away => DisconnectReason::GoingAway { reason },
        CloseCode::Protocol => {
            // 1002 is ambiguous: it can signal a genuine protocol error
            // *or* a TCP reset that prevented the close handshake.
            // Inspect the reason text to decide.
            if reason.contains("reset without closing")
                || reason.contains("Connection reset")
            {
                DisconnectReason::TransportError {
                    source: format!("server closed with code 1002: {reason}").into(),
                }
            } else {
                DisconnectReason::ProtocolRejection {
                    code: code.into(),
                    reason,
                }
            }
        }
        CloseCode::Unsupported | CloseCode::Policy => {
            DisconnectReason::ProtocolRejection {
                code: code.into(),
                reason,
            }
        }
        _ => DisconnectReason::TransportError {
            source: format!("server closed with code {}: {}", u16::from(code), reason)
                .into(),
        },
    }
}

/// Classify a raw `tungstenite::Error` into a [`DisconnectReason`].
///
/// Most variants map to [`TransportError`](DisconnectReason::TransportError).
/// Only [`tungstenite::Error::Protocol`] errors that indicate a genuine
/// server-side rejection (unsupported extensions, invalid frames, etc.)
/// become [`ProtocolRejection`](DisconnectReason::ProtocolRejection).
///
/// Notably, `"Connection reset without closing handshake"` is a
/// **transport-level** event (the peer dropped the TCP connection before
/// sending a WS Close frame) and is therefore retryable.
pub fn classify_tungstenite_error(
    error: tokio_tungstenite::tungstenite::Error,
) -> DisconnectReason {
    use tokio_tungstenite::tungstenite::Error as TError;

    if let TError::Protocol(ref msg) = error {
        let msg_str = msg.to_string();
        // "Connection reset without closing handshake" is a network-level
        // event — the peer dropped the TCP socket.  Retryable.
        if msg_str.contains("reset without closing")
            || msg_str.contains("Connection reset")
        {
            return DisconnectReason::TransportError {
                source: Box::new(error),
            };
        }
        // Any other Protocol error is a genuine server-side rejection.
        return DisconnectReason::ProtocolRejection {
            code: 1002,
            reason: msg_str,
        };
    }

    // Io, Tls, Http, Url, Capacity, WriteBufferFull, Utf8, etc.
    DisconnectReason::TransportError {
        source: Box::new(error),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared client exit reason
// ─────────────────────────────────────────────────────────────────────────────

/// Why a WebSocket client's message loop exited.
///
/// This is the **shared return type** for all exchange client
/// `receive_data()` methods.  It captures the raw exit cause before
/// any reconnection-policy interpretation — that mapping happens via
/// the [`From<WssExitReason>`] impl for [`DisconnectReason`], which
/// delegates to [`classify_close_frame`] and
/// [`classify_tungstenite_error`].
///
/// # Extensibility
///
/// New exchange clients should return this type from their message
/// loop rather than inventing per-client exit handling.  The
/// [`DisconnectReason`] conversion centralises close-frame
/// classification and transport-error handling in one place.
///
/// # Example
///
/// ```rust,ignore
/// use aetelier_connect::clients::disconnect::WssExitReason;
///
/// pub async fn receive_data(&self, tx: Sender<MyEvent>) -> WssExitReason {
///     let (ws_stream, _) = match connect_async(url).await {
///         Ok(s) => s,
///         Err(e) => return WssExitReason::Transport(e),
///     };
///     // … message loop …
///     // On server close:
///     //     return WssExitReason::ServerClose(frame);
///     // On transport error:
///     //     return WssExitReason::Transport(e);
///     WssExitReason::StreamEnded
/// }
/// ```
#[derive(Debug)]
pub enum WssExitReason {
    /// The WebSocket read stream ended (reader returned `None`).
    ///
    /// Typically means the server closed the TCP connection without
    /// sending a Close frame.
    StreamEnded,

    /// The `mpsc::Sender` failed — the caller dropped the receiver.
    ///
    /// This is an intentional shutdown signal from the DataWorker.
    ReceiverDropped,

    /// The server sent a WebSocket Close frame.
    ///
    /// The optional `CloseFrame` carries the close code and reason
    /// string.  Converted to the appropriate [`DisconnectReason`]
    /// variant via [`classify_close_frame`].
    ServerClose(Option<tungstenite::protocol::CloseFrame>),

    /// A transport-level error occurred while reading from the socket.
    ///
    /// TCP reset, TLS alert, unexpected EOF, protocol violation, etc.
    Transport(tungstenite::Error),

    /// Writing a Pong or application-level heartbeat message failed.
    ///
    /// The write half of the socket is broken.
    HeartbeatWriteFailed(tungstenite::Error),

    /// The initial connection could not be established.
    ///
    /// URL parse errors, DNS resolution failure, TLS handshake, etc.
    ConnectionFailed(String),

    /// The venue explicitly rejected the subscription ack BEFORE any data
    /// frame arrived — a deterministic protocol rejection (bad symbol or
    /// channel), never healed by retrying. Post-data venue error frames
    /// classify as [`ConnectionFailed`](Self::ConnectionFailed) instead.
    SubscriptionRejected(String),

    /// No frame of any kind arrived within the staleness deadline — the
    /// socket is presumed half-open (server gone, no Close frame).
    Stale { silence: Duration },
}

impl fmt::Display for WssExitReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StreamEnded => write!(f, "stream ended"),
            Self::ReceiverDropped => write!(f, "receiver dropped"),
            Self::ServerClose(frame) => match frame {
                Some(cf) => {
                    write!(f, "server close ({}): {}", u16::from(cf.code), cf.reason)
                }
                None => write!(f, "server close (no frame)"),
            },
            Self::Transport(e) => write!(f, "transport error: {e}"),
            Self::HeartbeatWriteFailed(e) => write!(f, "heartbeat write failed: {e}"),
            Self::ConnectionFailed(msg) => write!(f, "connection failed: {msg}"),
            Self::SubscriptionRejected(msg) => {
                write!(f, "subscription rejected: {msg}")
            }
            Self::Stale { silence } => {
                write!(f, "stale connection (no frames for {silence:?})")
            }
        }
    }
}

impl From<WssExitReason> for DisconnectReason {
    fn from(reason: WssExitReason) -> Self {
        match reason {
            WssExitReason::StreamEnded => classify_close_frame(None),
            WssExitReason::ReceiverDropped => DisconnectReason::ReceiverDropped,
            WssExitReason::ServerClose(frame) => classify_close_frame(frame),
            WssExitReason::Transport(e) => classify_tungstenite_error(e),
            WssExitReason::HeartbeatWriteFailed(e) => DisconnectReason::HeartbeatFailed {
                source: Box::new(e),
            },
            WssExitReason::ConnectionFailed(msg) => {
                DisconnectReason::TransportError { source: msg.into() }
            }
            // code 0 = application-level rejection (no WS close code); flows
            // the non-retryable GiveUp path so the worker stops loudly and
            // the feed set records Rejected.
            WssExitReason::SubscriptionRejected(reason) => {
                DisconnectReason::ProtocolRejection { code: 0, reason }
            }
            WssExitReason::Stale { silence } => DisconnectReason::StaleConnection {
                silence_duration: silence,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_tungstenite::tungstenite::Error as TError;
    use tokio_tungstenite::tungstenite::error::ProtocolError;
    use tokio_tungstenite::tungstenite::protocol::CloseFrame;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    /// Build a `CloseFrame` with the given code and reason text.
    fn close_frame(code: CloseCode, reason: &'static str) -> CloseFrame {
        CloseFrame {
            code,
            reason: reason.into(),
        }
    }

    // ── classify_close_frame: exact close-code mapping ──────────────────────

    #[test]
    fn close_code_1000_normal_is_clean_close() {
        let reason = classify_close_frame(Some(close_frame(CloseCode::Normal, "bye")));
        assert!(matches!(reason, DisconnectReason::CleanClose));
        assert!(reason.is_retryable());
    }

    #[test]
    fn close_code_1001_away_is_going_away_with_reason() {
        let reason =
            classify_close_frame(Some(close_frame(CloseCode::Away, "maintenance")));
        assert!(matches!(
            &reason,
            DisconnectReason::GoingAway { reason: r } if r == "maintenance"
        ));
        assert!(reason.is_retryable());
    }

    #[test]
    fn close_code_1002_protocol_without_reset_is_protocol_rejection_1002() {
        let reason =
            classify_close_frame(Some(close_frame(CloseCode::Protocol, "invalid frame")));
        assert!(matches!(
            &reason,
            DisconnectReason::ProtocolRejection { code: 1002, reason: r }
                if r == "invalid frame"
        ));
        assert!(!reason.is_retryable());
    }

    #[test]
    fn close_code_1002_protocol_with_reset_text_is_transport_error() {
        // 1002 whose reason text signals a TCP reset is reclassified as a
        // retryable transport error rather than a protocol rejection.
        let reason = classify_close_frame(Some(close_frame(
            CloseCode::Protocol,
            "Connection reset without closing handshake",
        )));
        assert!(matches!(reason, DisconnectReason::TransportError { .. }));
        assert!(reason.is_retryable());
    }

    #[test]
    fn close_code_1003_unsupported_is_protocol_rejection_1003() {
        let reason = classify_close_frame(Some(close_frame(
            CloseCode::Unsupported,
            "binary unsupported",
        )));
        assert!(matches!(
            &reason,
            DisconnectReason::ProtocolRejection { code: 1003, reason: r }
                if r == "binary unsupported"
        ));
        assert!(!reason.is_retryable());
    }

    #[test]
    fn close_code_1008_policy_is_protocol_rejection_1008() {
        let reason = classify_close_frame(Some(close_frame(
            CloseCode::Policy,
            "policy violation",
        )));
        assert!(matches!(
            &reason,
            DisconnectReason::ProtocolRejection { code: 1008, reason: r }
                if r == "policy violation"
        ));
        assert!(!reason.is_retryable());
    }

    #[test]
    fn close_frame_absent_is_transport_error() {
        let reason = classify_close_frame(None);
        assert!(matches!(reason, DisconnectReason::TransportError { .. }));
        assert!(reason.is_retryable());
    }

    // ── WssExitReason -> DisconnectReason: remaining variants ───────────────

    #[test]
    fn stream_ended_maps_to_transport_error() {
        let reason: DisconnectReason = WssExitReason::StreamEnded.into();
        assert!(matches!(reason, DisconnectReason::TransportError { .. }));
        assert!(reason.is_retryable());
    }

    #[test]
    fn receiver_dropped_maps_to_receiver_dropped_and_is_not_retryable() {
        let reason: DisconnectReason = WssExitReason::ReceiverDropped.into();
        assert!(matches!(reason, DisconnectReason::ReceiverDropped));
        assert!(!reason.is_retryable());
    }

    #[test]
    fn server_close_delegates_to_close_frame_classification() {
        let reason: DisconnectReason =
            WssExitReason::ServerClose(Some(close_frame(CloseCode::Normal, ""))).into();
        assert!(matches!(reason, DisconnectReason::CleanClose));
    }

    #[test]
    fn heartbeat_write_failed_maps_to_heartbeat_failed() {
        let err = TError::Protocol(ProtocolError::ResetWithoutClosingHandshake);
        let reason: DisconnectReason = WssExitReason::HeartbeatWriteFailed(err).into();
        assert!(matches!(reason, DisconnectReason::HeartbeatFailed { .. }));
        assert!(reason.is_retryable());
    }

    #[test]
    fn stale_maps_to_stale_connection_preserving_silence() {
        let silence = Duration::from_secs(30);
        let reason: DisconnectReason = WssExitReason::Stale { silence }.into();
        assert!(matches!(
            reason,
            DisconnectReason::StaleConnection { silence_duration }
                if silence_duration == Duration::from_secs(30)
        ));
        assert!(reason.is_retryable());
    }

    #[test]
    fn transport_reset_error_stays_retryable_transport_error() {
        // A raw tungstenite reset error is a network event, not a rejection.
        let err = TError::Protocol(ProtocolError::ResetWithoutClosingHandshake);
        let reason: DisconnectReason = WssExitReason::Transport(err).into();
        assert!(matches!(reason, DisconnectReason::TransportError { .. }));
        assert!(reason.is_retryable());
    }

    #[test]
    fn transport_genuine_protocol_error_is_protocol_rejection_1002() {
        let err = TError::Protocol(ProtocolError::UnexpectedContinueFrame);
        let reason: DisconnectReason = WssExitReason::Transport(err).into();
        assert!(matches!(
            reason,
            DisconnectReason::ProtocolRejection { code: 1002, .. }
        ));
        assert!(!reason.is_retryable());
    }

    #[test]
    fn subscription_rejection_maps_to_a_non_retryable_protocol_rejection() {
        let reason: DisconnectReason =
            WssExitReason::SubscriptionRejected("bad symbol".into()).into();
        assert!(matches!(
            &reason,
            DisconnectReason::ProtocolRejection { code: 0, reason: r } if r == "bad symbol"
        ));
        assert!(!reason.is_retryable());
    }

    #[test]
    fn post_data_stream_errors_stay_retryable() {
        let reason: DisconnectReason =
            WssExitReason::ConnectionFailed("stream error after data: rate limit".into())
                .into();
        assert!(matches!(reason, DisconnectReason::TransportError { .. }));
        assert!(reason.is_retryable());
    }
}
