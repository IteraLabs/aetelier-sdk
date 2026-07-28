//! Formal connection state machine for the data worker WSS lifecycle.
//!
//! [`ConnectionState`](crate::clients::connection_state::ConnectionState) models the discrete phases a WebSocket connection
//! passes through.  The [`ConnectionManager`](crate::clients::connection_manager::ConnectionManager)
//! drives transitions and emits structured tracing events at each boundary.
//!
//! # State diagram
//!
//! ```text
//! ┌──────────────┐
//! │ Disconnected │─────────────────────────────┐
//! └──────┬───────┘                             │
//!        │ start()                             │
//!        ▼                                     │
//! ┌──────────────┐                             │
//! │  Connecting  │                             │
//! └──────┬───────┘                             │
//!        │ ws_stream opened                    │
//!        ▼                                     │
//! ┌──────────────┐                             │
//! │Authenticating│  (skipped for public feeds) │
//! └──────┬───────┘                             │
//!        │ auth_ack / no-op                    │
//!        ▼                                     │
//! ┌──────────────┐                             │
//! │  Subscribing │                             │
//! └──────┬───────┘                             │
//!        │ sub_ack / first event               │
//!        ▼                                     │
//! ┌──────────────┐   disconnect   ┌───────────────┐
//! │  Streaming   │──────────────▶│  Reconnecting  │
//! └──────────────┘               └───────┬───────┘
//!        ▲                               │
//!        └───────────────────────────────┘
//!                 backoff elapsed
//! ```

use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::time::Instant;

/// Discrete connection lifecycle phase.
///
/// Each variant carries just enough context for meaningful log output
/// without heap allocation (the `attempt` counter and `reason` string
/// are emitted through the tracing span, not stored in the enum).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionState {
    /// Initial state — no connection attempt has been made yet.
    Disconnected,
    /// TCP + TLS handshake in progress.
    Connecting,
    /// Sending authentication credentials (private feeds only).
    /// Skipped for public-only subscriptions.
    Authenticating,
    /// Subscription frames have been sent; waiting for acknowledgement
    /// or the first data frame.
    Subscribing,
    /// Actively receiving data frames from the exchange.
    Streaming,
    /// Collection paused by user command; connection may remain open but
    /// events are not forwarded to sinks.
    Paused,
    /// Connection lost; waiting for the backoff delay before retrying.
    Reconnecting {
        /// 1-indexed consecutive reconnection attempt number.
        attempt: u32,
    },
}

impl ConnectionState {
    /// Whether this state represents an active, healthy connection.
    pub fn is_live(&self) -> bool {
        matches!(self, Self::Streaming)
    }

    /// Whether this state represents any kind of error condition.
    pub fn is_error(&self) -> bool {
        matches!(self, Self::Reconnecting { .. } | Self::Disconnected)
    }

    /// Whether the worker is paused.
    pub fn is_paused(&self) -> bool {
        matches!(self, Self::Paused)
    }
}

impl fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disconnected => write!(f, "disconnected"),
            Self::Connecting => write!(f, "connecting"),
            Self::Authenticating => write!(f, "authenticating"),
            Self::Subscribing => write!(f, "subscribing"),
            Self::Streaming => write!(f, "streaming"),
            Self::Paused => write!(f, "paused"),
            Self::Reconnecting { attempt } => {
                write!(f, "reconnecting(attempt={})", attempt)
            }
        }
    }
}

/// Timestamped record of a single state transition.
///
/// Stored by [`ConnectionManager`](super::connection_manager::ConnectionManager)
/// for diagnostic introspection and returned by
/// `ConnectionManager::transitions()`.
#[derive(Debug, Clone)]
pub struct StateTransition {
    /// Wall-clock timestamp (monotonic) when the transition occurred.
    pub at: Instant,
    /// State we transitioned *from*.
    pub from: ConnectionState,
    /// State we transitioned *to*.
    pub to: ConnectionState,
    /// Human-readable reason for the transition.
    pub reason: String,
    /// How long we spent in the `from` state.
    pub duration_in_prev: Duration,
}
