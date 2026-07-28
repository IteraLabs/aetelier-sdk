//! Connection lifecycle manager with state tracking and reconnection.
//!
//! [`ConnectionManager`](crate::clients::connection_manager::ConnectionManager) is a lightweight state-machine wrapper that sits
//! between the [`DataWorker`](crate::workers::data_worker::DataWorker) and
//! the exchange-specific WSS clients.  It does **not** own the WebSocket
//! connection itself — instead, it:
//!
//! 1. Tracks the current [`ConnectionState`](crate::clients::connection_state::ConnectionState) and logs every transition with
//!    a timestamp and reason via structured `tracing` events.
//! 2. Delegates reconnection decisions to [`ReconnectPolicy`](crate::clients::reconnect::ReconnectPolicy) (jittered
//!    exponential backoff + circuit breaker).
//! 3. Exposes a small diagnostic API (`state()`, `transitions()`,
//!    `consecutive_failures()`) for health reporting.
//!
//! # Usage
//!
//! The caller (typically [`DataWorker`](crate::workers::data_worker::DataWorker))
//! drives the state machine by calling `transition()` at each lifecycle
//! boundary:
//!
//! ```rust,ignore
//! manager.transition(ConnectionState::Connecting, "initial connect");
//! // … spawn exchange client …
//! manager.transition(ConnectionState::Subscribing, "client spawned");
//! // … first event arrives …
//! manager.transition(ConnectionState::Streaming, "first event received");
//! // … channel closes …
//! let action = manager.on_disconnect(&reason);
//! ```

use std::time::Duration;

use tokio::time::Instant;

use super::connection_state::{ConnectionState, StateTransition};
use super::disconnect::DisconnectReason;
use super::reconnect::{ReconnectAction, ReconnectPolicy};

/// Configuration for building a [`ConnectionManager`].
#[derive(Debug, Clone)]
pub struct ConnectionManagerConfig {
    /// Base delay before the first retry.
    pub initial_delay: Duration,
    /// Upper bound on exponential backoff.
    pub max_delay: Duration,
    /// Maximum consecutive failures before circuit breaker opens.
    /// `None` = infinite retries.
    pub max_attempts: Option<u32>,
    /// Fraction of the base delay added as uniform random jitter.
    pub jitter_factor: f64,
}

impl Default for ConnectionManagerConfig {
    /// Default tuned for the `data_worker` spec:
    /// 100 ms → 200 ms → 400 ms … capped at 10 s.
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            max_attempts: None,
            jitter_factor: 0.5,
        }
    }
}

/// Stateful connection lifecycle manager.
///
/// Tracks [`ConnectionState`] transitions, logs them as structured
/// tracing events, and delegates reconnection decisions to a
/// [`ReconnectPolicy`].
pub struct ConnectionManager {
    state: ConnectionState,
    state_entered_at: Instant,
    policy: ReconnectPolicy,
    transitions: Vec<StateTransition>,
    label: String,
}

impl ConnectionManager {
    /// Create a new manager with the given label and configuration.
    ///
    /// The `label` is used as a prefix in all tracing events (e.g.
    /// `"bybit:BTCUSDT"`).
    pub fn new(label: impl Into<String>, config: ConnectionManagerConfig) -> Self {
        let policy = ReconnectPolicy::builder()
            .initial_delay(config.initial_delay)
            .max_delay(config.max_delay)
            .max_attempts(config.max_attempts)
            .jitter_factor(config.jitter_factor)
            .build();

        Self {
            state: ConnectionState::Disconnected,
            state_entered_at: Instant::now(),
            policy,
            transitions: Vec::new(),
            label: label.into(),
        }
    }

    /// Create a manager with the default `data_worker` backoff config
    /// (100 ms initial, 10 s max, infinite retries).
    pub fn with_defaults(label: impl Into<String>) -> Self {
        Self::new(label, ConnectionManagerConfig::default())
    }

    /// Current connection state.
    pub fn state(&self) -> ConnectionState {
        self.state
    }

    /// How long we've been in the current state.
    pub fn time_in_state(&self) -> Duration {
        self.state_entered_at.elapsed()
    }

    /// Number of consecutive reconnection failures.
    pub fn consecutive_failures(&self) -> u32 {
        self.policy.consecutive_failures()
    }

    /// Read-only view of all state transitions since creation.
    pub fn transitions(&self) -> &[StateTransition] {
        &self.transitions
    }

    /// Transition to a new state, logging the event.
    ///
    /// This is the central method that all state changes flow through.
    /// It records the transition, emits a structured tracing event, and
    /// updates internal timestamps.
    pub fn transition(&mut self, new_state: ConnectionState, reason: impl Into<String>) {
        let now = Instant::now();
        let reason = reason.into();
        let duration_in_prev = now.duration_since(self.state_entered_at);
        let prev_state = self.state;

        self.transitions.push(StateTransition {
            at: now,
            from: prev_state,
            to: new_state,
            reason: reason.clone(),
            duration_in_prev,
        });

        tracing::info!(
            label = %self.label,
            from = %prev_state,
            to = %new_state,
            reason = %reason,
            elapsed_in_prev_ms = duration_in_prev.as_millis() as u64,
            "connection.state_transition"
        );

        self.state = new_state;
        self.state_entered_at = now;
    }

    /// Signal that a connection was successfully established and the
    /// first data frame was received.
    ///
    /// Resets the reconnection policy's failure counter and backoff delay.
    pub fn on_connected(&mut self) {
        self.policy.on_connected();
    }

    /// Pause the worker.
    ///
    /// Transitions to [`ConnectionState::Paused`].  The caller is responsible
    /// for stopping event forwarding while in this state.  The underlying
    /// WebSocket connection is *not* torn down — the worker simply stops
    /// delivering events to sinks.
    pub fn pause(&mut self) {
        if self.state != ConnectionState::Paused {
            self.transition(ConnectionState::Paused, "user requested pause");
        }
    }

    /// Resume a paused worker.
    ///
    /// Transitions back to [`ConnectionState::Streaming`].
    pub fn resume(&mut self) {
        if self.state == ConnectionState::Paused {
            self.transition(ConnectionState::Streaming, "user requested resume");
        }
    }

    /// Determine the next action after a disconnection.
    ///
    /// Transitions the state to [`Reconnecting`](ConnectionState::Reconnecting)
    /// and consults the [`ReconnectPolicy`].
    ///
    /// Returns the [`ReconnectAction`] the caller should follow.
    pub fn on_disconnect(&mut self, reason: &DisconnectReason) -> ReconnectAction {
        let attempt = self.policy.consecutive_failures() + 1;
        self.transition(
            ConnectionState::Reconnecting { attempt },
            format!("{}", reason),
        );
        self.policy.next_action(reason)
    }
}
