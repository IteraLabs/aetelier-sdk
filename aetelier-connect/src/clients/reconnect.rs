//! Reconnection policy, error classification, and connection health monitoring.
//!
//! This module provides the building blocks for resilient WebSocket connections:
//!
//! - [`DisconnectReason`](crate::clients::disconnect::DisconnectReason) — a typed classification of *why* a connection ended,
//!   derived from WebSocket close frames, transport errors, or application-level
//!   signals (stale connection, receiver drop).
//!
//! - [`ReconnectPolicy`](crate::clients::reconnect::ReconnectPolicy) — a stateful backoff engine with **jittered exponential
//!   backoff**, a configurable **max-attempts** limit, and a three-state **circuit
//!   breaker** (`Closed → Open → HalfOpen → Closed`).
//!
//! - [`HealthMonitor`](crate::clients::reconnect::HealthMonitor) — stale-connection detection based on a per-exchange
//!   silence timeout.  Designed to slot into a `tokio::select!` loop via its
//!   `deadline()` method.
//!
//! # Architecture
//!
//! Connection ownership is layered:
//!
//! | Layer | Responsibility |
//! |-------|---------------|
//! | `WssClient::run()` | Single connection lifetime; returns a `DisconnectReason` on exit |
//! | Exchange client (`receive_data`) | Subscription, heartbeat, decode; delegates to `WssClient` |
//! | `MarketWorker` | Owns a `ReconnectPolicy`; consumes the reason and decides retry / give-up / circuit-open |
//!
//! # Jitter rationale
//!
//! Pure exponential backoff (`delay × 2`) causes **thundering-herd** spikes when
//! many workers disconnect simultaneously (e.g. exchange maintenance window).
//! Adding uniform random jitter spreads reconnection attempts:
//!
//! ```text
//! actual_delay = base_delay + rand(0 .. base_delay × jitter_factor)
//! ```
//!
//! With the default `jitter_factor = 0.5`, a 4 s base delay becomes a uniform
//! draw from `[4.0, 6.0)` seconds.

use crate::clients::disconnect::DisconnectReason;
use rand::Rng;
use std::fmt;
use std::time::Duration;
use tokio::time::Instant;

// ─────────────────────────────────────────────────────────────────────────────
// Reconnection policy
// ─────────────────────────────────────────────────────────────────────────────

/// The three states of the circuit breaker.
///
/// ```text
///   ┌──────────┐  max_attempts exceeded   ┌──────────┐
///   │  Closed  │ ───────────────────────▶ │   Open   │
///   └──────────┘                          └──────────┘
///        ▲                                     │
///        │  probe succeeds                     │ cooldown expires
///        │                                     ▼
///        │                                ┌──────────┐
///        └─────────────────────────────── │ HalfOpen │
///                                         └──────────┘
///                  probe fails ──────────▶ back to Open
///                                         (doubled cooldown)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation — connection attempts are allowed.
    Closed,
    /// Tripped — reject connection attempts until `cooldown_until`.
    Open,
    /// One probe attempt is allowed after cooldown expiry.
    HalfOpen,
}

impl fmt::Display for CircuitState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => write!(f, "closed"),
            Self::Open => write!(f, "open"),
            Self::HalfOpen => write!(f, "half_open"),
        }
    }
}

/// What the caller should do after consulting the [`ReconnectPolicy`].
#[derive(Debug)]
pub enum ReconnectAction {
    /// Wait this long, then reconnect.
    RetryAfter(Duration),
    /// Reconnect immediately (no delay).
    RetryImmediately,
    /// Stop trying — either the error is non-retryable or max attempts exhausted.
    GiveUp { reason: String },
    /// Circuit breaker is open — wait until `until` before probing.
    CircuitOpen { until: Instant },
}

/// Stateful reconnection policy with jittered exponential backoff and a
/// circuit breaker.
///
/// # Example
///
/// ```rust,ignore
/// use std::time::Duration;
/// use aetelier_connect::clients::reconnect::{ReconnectPolicy, DisconnectReason, ReconnectAction};
///
/// let mut policy = ReconnectPolicy::builder()
///     .initial_delay(Duration::from_secs(1))
///     .max_delay(Duration::from_secs(30))
///     .max_attempts(Some(50))
///     .jitter_factor(0.5)
///     .build();
///
/// // After a transport error:
/// let reason = DisconnectReason::TransportError {
///     source: "connection reset".into(),
/// };
/// match policy.next_action(&reason) {
///     ReconnectAction::RetryAfter(d) => { /* sleep d, then reconnect */ }
///     ReconnectAction::GiveUp { reason } => { /* surface error */ }
///     _ => {}
/// }
///
/// // After a successful connection + first message:
/// policy.on_connected();
/// ```
pub struct ReconnectPolicy {
    // ── Configuration (immutable after build) ────────────────────────────
    initial_delay: Duration,
    max_delay: Duration,
    max_attempts: Option<u32>,
    jitter_factor: f64,

    // ── Mutable state ────────────────────────────────────────────────────
    current_delay: Duration,
    consecutive_failures: u32,
    circuit_state: CircuitState,
    cooldown_until: Option<Instant>,
    cooldown_duration: Duration,
}

impl ReconnectPolicy {
    /// Create a builder for configuring a new policy.
    pub fn builder() -> ReconnectPolicyBuilder {
        ReconnectPolicyBuilder::default()
    }

    /// Number of consecutive failures since the last successful connection.
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    /// Current circuit breaker state.
    pub fn circuit_state(&self) -> CircuitState {
        self.circuit_state
    }

    /// Decide what to do after a disconnection.
    ///
    /// This is the core decision function.  It inspects the
    /// [`DisconnectReason`], advances the backoff state, and returns a
    /// [`ReconnectAction`] telling the caller how to proceed.
    ///
    /// # Jitter formula
    ///
    /// ```text
    /// actual_delay = base_delay × factor + rand(0 .. base_delay × jitter_factor)
    /// ```
    ///
    /// where `factor` comes from
    /// [`DisconnectReason::suggested_delay_factor()`].
    pub fn next_action(&mut self, reason: &DisconnectReason) -> ReconnectAction {
        // ── Non-retryable errors → give up immediately ───────────────────
        if !reason.is_retryable() {
            tracing::error!(
                reason = %reason,
                "reconnect.non_retryable"
            );
            return ReconnectAction::GiveUp {
                reason: format!("{reason}"),
            };
        }

        // ── Circuit breaker check ────────────────────────────────────────
        match self.circuit_state {
            CircuitState::Open => {
                if let Some(until) = self.cooldown_until
                    && Instant::now() < until
                {
                    tracing::error!(
                        cooldown_remaining_ms = (until - Instant::now()).as_millis() as u64,
                        circuit = %self.circuit_state,
                        "reconnect.circuit_open"
                    );
                    return ReconnectAction::CircuitOpen { until };
                }
                // Cooldown expired → transition to HalfOpen
                self.circuit_state = CircuitState::HalfOpen;
                tracing::info!("reconnect.circuit_half_open");
                // Allow one probe with initial delay
                return ReconnectAction::RetryAfter(self.initial_delay);
            }
            CircuitState::HalfOpen => {
                // Probe failed — re-open with doubled cooldown
                self.cooldown_duration =
                    (self.cooldown_duration * 2).min(self.max_delay * 8);
                self.cooldown_until = Some(Instant::now() + self.cooldown_duration);
                self.circuit_state = CircuitState::Open;
                tracing::error!(
                    cooldown_ms = self.cooldown_duration.as_millis() as u64,
                    "reconnect.circuit_reopen"
                );
                return ReconnectAction::CircuitOpen {
                    until: self.cooldown_until.unwrap(),
                };
            }
            CircuitState::Closed => { /* normal path — continue below */ }
        }

        // ── Immediate retry for CleanClose ───────────────────────────────
        let factor = reason.suggested_delay_factor();
        if factor == 0.0 {
            // Don't increment failures for clean closes
            tracing::info!(reason = %reason, "reconnect.retry_immediately");
            return ReconnectAction::RetryImmediately;
        }

        // ── Increment failure counter ────────────────────────────────────
        self.consecutive_failures += 1;

        // ── Max-attempts check → open circuit ────────────────────────────
        if let Some(max) = self.max_attempts
            && self.consecutive_failures >= max
        {
            self.cooldown_duration = self.max_delay * 4;
            self.cooldown_until = Some(Instant::now() + self.cooldown_duration);
            self.circuit_state = CircuitState::Open;
            tracing::error!(
                attempts = self.consecutive_failures,
                max_attempts = max,
                cooldown_ms = self.cooldown_duration.as_millis() as u64,
                circuit = %self.circuit_state,
                "reconnect.circuit_open"
            );
            return ReconnectAction::CircuitOpen {
                until: self.cooldown_until.unwrap(),
            };
        }

        // ── Compute jittered delay ───────────────────────────────────────
        let base_ms = self.current_delay.as_millis() as f64 * factor;
        let jitter_ms = if self.jitter_factor > 0.0 {
            rand::rng().random_range(0.0..base_ms * self.jitter_factor)
        } else {
            0.0
        };
        let delay = Duration::from_millis((base_ms + jitter_ms) as u64);

        tracing::warn!(
            attempts = self.consecutive_failures,
            delay_ms = delay.as_millis() as u64,
            base_ms = base_ms as u64,
            jitter_ms = jitter_ms as u64,
            reason = %reason,
            "reconnect.backoff"
        );

        // ── Advance base delay for next iteration ────────────────────────
        self.current_delay = (self.current_delay * 2).min(self.max_delay);

        ReconnectAction::RetryAfter(delay)
    }

    /// Signal that a connection was successfully established.
    ///
    /// Resets the failure counter, base delay, and transitions the circuit
    /// breaker to [`CircuitState::Closed`].
    pub fn on_connected(&mut self) {
        self.consecutive_failures = 0;
        self.current_delay = self.initial_delay;
        self.cooldown_until = None;
        self.cooldown_duration = self.max_delay * 4;

        if self.circuit_state != CircuitState::Closed {
            tracing::info!(
                prev_state = %self.circuit_state,
                "reconnect.circuit_closed"
            );
            self.circuit_state = CircuitState::Closed;
        }
    }

    /// Signal that a valid message was received on the active connection.
    ///
    /// Resets the backoff delay so that the *next* disconnection starts from
    /// [`initial_delay`](ReconnectPolicyBuilder::initial_delay) rather than
    /// continuing an escalated sequence.
    pub fn on_message_received(&mut self) {
        self.current_delay = self.initial_delay;
    }
}

/// Builder for [`ReconnectPolicy`].
///
/// All fields have sensible defaults:
///
/// | Field | Default |
/// |-------|---------|
/// | `initial_delay` | 1 s |
/// | `max_delay` | 30 s |
/// | `max_attempts` | `None` (infinite) |
/// | `jitter_factor` | 0.5 |
pub struct ReconnectPolicyBuilder {
    initial_delay: Duration,
    max_delay: Duration,
    max_attempts: Option<u32>,
    jitter_factor: f64,
}

impl Default for ReconnectPolicyBuilder {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            max_attempts: None,
            jitter_factor: 0.5,
        }
    }
}

impl ReconnectPolicyBuilder {
    /// Base delay before the first retry (default: 1 s).
    pub fn initial_delay(mut self, d: Duration) -> Self {
        self.initial_delay = d;
        self
    }

    /// Upper bound on the exponential backoff (default: 30 s).
    pub fn max_delay(mut self, d: Duration) -> Self {
        self.max_delay = d;
        self
    }

    /// Maximum consecutive failures before the circuit breaker opens.
    ///
    /// `None` means infinite retries (circuit breaker never trips on count
    /// alone).
    pub fn max_attempts(mut self, n: Option<u32>) -> Self {
        self.max_attempts = n;
        self
    }

    /// Fraction of the base delay added as uniform random jitter (default: 0.5).
    ///
    /// A value of `0.5` means the actual delay is drawn uniformly from
    /// `[base, base × 1.5)`.  Set to `0.0` to disable jitter entirely.
    pub fn jitter_factor(mut self, f: f64) -> Self {
        self.jitter_factor = f.max(0.0);
        self
    }

    /// Build the policy.  Panics if `initial_delay > max_delay`.
    pub fn build(self) -> ReconnectPolicy {
        assert!(
            self.initial_delay <= self.max_delay,
            "initial_delay ({:?}) must be <= max_delay ({:?})",
            self.initial_delay,
            self.max_delay,
        );

        ReconnectPolicy {
            initial_delay: self.initial_delay,
            max_delay: self.max_delay,
            max_attempts: self.max_attempts,
            jitter_factor: self.jitter_factor,
            current_delay: self.initial_delay,
            consecutive_failures: 0,
            circuit_state: CircuitState::Closed,
            cooldown_until: None,
            cooldown_duration: self.max_delay * 4,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Connection health monitoring
// ─────────────────────────────────────────────────────────────────────────────

/// Stale-connection detector.
///
/// Tracks the timestamp of the last received WebSocket activity (any frame:
/// Text, Ping, Pong) and exposes a [`deadline()`](Self::deadline) suitable
/// for use in a `tokio::select!` branch:
///
/// ```rust,ignore
/// loop {
///     tokio::select! {
///         Some(msg) = read.next() => {
///             health.record_activity();
///             // … handle msg …
///         }
///         _ = tokio::time::sleep_until(health.deadline()) => {
///             return DisconnectReason::StaleConnection {
///                 silence_duration: health.timeout(),
///             };
///         }
///     }
/// }
/// ```
pub struct HealthMonitor {
    timeout: Duration,
    last_activity: Instant,
}

impl HealthMonitor {
    /// Create a new monitor with the given staleness timeout.
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            last_activity: Instant::now(),
        }
    }

    /// Record that a message was received, resetting the staleness clock.
    pub fn record_activity(&mut self) {
        self.last_activity = Instant::now();
    }

    /// The [`Instant`] at which the connection will be considered stale.
    ///
    /// Use with `tokio::time::sleep_until(health.deadline())` inside a
    /// `tokio::select!` loop.
    pub fn deadline(&self) -> Instant {
        self.last_activity + self.timeout
    }

    /// Whether the connection is currently stale (no activity within timeout).
    pub fn is_stale(&self) -> bool {
        Instant::now() > self.deadline()
    }

    /// The configured staleness timeout.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

/// Per-exchange staleness timeout configuration.
///
/// Implement this trait on exchange client types to override the default
/// 60-second silence timeout.  Exchanges that send frequent heartbeats
/// (e.g. Kraken, ~1 /s) should use a much shorter timeout.
///
/// # Defaults
///
/// The blanket default is 60 seconds — the platform-wide staleness default,
/// aligned with the framework transport's `STALE_AFTER` and the TOML
/// `staleness_timeout_secs` fallback, so every layer agrees on when a silent
/// socket is presumed dead. Long enough to tolerate quiet markets on
/// event-driven exchanges (Bybit, Coinbase) while still detecting truly dead
/// connections.
///
/// | Exchange | Recommended override |
/// |----------|---------------------|
/// | Kraken | 5 s (server heartbeats ~1/s) |
/// | Bybit | 60 s (event-driven) |
/// | Coinbase | 60 s (event-driven) |
pub trait ConnectionHealth {
    /// How long silence is tolerated before declaring the connection stale.
    fn staleness_timeout(&self) -> Duration {
        Duration::from_secs(60)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::disconnect::DisconnectReason;

    fn transport() -> DisconnectReason {
        DisconnectReason::TransportError {
            source: "connection reset".into(),
        }
    }

    fn going_away() -> DisconnectReason {
        DisconnectReason::GoingAway {
            reason: "server going away".into(),
        }
    }

    /// A jitter-free policy makes `next_action` fully deterministic, so the
    /// backoff ladder can be asserted exactly.
    fn no_jitter(
        initial_ms: u64,
        max_ms: u64,
        max_attempts: Option<u32>,
    ) -> ReconnectPolicy {
        ReconnectPolicy::builder()
            .initial_delay(Duration::from_millis(initial_ms))
            .max_delay(Duration::from_millis(max_ms))
            .max_attempts(max_attempts)
            .jitter_factor(0.0)
            .build()
    }

    fn retry_after(action: ReconnectAction) -> Duration {
        match action {
            ReconnectAction::RetryAfter(d) => d,
            other => panic!("expected RetryAfter, got {other:?}"),
        }
    }

    #[test]
    fn backoff_escalates_and_caps_at_max_delay() {
        // GoingAway has factor 1.0, so each delay equals the current base
        // delay, which doubles per failure and caps at max_delay.
        let mut p = no_jitter(1_000, 30_000, None);
        let got: Vec<u64> = (0..7)
            .map(|_| retry_after(p.next_action(&going_away())).as_millis() as u64)
            .collect();
        assert_eq!(
            got,
            vec![1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000]
        );
        assert_eq!(p.consecutive_failures(), 7);
    }

    #[test]
    fn jitter_stays_within_envelope() {
        // With jitter_factor 0.5 the first delay is drawn from [base, base*1.5).
        // rand is unseeded, so assert the bound over many fresh policies.
        for _ in 0..200 {
            let mut p = ReconnectPolicy::builder()
                .initial_delay(Duration::from_millis(1_000))
                .max_delay(Duration::from_secs(30))
                .jitter_factor(0.5)
                .build();
            let d = retry_after(p.next_action(&going_away())).as_millis() as u64;
            assert!((1_000..1_500).contains(&d), "delay {d} out of [1000,1500)");
        }
    }

    #[test]
    fn on_connected_resets_failures_delay_and_circuit() {
        let mut p = no_jitter(1_000, 30_000, Some(5));
        p.next_action(&going_away());
        p.next_action(&going_away());
        assert_eq!(p.consecutive_failures(), 2);

        p.on_connected();
        assert_eq!(p.consecutive_failures(), 0);
        assert_eq!(p.circuit_state(), CircuitState::Closed);
        // Base delay is back to initial: the next failure yields the first-rung
        // delay, not a continuation of the escalated ladder.
        assert_eq!(
            retry_after(p.next_action(&going_away())).as_millis() as u64,
            1_000
        );
    }

    #[test]
    fn on_message_received_resets_delay_but_not_failures() {
        // This is exactly why on_connected is needed in the worker loop:
        // a live stream resets the backoff ladder but NOT the failure counter.
        let mut p = no_jitter(1_000, 30_000, Some(10));
        p.next_action(&going_away());
        p.next_action(&going_away());
        p.next_action(&going_away());
        assert_eq!(p.consecutive_failures(), 3);

        p.on_message_received();
        // Delay ladder reset ...
        assert_eq!(
            retry_after(p.next_action(&going_away())).as_millis() as u64,
            1_000
        );
        // ... but failures kept climbing (now 4).
        assert_eq!(p.consecutive_failures(), 4);
    }

    #[test]
    fn circuit_opens_after_max_attempts() {
        let mut p = no_jitter(1_000, 30_000, Some(3));
        assert!(matches!(
            p.next_action(&going_away()),
            ReconnectAction::RetryAfter(_)
        ));
        assert!(matches!(
            p.next_action(&going_away()),
            ReconnectAction::RetryAfter(_)
        ));
        // Third failure hits max_attempts → circuit opens.
        assert!(matches!(
            p.next_action(&going_away()),
            ReconnectAction::CircuitOpen { .. }
        ));
        assert_eq!(p.consecutive_failures(), 3);
        assert_eq!(p.circuit_state(), CircuitState::Open);
    }

    #[test]
    fn clean_close_retries_immediately_without_counting() {
        let mut p = no_jitter(1_000, 30_000, Some(3));
        assert!(matches!(
            p.next_action(&DisconnectReason::CleanClose),
            ReconnectAction::RetryImmediately
        ));
        assert_eq!(p.consecutive_failures(), 0);
    }

    #[test]
    fn non_retryable_reason_gives_up() {
        let mut p = no_jitter(1_000, 30_000, None);
        assert!(matches!(
            p.next_action(&DisconnectReason::ReceiverDropped),
            ReconnectAction::GiveUp { .. }
        ));
    }

    #[tokio::test]
    async fn circuit_transitions_open_half_open_reopen() {
        // Tiny delays so the real cooldown is a few ms.
        let mut p = no_jitter(1, 1, Some(1));
        // One failure hits max_attempts(1) → Open, cooldown = max_delay * 4.
        assert!(matches!(
            p.next_action(&transport()),
            ReconnectAction::CircuitOpen { .. }
        ));
        assert_eq!(p.circuit_state(), CircuitState::Open);

        // While the cooldown is live, the circuit stays open.
        assert!(matches!(
            p.next_action(&transport()),
            ReconnectAction::CircuitOpen { .. }
        ));

        // After the cooldown expires, one probe is allowed (HalfOpen).
        // The cooldown compares against `Instant::now()` sampled *inside*
        // `next_action`, so tokio::time::pause/advance cannot drive this branch
        // deterministically; we instead wait in real time with a wide margin —
        // the cooldown here is only max_delay(1ms) * 4 = 4ms, so 200ms is a
        // generous, non-racy bound that guarantees expiry on any loaded CI.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(matches!(
            p.next_action(&transport()),
            ReconnectAction::RetryAfter(_)
        ));
        assert_eq!(p.circuit_state(), CircuitState::HalfOpen);

        // A failure during the probe re-opens with a doubled cooldown.
        assert!(matches!(
            p.next_action(&transport()),
            ReconnectAction::CircuitOpen { .. }
        ));
        assert_eq!(p.circuit_state(), CircuitState::Open);
    }

    #[tokio::test]
    async fn health_monitor_detects_and_clears_staleness() {
        // `is_stale()` compares the configured timeout against a freshly
        // sampled `Instant::now()`, so tokio::time::pause/advance cannot drive
        // it deterministically; we wait in real time instead. A 50ms timeout
        // paired with a 200ms sleep leaves a 150ms margin for the "stale"
        // check, while the 50ms timeout gives the post-reset "not stale" check
        // ample slack before the deadline — both bounds are far from racy.
        let mut h = HealthMonitor::new(Duration::from_millis(50));
        assert!(!h.is_stale());
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(h.is_stale());
        h.record_activity();
        assert!(!h.is_stale());
    }
}
