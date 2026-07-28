//! OpenTelemetry metric instrument definitions for the aetelier-sdk engine.
//!
//! This module defines the four beta instruments that back the dashboard's
//! metrics row:
//!
//! | Instrument | OTel Kind | Dashboard Panel |
//! |---|---|---|
//! | `messages_received` | `Counter<u64>` | MESSAGES/S, LOB/S, TRADES/S |
//! | `event_latency_ms` | `Histogram<f64>` | LATENCY P99 |
//! | `worker_connection_state` | `Gauge<u64>` | Sidebar state badges |
//! | `sink_queue_depth` | `Gauge<u64>` | Sink status panel |
//!
//! # Usage
//!
//! ```ignore
//! use aetelier_telemetry::meters::IngestionMeters;
//!
//! let meters = IngestionMeters::new(&meter);
//! meters.record_event(&attrs);
//! meters.record_latency(12.5, &attrs);
//! ```

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};

// ─────────────────────────────────────────────────────────────────────────────
// Instrument name constants
// ─────────────────────────────────────────────────────────────────────────────

/// Counter: total messages received (one increment per classified event).
pub const MESSAGES_RECEIVED: &str = "messages_received";

/// Histogram: end-to-end event latency in milliseconds
/// (exchange timestamp → local receive time).
pub const EVENT_LATENCY_MS: &str = "event_latency_ms";

/// Gauge: current connection state as a numeric code.
///
/// Encoding: 0=Disconnected, 1=Connecting, 2=Authenticating,
/// 3=Subscribing, 4=Streaming, 5=Paused, 6=Reconnecting.
pub const WORKER_CONNECTION_STATE: &str = "worker_connection_state";

/// Gauge: number of pending messages in a channel sink's queue.
pub const SINK_QUEUE_DEPTH: &str = "sink_queue_depth";

// ─────────────────────────────────────────────────────────────────────────────
// Latency histogram bucket boundaries (milliseconds)
// ─────────────────────────────────────────────────────────────────────────────

/// Histogram bucket boundaries tuned for WebSocket event latency.
///
/// Range: 0.5 ms (co-located) to 2000 ms (degraded / reconnecting).
/// The P99 for a healthy connection should land in the 5–50 ms range.
pub const LATENCY_BUCKETS_MS: &[f64] = &[
    0.5, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2000.0,
];

// ─────────────────────────────────────────────────────────────────────────────
// IngestionMeters
// ─────────────────────────────────────────────────────────────────────────────

/// Holds the four beta OTel instruments for the ingestion pipeline.
///
/// Created once per `MeterProvider` and shared (cloned) across workers.
/// All OTel instrument handles are `Clone + Send + Sync`.
#[derive(Clone)]
pub struct IngestionMeters {
    /// Total messages received (dimensioned by exchange, symbol, topic).
    messages_received: Counter<u64>,

    /// Event processing latency in milliseconds.
    event_latency: Histogram<f64>,

    /// Current connection state per worker (numeric encoding).
    connection_state: Gauge<u64>,

    /// Current channel sink queue depth per worker.
    queue_depth: Gauge<u64>,
}

impl IngestionMeters {
    /// Create all instruments from a [`Meter`].
    ///
    /// The `Meter` should come from the configured `MeterProvider` —
    /// typically `global::meter("aetelier")`.
    pub fn new(meter: &Meter) -> Self {
        let messages_received = meter
            .u64_counter(MESSAGES_RECEIVED)
            .with_description("Total messages received by ingestion workers")
            .with_unit("events")
            .build();

        let event_latency = meter
            .f64_histogram(EVENT_LATENCY_MS)
            .with_description(
                "Event latency from exchange timestamp to local receive (ms)",
            )
            .with_unit("ms")
            .build();

        let connection_state = meter
            .u64_gauge(WORKER_CONNECTION_STATE)
            .with_description("Current connection state per worker (numeric encoding)")
            .with_unit("{state}")
            .build();

        let queue_depth = meter
            .u64_gauge(SINK_QUEUE_DEPTH)
            .with_description("Pending messages in channel sink queue")
            .with_unit("messages")
            .build();

        Self {
            messages_received,
            event_latency,
            connection_state,
            queue_depth,
        }
    }

    // ── Recording methods ────────────────────────────────────────────────

    /// Record one received message.
    ///
    /// Call once per classified event in the ingestion loop.
    pub fn record_event(&self, attrs: &[KeyValue]) {
        self.messages_received.add(1, attrs);
    }

    /// Record event processing latency in milliseconds.
    ///
    /// `latency_ms` should be `(received_at_us - exchange_ts_ns) / 1_000_000.0`.
    /// Negative values (clock skew) are clamped to 0.
    pub fn record_latency(&self, latency_ms: f64, attrs: &[KeyValue]) {
        self.event_latency.record(latency_ms.max(0.0), attrs);
    }

    /// Update the connection state gauge for a worker.
    pub fn set_connection_state(&self, state_code: u64, attrs: &[KeyValue]) {
        self.connection_state.record(state_code, attrs);
    }

    /// Update the queue depth gauge for a sink.
    pub fn set_queue_depth(&self, depth: u64, attrs: &[KeyValue]) {
        self.queue_depth.record(depth, attrs);
    }

    // ── Accessors (for testing) ──────────────────────────────────────────

    /// Access the raw messages counter (for testing / advanced usage).
    pub fn messages_counter(&self) -> &Counter<u64> {
        &self.messages_received
    }

    /// Access the raw latency histogram (for testing / advanced usage).
    pub fn latency_histogram(&self) -> &Histogram<f64> {
        &self.event_latency
    }

    /// Access the raw connection state gauge (for testing / advanced usage).
    pub fn connection_state_gauge(&self) -> &Gauge<u64> {
        &self.connection_state
    }

    /// Access the raw queue depth gauge (for testing / advanced usage).
    pub fn queue_depth_gauge(&self) -> &Gauge<u64> {
        &self.queue_depth
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ConnectionState → numeric encoding
// ─────────────────────────────────────────────────────────────────────────────

/// Encode a connection state name to a numeric gauge value.
///
/// This is intentionally a simple function that takes a `&str` (from
/// `ConnectionState::to_string()`) so that `aetelier-telemetry` does not
/// depend on `aetelier-connect`.
pub fn connection_state_code(state_str: &str) -> u64 {
    match state_str {
        "disconnected" => 0,
        "connecting" => 1,
        "authenticating" => 2,
        "subscribing" => 3,
        "streaming" => 4,
        "paused" => 5,
        _ if state_str.starts_with("reconnecting") => 6,
        _ => 99,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_state_codes() {
        assert_eq!(connection_state_code("disconnected"), 0);
        assert_eq!(connection_state_code("connecting"), 1);
        assert_eq!(connection_state_code("authenticating"), 2);
        assert_eq!(connection_state_code("subscribing"), 3);
        assert_eq!(connection_state_code("streaming"), 4);
        assert_eq!(connection_state_code("paused"), 5);
        assert_eq!(connection_state_code("reconnecting(attempt=3)"), 6);
        assert_eq!(connection_state_code("unknown_state"), 99);
    }

    #[test]
    fn test_latency_buckets_are_sorted() {
        for window in LATENCY_BUCKETS_MS.windows(2) {
            assert!(
                window[0] < window[1],
                "buckets must be strictly increasing: {} >= {}",
                window[0],
                window[1]
            );
        }
    }
}
