//! Integration test: init → instrument → shutdown lifecycle.
//!
//! Verifies that the full telemetry stack can be initialized with a no-op
//! exporter, instruments can be created and used, and the guard's drop
//! performs a clean shutdown without panics.

use aetelier_telemetry::exporters::ExporterKind;
use aetelier_telemetry::{TelemetryConfig, ingestion_meters, init_telemetry};

#[tokio::test]
async fn init_noop_and_record_events() {
    let config = TelemetryConfig {
        service_name: "test-init".to_string(),
        collect_interval_secs: 60, // long interval — we don't need actual export
        exporter: ExporterKind::None,
        tracing_enabled: false, // avoid installing global subscriber in tests
    };

    let guard = init_telemetry(&config)
        .expect("init_telemetry should succeed with None exporter");

    // Create instrument handles from the global provider.
    let meters = ingestion_meters("aetelier-test");

    // Record a batch of events — should not panic.
    let attrs = aetelier_telemetry::attributes::event_attributes(
        "bybit",
        "BTCUSDT",
        "bybit:BTCUSDT:0",
        "orderbook.50.BTCUSDT",
    );
    for _ in 0..100 {
        meters.record_event(&attrs);
    }

    // Record latencies — including edge cases.
    meters.record_latency(0.0, &attrs);
    meters.record_latency(42.5, &attrs);
    meters.record_latency(-10.0, &attrs); // negative → clamped to 0

    // Connection state gauge.
    let worker_attrs = aetelier_telemetry::attributes::worker_attributes(
        "bybit",
        "BTCUSDT",
        "perpetual",
        "bybit:BTCUSDT:0",
    );
    meters.set_connection_state(
        aetelier_telemetry::meters::connection_state_code("streaming"),
        &worker_attrs,
    );

    // Queue depth gauge.
    meters.set_queue_depth(42, &worker_attrs);

    // Drop guard — should flush and shut down cleanly.
    drop(guard);
}
