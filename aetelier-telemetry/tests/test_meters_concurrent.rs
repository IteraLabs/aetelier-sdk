//! Integration test: concurrent meter recording from multiple tasks.
//!
//! Verifies that `IngestionMeters` can be cloned and used from multiple
//! Tokio tasks simultaneously without panics or data races.  This
//! exercises the `Clone + Send + Sync` guarantees of OTel instrument handles.

use aetelier_telemetry::attributes;
use aetelier_telemetry::exporters::ExporterKind;
use aetelier_telemetry::meters::connection_state_code;
use aetelier_telemetry::{TelemetryConfig, ingestion_meters, init_telemetry};
use std::sync::Arc;

#[tokio::test]
async fn concurrent_recording_from_multiple_workers() {
    let config = TelemetryConfig {
        service_name: "test-concurrent".to_string(),
        collect_interval_secs: 60,
        exporter: ExporterKind::None,
        tracing_enabled: false,
    };

    let guard = init_telemetry(&config).expect("init should succeed");
    let meters = Arc::new(ingestion_meters("concurrent-test"));

    let num_workers = 8;
    let events_per_worker = 500;

    let mut handles = Vec::new();

    for worker_id in 0..num_workers {
        let meters = Arc::clone(&meters);
        let handle = tokio::spawn(async move {
            let exchange = "bybit";
            let symbol = "BTCUSDT";
            let worker_label = format!("bybit:BTCUSDT:{}", worker_id);
            let topic = "orderbook.50.BTCUSDT";

            let event_attrs =
                attributes::event_attributes(exchange, symbol, &worker_label, topic);
            let worker_attrs = attributes::worker_attributes(
                exchange,
                symbol,
                "perpetual",
                &worker_label,
            );

            // Record connection state.
            meters
                .set_connection_state(connection_state_code("streaming"), &worker_attrs);

            // Simulate event ingestion.
            for i in 0..events_per_worker {
                meters.record_event(&event_attrs);
                meters.record_latency((i as f64) * 0.1, &event_attrs);
            }

            // Update queue depth.
            meters.set_queue_depth(events_per_worker as u64, &worker_attrs);
        });
        handles.push(handle);
    }

    // All tasks should complete without panic.
    for handle in handles {
        handle.await.expect("worker task should not panic");
    }

    drop(guard);
}
