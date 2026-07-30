//! Integration test: TOML config deserialization round-trips.
//!
//! Verifies that realistic TOML configuration strings deserialize into
//! valid `TelemetryConfig` values and that those configs can successfully
//! initialize the telemetry stack.

use aetelier_telemetry::exporters::ExporterKind;
use aetelier_telemetry::{TelemetryConfig, init_telemetry};

#[test]
fn deserialize_full_production_config() {
    let toml_str = r#"
        service_name = "aetelier-prod"
        collect_interval_secs = 2
        tracing_enabled = true

        [exporter]
        type = "otlp"
        endpoint = "http://otel-collector.internal:4317"
    "#;

    let config: TelemetryConfig =
        toml::from_str(toml_str).expect("should deserialize production config");

    assert_eq!(config.service_name, "aetelier-prod");
    assert_eq!(config.collect_interval_secs, 2);
    assert!(config.tracing_enabled);
    match &config.exporter {
        ExporterKind::Otlp { endpoint } => {
            assert_eq!(endpoint, "http://otel-collector.internal:4317");
        }
        other => panic!("expected Otlp variant, got {:?}", other),
    }
}

#[test]
fn deserialize_development_config() {
    let toml_str = r#"
        service_name = "aetelier-dev"
        collect_interval_secs = 10

        [exporter]
        type = "stdout"
    "#;

    let config: TelemetryConfig =
        toml::from_str(toml_str).expect("should deserialize development config");

    assert!(matches!(config.exporter, ExporterKind::Stdout));
}

#[test]
fn deserialize_minimal_defaults() {
    // Completely empty — all fields have defaults.
    let config: TelemetryConfig =
        toml::from_str("").expect("empty TOML should use defaults");

    assert_eq!(config.service_name, "aetelier-engine");
    assert_eq!(config.collect_interval_secs, 5);
    assert!(config.tracing_enabled);
    assert!(matches!(config.exporter, ExporterKind::Stdout));
}

#[tokio::test]
async fn deserialized_none_config_initializes_successfully() {
    let toml_str = r#"
        service_name = "test-none"
        collect_interval_secs = 60
        tracing_enabled = false

        [exporter]
        type = "none"
    "#;

    let config: TelemetryConfig = toml::from_str(toml_str).unwrap();
    let guard = init_telemetry(&config).expect("None exporter should always initialize");

    // Verify we can create meters from the global provider.
    let meters = aetelier_telemetry::ingestion_meters("config-test");
    meters.record_event(&[]);

    drop(guard);
}
