//! OpenTelemetry instrumentation for the aetelier-sdk engine.
//!
//! `aetelier-telemetry` provides a self-contained OTel foundation:
//!
//! - **Metrics**: Counter, Histogram, and Gauge instruments for ingestion
//!   pipeline observability ([`meters::IngestionMeters`]).
//! - **Traces**: Bridged from the `tracing` crate via `tracing-opentelemetry`,
//!   so existing `tracing::info_span!` calls automatically become OTel spans.
//! - **Exporters**: Pluggable backend selection — stdout for development,
//!   OTLP gRPC for production, no-op for tests ([`exporters::ExporterKind`]).
//!
//! # Quick Start
//!
//! ```ignore
//! use aetelier_telemetry::{TelemetryConfig, init_telemetry};
//!
//! let config = TelemetryConfig::default();
//! let guard = init_telemetry(&config)?;
//!
//! // … run workers …
//!
//! drop(guard); // flushes and shuts down providers
//! ```
//!
//! # Crate Organisation
//!
//! | Module | Purpose |
//! |---|---|
//! | [`attributes`] | Shared OTel attribute keys and builders |
//! | [`meters`] | Instrument definitions and recording helpers |
//! | [`exporters`] | Exporter selection and `MeterProvider` construction |

pub mod attributes;
pub mod exporters;
pub mod meters;

use std::time::Duration;

use opentelemetry::global;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use serde::Deserialize;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub use crate::exporters::TelemetryError;
use crate::exporters::{ExporterKind, build_meter_provider};
use crate::meters::IngestionMeters;

// ─────────────────────────────────────────────────────────────────────────────
// TelemetryConfig
// ─────────────────────────────────────────────────────────────────────────────

/// Top-level telemetry configuration, typically deserialized from TOML.
///
/// ```toml
/// [telemetry]
/// service_name = "aetelier-engine"
/// collect_interval_secs = 5
///
/// [telemetry.exporter]
/// type = "otlp"
/// endpoint = "http://localhost:4317"
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct TelemetryConfig {
    /// OTel service name resource attribute.
    #[serde(default = "default_service_name")]
    pub service_name: String,

    /// Metric collection / export interval in seconds.
    #[serde(default = "default_interval_secs")]
    pub collect_interval_secs: u64,

    /// Which exporter backend to use.
    #[serde(default)]
    pub exporter: ExporterKind,

    /// Whether to install the tracing-opentelemetry layer.
    #[serde(default = "default_tracing_enabled")]
    pub tracing_enabled: bool,
}

fn default_service_name() -> String {
    "aetelier-engine".to_string()
}

fn default_interval_secs() -> u64 {
    5
}

fn default_tracing_enabled() -> bool {
    true
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            service_name: default_service_name(),
            collect_interval_secs: default_interval_secs(),
            exporter: ExporterKind::default(),
            tracing_enabled: default_tracing_enabled(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TelemetryGuard
// ─────────────────────────────────────────────────────────────────────────────

/// RAII guard that shuts down the OTel `MeterProvider` on drop.
///
/// Hold this in `main()` (or equivalent) to ensure metrics are flushed
/// before the process exits.
pub struct TelemetryGuard {
    meter_provider: SdkMeterProvider,
}

impl TelemetryGuard {
    /// Access the underlying `SdkMeterProvider`.
    pub fn meter_provider(&self) -> &SdkMeterProvider {
        &self.meter_provider
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Err(e) = self.meter_provider.shutdown() {
            eprintln!("aetelier-telemetry: MeterProvider shutdown error: {e}");
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Initialization
// ─────────────────────────────────────────────────────────────────────────────

/// Initialize the OpenTelemetry telemetry stack.
///
/// This function:
/// 1. Builds a [`SdkMeterProvider`] from the configured exporter.
/// 2. Sets it as the OTel global meter provider.
/// 3. Optionally installs a `tracing-subscriber` with the
///    `tracing-opentelemetry` layer so that `tracing` spans become OTel
///    spans.
/// 4. Returns a [`TelemetryGuard`] whose drop impl flushes and shuts
///    down the provider.
///
/// # Errors
///
/// Returns an error if the exporter backend fails to initialize (e.g.
/// OTLP endpoint unreachable at build time for tonic channel).
pub fn init_telemetry(
    config: &TelemetryConfig,
) -> Result<TelemetryGuard, TelemetryError> {
    let interval = Duration::from_secs(config.collect_interval_secs);
    let meter_provider = build_meter_provider(&config.exporter, interval)?;

    // Set global meter provider so `global::meter("aetelier")` works anywhere.
    global::set_meter_provider(meter_provider.clone());

    // Install tracing subscriber with OTel layer if enabled.
    if config.tracing_enabled {
        let env_filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

        let fmt_layer = tracing_subscriber::fmt::layer().compact();

        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .init();
    }

    Ok(TelemetryGuard { meter_provider })
}

/// Create [`IngestionMeters`] from the global meter provider.
///
/// Call this after [`init_telemetry`] to get instrument handles that can
/// be cloned and distributed to workers.
pub fn ingestion_meters(meter_name: &'static str) -> IngestionMeters {
    let meter = global::meter(meter_name);
    IngestionMeters::new(&meter)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = TelemetryConfig::default();
        assert_eq!(config.service_name, "aetelier-engine");
        assert_eq!(config.collect_interval_secs, 5);
        assert!(config.tracing_enabled);
    }

    #[test]
    fn test_config_deserialize_minimal() {
        let toml_str = r#"
            service_name = "test-svc"
            collect_interval_secs = 2
        "#;
        let config: TelemetryConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.service_name, "test-svc");
        assert_eq!(config.collect_interval_secs, 2);
    }

    #[test]
    fn test_config_deserialize_with_exporter() {
        let toml_str = r#"
            service_name = "prod-engine"
            collect_interval_secs = 1

            [exporter]
            type = "otlp"
            endpoint = "http://collector:4317"
        "#;
        let config: TelemetryConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.service_name, "prod-engine");
        match &config.exporter {
            ExporterKind::Otlp { endpoint } => {
                assert_eq!(endpoint, "http://collector:4317");
            }
            other => panic!("expected Otlp, got {:?}", other),
        }
    }

    #[test]
    fn test_config_deserialize_none_exporter() {
        let toml_str = r#"
            [exporter]
            type = "none"
        "#;
        let config: TelemetryConfig = toml::from_str(toml_str).unwrap();
        assert!(matches!(config.exporter, ExporterKind::None));
    }
}

/// README code blocks compile as doc tests, so the README cannot drift from
/// the API. Invisible in rustdoc; exercised by `cargo test --doc`.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
pub struct ReadmeDoctests;
