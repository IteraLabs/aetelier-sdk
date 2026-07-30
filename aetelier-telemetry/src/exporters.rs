//! Exporter configuration and construction.
//!
//! Provides [`ExporterKind`] for selecting between OTLP (production) and
//! stdout (development) metric exporters, plus builder functions for
//! constructing the corresponding `MeterProvider`.
//!
//! # Examples
//!
//! ```ignore
//! // Development: print metrics to stderr
//! let provider = build_meter_provider(ExporterKind::Stdout, Duration::from_secs(5))?;
//!
//! // Production: push to OTLP collector
//! let provider = build_meter_provider(
//!     ExporterKind::Otlp { endpoint: "http://localhost:4317".into() },
//!     Duration::from_secs(2),
//! )?;
//! ```

use std::time::Duration;

use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use serde::Deserialize;
use thiserror::Error;

/// Errors from telemetry initialization.
#[derive(Error, Debug)]
pub enum TelemetryError {
    /// The metrics exporter backend failed to build (e.g. OTLP endpoint
    /// unusable at construction time).
    #[error("failed to build metrics exporter: {0}")]
    ExporterBuild(String),
}

// ─────────────────────────────────────────────────────────────────────────────
// ExporterKind
// ─────────────────────────────────────────────────────────────────────────────

/// Which metrics exporter to use.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ExporterKind {
    /// Print metrics to stderr in human-readable form.
    /// Best for local development and debugging.
    #[default]
    Stdout,
    /// Push metrics via OTLP gRPC to an OpenTelemetry Collector.
    Otlp {
        /// gRPC endpoint (e.g. `"http://localhost:4317"`).
        endpoint: String,
    },
    /// No-op exporter — instruments are active (no panics) but data
    /// is silently discarded.  Useful for tests and benchmarks.
    None,
}

// impl Default for ExporterKind {
//     fn default() -> Self {
//         ExporterKind::Stdout
//     }
// }

// ─────────────────────────────────────────────────────────────────────────────
// MeterProvider construction
// ─────────────────────────────────────────────────────────────────────────────

/// Build a [`SdkMeterProvider`] with the specified exporter and collection
/// interval.
///
/// The returned provider should be set as the global meter provider via
/// [`opentelemetry::global::set_meter_provider`].
///
/// # Arguments
///
/// - `kind` — which exporter backend to use.
/// - `interval` — how often the SDK collects and exports metric batches.
///   For dashboard responsiveness, 1–5 seconds is recommended.  The OTel
///   default is 60 s which is too slow for real-time sparklines.
pub fn build_meter_provider(
    kind: &ExporterKind,
    interval: Duration,
) -> Result<SdkMeterProvider, TelemetryError> {
    match kind {
        ExporterKind::Stdout => {
            let exporter = opentelemetry_stdout::MetricExporter::default();
            let reader = PeriodicReader::builder(exporter)
                .with_interval(interval)
                .build();
            let provider = SdkMeterProvider::builder().with_reader(reader).build();
            Ok(provider)
        }

        ExporterKind::Otlp { endpoint } => {
            let exporter = opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .build()
                .map_err(|e| TelemetryError::ExporterBuild(e.to_string()))?;
            let reader = PeriodicReader::builder(exporter)
                .with_interval(interval)
                .build();
            let provider = SdkMeterProvider::builder().with_reader(reader).build();
            Ok(provider)
        }

        ExporterKind::None => {
            // No reader attached → instruments are no-ops.
            let provider = SdkMeterProvider::builder().build();
            Ok(provider)
        }
    }
}
