//! Configuration for [`DataWorker`](crate::workers::data_worker::DataWorker).
//!
//! [`DataWorkerConfig`] is the lean, DataWorker-specific config — no
//! synchronisation fields, no Parquet flush cadence, no grid spacing.
//! Just exchange + symbol + datatypes + output sinks.
//!
//! [`DataWorkerManifest`] is the multi-worker TOML manifest that resolves
//! into a `Vec<DataWorkerConfig>`.

use serde::Deserialize;
use std::path::Path;

use super::common::{
    CommonWorkerFields, ManifestMetadata, OutputSinkConfig, ReconnectSection,
};
use aetelier_types::config::markets::market_config::{
    DataTypesSection, MarketSnapshotConfig,
};
use aetelier_types::exchanges::{MarketType, VenueEnvironment};

use crate::errors::ConnectError;

// ─────────────────────────────────────────────────────────────────────────────
// DataWorkerConfig — per-worker resolved config
// ─────────────────────────────────────────────────────────────────────────────

/// Fully-resolved configuration for a single [`DataWorker`](crate::workers::data_worker::DataWorker).
///
/// Created by [`DataWorkerManifest::resolve_all()`] or constructed
/// directly for library / test usage.
#[derive(Debug, Clone)]
pub struct DataWorkerConfig {
    /// Shared worker fields (exchange, symbol, datatypes, tuning).
    pub common: CommonWorkerFields,
    /// Output sinks to fan events into.
    pub output: Vec<OutputSinkConfig>,
    /// Ingest via the framework engine (registry adapter → normalized
    /// `DomainEvent` → domain topics) instead of the legacy per-venue spawn
    /// sprawl. Only takes effect when the venue is registered and the enabled
    /// datatypes are a subset of {orderbook, trades}; otherwise the worker runs
    /// the legacy raw path. Default `false`.
    pub framework_ingest: bool,
}

impl DataWorkerConfig {
    /// Create from a legacy [`MarketSnapshotConfig`] with a channel-only
    /// sink.  Used for backward compatibility.
    pub fn from_legacy(cfg: &MarketSnapshotConfig) -> Self {
        Self {
            common: CommonWorkerFields::from(cfg),
            output: vec![OutputSinkConfig::Channel],
            framework_ingest: false,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DataWorkerManifest — multi-worker TOML manifest
// ─────────────────────────────────────────────────────────────────────────────

/// Top-level TOML manifest for spawning multiple [`DataWorker`](crate::workers::data_worker::DataWorker)s.
///
/// # Example TOML
///
/// ```toml
/// [collect]
/// exchange = "bybit"
///
/// [collect.datatypes.orderbook]
/// enabled = true
/// depth   = 50
///
/// [collect.datatypes.trades]
/// enabled = true
///
/// [[collect.output]]
/// type = "channel"
///
/// [[workers]]
/// symbol = "BTCUSDT"
///
/// [[workers]]
/// symbol = "SOLUSDT"
///
/// [session]
/// duration_hours = 8
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct DataWorkerManifest {
    /// Shared collect applied to all workers.
    pub collect: DataWorkerCollect,
    /// Per-symbol worker definitions.
    pub workers: Vec<DataWorkerEntry>,
    /// Session parameters (duration, etc.).
    #[serde(default)]
    pub session: SessionSection,
    /// Platform-injected identity metadata (present when received over the wire).
    #[serde(default)]
    pub metadata: Option<ManifestMetadata>,
}

/// Shared collect section in a [`DataWorkerManifest`].
#[derive(Debug, Clone, Deserialize)]
pub struct DataWorkerCollect {
    /// Default exchange for all workers.
    pub exchange: String,
    /// Default market type (spot, perpetual, inverse).
    #[serde(default)]
    pub market_type: MarketType,
    #[serde(default)]
    pub environment: VenueEnvironment,
    /// Which data feeds to subscribe to.
    pub datatypes: DataTypesSection,
    /// Output sinks.
    #[serde(default = "default_output_sinks")]
    pub output: Vec<OutputSinkConfig>,
    /// Broadcast channel capacity per topic.
    #[serde(default)]
    pub channel_capacity: Option<usize>,
    /// Staleness timeout in seconds.
    #[serde(default)]
    pub staleness_timeout_secs: Option<u64>,
    /// Gap detection silence threshold in seconds.
    #[serde(default)]
    pub gap_threshold_secs: Option<u64>,
    /// Reconnection tuning.
    #[serde(default)]
    pub reconnect: Option<ReconnectSection>,
    /// Ingest via the framework engine instead of the legacy raw path.
    /// Default `false` so every existing manifest stays on the legacy path.
    #[serde(default)]
    pub framework_ingest: bool,
}

/// A single worker entry — identifies a symbol (exchange comes from collect).
#[derive(Debug, Clone, Deserialize)]
pub struct DataWorkerEntry {
    /// Trading pair (e.g. `"BTCUSDT"`).
    pub symbol: String,
    /// Override the default exchange for this worker.
    #[serde(default)]
    pub exchange: Option<String>,
    /// Override market type for this worker.
    #[serde(default)]
    pub market_type: Option<MarketType>,
    /// Override datatypes for this worker.
    #[serde(default)]
    pub datatypes: Option<DataTypesSection>,
    /// Override output sinks for this worker.
    #[serde(default)]
    pub output: Option<Vec<OutputSinkConfig>>,
    /// Override framework ingest for this worker.
    #[serde(default)]
    pub framework_ingest: Option<bool>,
}

/// Session parameters (shared with MarketWorkerManifest).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct SessionSection {
    /// Total collection duration in hours.  `None` = run until Ctrl-C.
    pub duration_hours: Option<f64>,
}

impl SessionSection {
    /// Session duration in seconds, if specified.
    pub fn duration_secs(&self) -> Option<f64> {
        self.duration_hours.map(|h| h * 3600.0)
    }
}

fn default_output_sinks() -> Vec<OutputSinkConfig> {
    vec![OutputSinkConfig::Channel]
}

impl DataWorkerManifest {
    /// Load and parse from a TOML file on disk.
    pub fn from_toml(path: &Path) -> Result<Self, ConnectError> {
        let contents =
            std::fs::read_to_string(path).map_err(|source| ConnectError::Read {
                path: format!("{path:?}"),
                source,
            })?;
        Self::from_str(&contents)
    }

    /// Parse from a TOML string (e.g. received over the wire from the control
    /// plane).
    // Intentionally not the `FromStr` trait: this parses TOML content, not a
    // canonical string round-trip.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(toml_content: &str) -> Result<Self, ConnectError> {
        let manifest: Self = toml::from_str(toml_content)
            .map_err(|e| ConnectError::Parse(format!("DataWorkerManifest: {e}")))?;
        Ok(manifest)
    }

    /// Resolve every [`DataWorkerEntry`] into a fully-specified
    /// [`DataWorkerConfig`] by merging with shared collect.
    pub fn resolve_all(&self) -> Vec<DataWorkerConfig> {
        self.workers.iter().map(|w| self.resolve_entry(w)).collect()
    }

    /// Session duration in seconds, if specified.
    pub fn duration_secs(&self) -> Option<f64> {
        self.session.duration_secs()
    }

    fn resolve_entry(&self, entry: &DataWorkerEntry) -> DataWorkerConfig {
        let d = &self.collect;
        DataWorkerConfig {
            common: CommonWorkerFields {
                exchange: entry.exchange.clone().unwrap_or_else(|| d.exchange.clone()),
                symbol: entry.symbol.clone(),
                market_type: entry.market_type.unwrap_or(d.market_type),
                environment: d.environment,
                datatypes: entry
                    .datatypes
                    .clone()
                    .unwrap_or_else(|| d.datatypes.clone()),
                channel_capacity: d.channel_capacity,
                staleness_timeout_secs: d.staleness_timeout_secs,
                gap_threshold_secs: d.gap_threshold_secs,
                reconnect: d.reconnect.clone(),
            },
            output: entry.output.clone().unwrap_or_else(|| d.output.clone()),
            framework_ingest: entry.framework_ingest.unwrap_or(d.framework_ingest),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL_TOML: &str = r#"
[collect]
exchange = "bybit"

[collect.datatypes.trades]
enabled = true

[[workers]]
symbol = "BTCUSDT"
"#;

    #[test]
    fn data_worker_environment_defaults_production_and_parses_testnet() {
        let resolved = DataWorkerManifest::from_str(MINIMAL_TOML)
            .unwrap()
            .resolve_all()
            .remove(0);
        assert_eq!(resolved.common.environment, VenueEnvironment::Production);

        let testnet = MINIMAL_TOML.replace(
            "exchange = \"bybit\"",
            "exchange = \"bybit\"\nenvironment = \"testnet\"",
        );
        let resolved = DataWorkerManifest::from_str(&testnet)
            .unwrap()
            .resolve_all()
            .remove(0);
        assert_eq!(resolved.common.environment, VenueEnvironment::Testnet);
    }

    const WITH_METADATA_TOML: &str = r#"
[metadata]
manifest_id = "mfst_init_bnd_001"
binding_id  = "bnd_001"
service_id  = "svc_bybit_btc"

[collect]
exchange = "bybit"

[collect.datatypes.trades]
enabled = true

[[workers]]
symbol = "BTCUSDT"

[[workers]]
symbol = "ETHUSDT"
"#;

    #[test]
    fn from_str_minimal() {
        let manifest = DataWorkerManifest::from_str(MINIMAL_TOML).unwrap();
        assert_eq!(manifest.collect.exchange, "bybit");
        assert_eq!(manifest.workers.len(), 1);
        assert_eq!(manifest.workers[0].symbol, "BTCUSDT");
        assert!(manifest.metadata.is_none());
    }

    #[test]
    fn from_str_with_metadata() {
        let manifest = DataWorkerManifest::from_str(WITH_METADATA_TOML).unwrap();
        let meta = manifest.metadata.as_ref().unwrap();
        assert_eq!(meta.manifest_id.as_deref(), Some("mfst_init_bnd_001"));
        assert_eq!(meta.binding_id.as_deref(), Some("bnd_001"));
        assert_eq!(meta.service_id.as_deref(), Some("svc_bybit_btc"));
        assert_eq!(manifest.workers.len(), 2);
    }

    #[test]
    fn from_str_resolves_correctly() {
        let manifest = DataWorkerManifest::from_str(WITH_METADATA_TOML).unwrap();
        let configs = manifest.resolve_all();
        assert_eq!(configs.len(), 2);
        assert_eq!(configs[0].common.exchange, "bybit");
        assert_eq!(configs[0].common.symbol, "BTCUSDT");
        assert_eq!(configs[1].common.symbol, "ETHUSDT");
    }

    #[test]
    fn from_str_invalid_toml() {
        let result = DataWorkerManifest::from_str("not valid toml {{{}");
        assert!(result.is_err());
    }

    // Regression: a minimal manifest (single datatype + one parquet output)
    // crashed the agent at parse. Real platform manifests emit
    // `path` for a parquet sink, but `OutputSinkConfig::Parquet` named the field
    // `dir` with no alias → "missing field `dir`" reported at `[[collect.output]]`.
    const PARQUET_OUTPUT_TOML: &str = r#"
[metadata]
manifest_id = "mfst_x"
binding_id  = "bnd_x"
service_id  = "svc_x"

[collect]
exchange = "binance-futures"

[collect.datatypes.orderbook]
enabled = true
depth = 20

[[collect.output]]
type = "parquet"
path = "/data/x/"

[[workers]]
symbol = "BTCUSDT"
"#;

    #[test]
    fn from_str_parquet_output_with_path() {
        let manifest = DataWorkerManifest::from_str(PARQUET_OUTPUT_TOML)
            .expect("orderbook-only + single parquet(path) manifest must parse");
        assert_eq!(
            manifest.collect.output.len(),
            1,
            "the declared sink is parsed"
        );
        match &manifest.collect.output[0] {
            OutputSinkConfig::Parquet { dir } => assert_eq!(dir, "/data/x/"),
            other => panic!("expected Parquet sink, got {other:?}"),
        }
    }

    #[test]
    fn metadata_default_is_none() {
        let meta = ManifestMetadata::default();
        assert!(meta.manifest_id.is_none());
        assert!(meta.binding_id.is_none());
        assert!(meta.service_id.is_none());
    }

    #[test]
    fn framework_ingest_defaults_false_and_resolves_overrides() {
        // Absent in TOML → false on collect and every resolved worker.
        let m = DataWorkerManifest::from_str(MINIMAL_TOML).unwrap();
        assert!(!m.collect.framework_ingest);
        assert!(!m.resolve_all()[0].framework_ingest);

        // Collect on; per-worker override turns it off for the second worker.
        let toml = r#"
[collect]
exchange = "binance"
framework_ingest = true

[collect.datatypes.orderbook]
enabled = true
depth = 50

[[workers]]
symbol = "BTCUSDT"

[[workers]]
symbol = "ETHUSDT"
framework_ingest = false
"#;
        let m = DataWorkerManifest::from_str(toml).unwrap();
        assert!(m.collect.framework_ingest);
        let cfgs = m.resolve_all();
        assert!(cfgs[0].framework_ingest, "inherits collect");
        assert!(!cfgs[1].framework_ingest, "per-worker override wins");
    }

    #[test]
    fn metadata_partial_fields() {
        let toml = r#"
[metadata]
binding_id = "bnd_partial"

[collect]
exchange = "bybit"

[collect.datatypes.trades]
enabled = true

[[workers]]
symbol = "BTCUSDT"
"#;
        let manifest = DataWorkerManifest::from_str(toml).unwrap();
        let meta = manifest.metadata.as_ref().unwrap();
        assert_eq!(meta.binding_id.as_deref(), Some("bnd_partial"));
        assert!(meta.manifest_id.is_none());
        assert!(meta.service_id.is_none());
    }
}
