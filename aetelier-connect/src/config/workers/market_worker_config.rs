//! Configuration for [`MarketWorker`](crate::workers::market_worker::MarketWorker).
//!
//! [`MarketWorkerConfig`] extends [`CommonWorkerFields`] with synchronisation
//! parameters (clock mode, grid spacing, flush threshold).
//!
//! [`MarketWorkerManifest`] is the multi-worker TOML manifest that resolves
//! into a `Vec<MarketWorkerConfig>`.

use serde::Deserialize;
use std::path::Path;

use super::common::{
    CommonWorkerFields, ManifestMetadata, OutputSinkConfig, ReconnectSection,
};
use aetelier_types::config::markets::market_config::{
    DataTypesSection, MarketSnapshotConfig, SyncMode, TimeUnit, UpdateFrequency,
};
use aetelier_types::exchanges::MarketType;

use crate::errors::ConnectError;

// ──────────────────────────────────────────────────────────────────────────────────────
// SyncSection
// ──────────────────────────────────────────────────────────────────────────────────────

/// Synchronisation parameters for the MarketWorker.
#[derive(Debug, Clone, Deserialize)]
pub struct SyncSection {
    /// Which event drives the grid clock.
    pub sync_mode: SyncMode,
    /// Grid spacing (value + unit).
    pub update_frequency: UpdateFrequency,
    /// Grid ticks before flushing / emitting a batch.
    pub flush_threshold: usize,
}

impl SyncSection {
    /// Grid period in microseconds (the platform timestamp standard;
    /// sub-microsecond configs round down).
    pub fn period_us(&self) -> u64 {
        let v = self.update_frequency.value;
        match self.update_frequency.unit {
            TimeUnit::Nanos => v / 1_000,
            TimeUnit::Micros => v,
            TimeUnit::Millis => v * 1_000,
            TimeUnit::Secs => v * 1_000_000,
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────────────
// ReconcileSection
// ──────────────────────────────────────────────────────────────────────────────────────

/// Live-reconciliation parameters (`[collect.reconcile]`, optional; a metered
/// Pro feature). Only meaningful for the synchronized MarketWorker at
/// cadence ≥ 50ms — validated loudly at worker construction.
#[derive(Debug, Clone, Deserialize)]
pub struct ReconcileSection {
    /// Master switch. `false` behaves as if the section were absent.
    #[serde(default)]
    pub enabled: bool,
    /// Emission hold-back window W (value + unit). Rows emit W after their
    /// boundary so REST-recovered prints land in their true rows. Default 1s
    /// (≥ venue REST RTT + margin) when omitted.
    #[serde(default)]
    pub emission_delay: Option<UpdateFrequency>,
    /// Periodic sweep cadence in seconds (0 disables the sweep; incident-
    /// triggered fetches still run). Default 60.
    #[serde(default = "default_sweep_secs")]
    pub sweep_secs: u64,
}

fn default_sweep_secs() -> u64 {
    60
}

impl ReconcileSection {
    /// The hold-back window in microseconds (default 1s).
    pub fn emission_delay_us(&self) -> u64 {
        match &self.emission_delay {
            None => 1_000_000,
            Some(f) => match f.unit {
                TimeUnit::Nanos => f.value / 1_000,
                TimeUnit::Micros => f.value,
                TimeUnit::Millis => f.value * 1_000,
                TimeUnit::Secs => f.value * 1_000_000,
            },
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────────────
// MarketWorkerConfig — per-worker resolved config
// ──────────────────────────────────────────────────────────────────────────────────────

/// Fully-resolved configuration for a single [`MarketWorker`](crate::workers::market_worker::MarketWorker).
#[derive(Debug, Clone)]
pub struct MarketWorkerConfig {
    /// Shared worker fields (exchange, symbol, datatypes, tuning).
    pub common: CommonWorkerFields,
    /// Synchronisation parameters.
    pub sync: SyncSection,
    /// Output sinks.
    pub output: Vec<OutputSinkConfig>,
    /// Reconstruct the book through the framework adapter + runtime instead of
    /// the legacy per-exchange ingestion path. Only takes effect when the venue
    /// is registered *and* REST-seeded and the enabled datatypes are a subset of
    /// {orderbook, trades}; otherwise the worker falls back to the legacy path.
    /// Default `false`.
    pub framework_ingest: bool,
    /// Live-reconciliation settings (`None` = feature off).
    pub reconcile: Option<ReconcileSection>,
}

impl MarketWorkerConfig {
    /// Create from a legacy [`MarketSnapshotConfig`] with a channel-only
    /// sink.  Used for backward compatibility.
    pub fn from_legacy(cfg: &MarketSnapshotConfig) -> Self {
        Self {
            common: CommonWorkerFields::from(cfg),
            sync: SyncSection {
                sync_mode: cfg.symbol.sync_mode,
                update_frequency: cfg.update_frequency.clone(),
                flush_threshold: cfg.pipeline.flush_threshold,
            },
            output: vec![OutputSinkConfig::Channel],
            framework_ingest: false,
            reconcile: None,
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────────────
// MarketWorkerManifest — multi-worker TOML manifest
// ──────────────────────────────────────────────────────────────────────────────────────

/// Top-level TOML manifest for spawning multiple [`MarketWorker`](crate::workers::market_worker::MarketWorker)s.
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
/// [collect.sync]
/// sync_mode = "on_trade"
/// flush_threshold = 36000
///
/// [collect.sync.update_frequency]
/// value = 100
/// unit  = "Millis"
///
/// [[collect.output]]
/// type = "channel"
///
/// [[workers]]
/// symbol = "BTCUSDT"
///
/// [[workers]]
/// symbol = "ETHUSDT"
/// sync_mode = "on_orderbook"
///
/// [session]
/// duration_hours = 8
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct MarketWorkerManifest {
    /// Shared collectors applied to all workers.
    pub collect: MarketWorkerCollect,
    /// Per-symbol worker definitions.
    pub workers: Vec<MarketWorkerEntry>,
    /// Session parameters.
    #[serde(default)]
    pub session: super::data_worker_config::SessionSection,
    /// Platform-injected identity metadata (present when received over the wire).
    #[serde(default)]
    pub metadata: Option<ManifestMetadata>,
}

/// Shared collect instructions for a [`MarketWorkerManifest`].
#[derive(Debug, Clone, Deserialize)]
pub struct MarketWorkerCollect {
    /// Default exchange for all workers.
    pub exchange: String,
    /// Default market type (spot, perpetual, inverse).
    #[serde(default)]
    pub market_type: MarketType,
    /// Which data feeds to subscribe to.
    pub datatypes: DataTypesSection,
    /// Synchronisation parameters.
    pub sync: SyncSection,
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
    /// Live-reconciliation section (`[collect.reconcile]`, optional).
    #[serde(default)]
    pub reconcile: Option<ReconcileSection>,
    /// Ingest via the framework engine instead of the legacy raw path.
    /// Lives at `[collect]` (not `[collect.sync]`) so the platform's single
    /// `[collect]`-level injector reaches both Data and Market manifests.
    /// Default `false` so every existing manifest stays on the legacy path.
    #[serde(default)]
    pub framework_ingest: bool,
}

/// A single worker entry in a [`MarketWorkerManifest`].
#[derive(Debug, Clone, Deserialize)]
pub struct MarketWorkerEntry {
    /// Trading pair (e.g. `"BTCUSDT"`).
    pub symbol: String,
    /// Override the default exchange.
    #[serde(default)]
    pub exchange: Option<String>,
    /// Override market type for this worker.
    #[serde(default)]
    pub market_type: Option<MarketType>,
    /// Override sync_mode for this worker.
    #[serde(default)]
    pub sync_mode: Option<SyncMode>,
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

fn default_output_sinks() -> Vec<OutputSinkConfig> {
    vec![OutputSinkConfig::Channel]
}

impl MarketWorkerManifest {
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
            .map_err(|e| ConnectError::Parse(format!("MarketWorkerManifest: {e}")))?;
        Ok(manifest)
    }

    /// Resolve every [`MarketWorkerEntry`] into a fully-specified
    /// [`MarketWorkerConfig`].
    pub fn resolve_all(&self) -> Vec<MarketWorkerConfig> {
        self.workers.iter().map(|w| self.resolve_entry(w)).collect()
    }

    /// Session duration in seconds, if specified.
    pub fn duration_secs(&self) -> Option<f64> {
        self.session.duration_secs()
    }

    fn resolve_entry(&self, entry: &MarketWorkerEntry) -> MarketWorkerConfig {
        let d = &self.collect;
        MarketWorkerConfig {
            common: CommonWorkerFields {
                exchange: entry.exchange.clone().unwrap_or_else(|| d.exchange.clone()),
                symbol: entry.symbol.clone(),
                market_type: entry.market_type.unwrap_or(d.market_type),
                datatypes: entry
                    .datatypes
                    .clone()
                    .unwrap_or_else(|| d.datatypes.clone()),
                channel_capacity: d.channel_capacity,
                staleness_timeout_secs: d.staleness_timeout_secs,
                gap_threshold_secs: d.gap_threshold_secs,
                reconnect: d.reconnect.clone(),
            },
            sync: SyncSection {
                sync_mode: entry.sync_mode.unwrap_or(d.sync.sync_mode),
                update_frequency: d.sync.update_frequency.clone(),
                flush_threshold: d.sync.flush_threshold,
            },
            output: entry.output.clone().unwrap_or_else(|| d.output.clone()),
            framework_ingest: entry.framework_ingest.unwrap_or(d.framework_ingest),
            reconcile: d.reconcile.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconcile_section_parses_with_defaults_and_resolves() {
        let toml = r#"
[collect]
exchange = "binance"
framework_ingest = true

[collect.datatypes.orderbook]
enabled = true
depth = 25

[collect.datatypes.trades]
enabled = true

[collect.sync]
sync_mode = "on_time"
flush_threshold = 600

[collect.sync.update_frequency]
value = 100
unit = "Millis"

[collect.reconcile]
enabled = true

[[workers]]
symbol = "BTCUSDT"
"#;
        let m = MarketWorkerManifest::from_str(toml).unwrap();
        let cfgs = m.resolve_all();
        let r = cfgs[0].reconcile.as_ref().expect("section resolved");
        assert!(r.enabled);
        assert_eq!(r.emission_delay_us(), 1_000_000, "default W = 1s");
        assert_eq!(r.sweep_secs, 60, "default sweep");
    }

    #[test]
    fn reconcile_absent_resolves_none() {
        let m = MarketWorkerManifest::from_str(MINIMAL_MARKET_TOML).unwrap();
        assert!(m.resolve_all()[0].reconcile.is_none());
    }

    const MINIMAL_MARKET_TOML: &str = r#"
[collect]
exchange = "bybit"

[collect.datatypes.orderbook]
enabled = true
depth   = 50

[collect.datatypes.trades]
enabled = true

[collect.sync]
sync_mode = "on_trade"
flush_threshold = 36000

[collect.sync.update_frequency]
value = 100
unit  = "Millis"

[[workers]]
symbol = "BTCUSDT"
"#;

    const WITH_METADATA_MARKET_TOML: &str = r#"
[metadata]
manifest_id = "mfst_mkt_001"
binding_id  = "bnd_mkt_001"
service_id  = "svc_market"

[collect]
exchange = "bybit"

[collect.datatypes.orderbook]
enabled = true
depth   = 50

[collect.datatypes.trades]
enabled = true

[collect.sync]
sync_mode = "on_trade"
flush_threshold = 36000

[collect.sync.update_frequency]
value = 100
unit  = "Millis"

[[workers]]
symbol = "BTCUSDT"

[[workers]]
symbol = "ETHUSDT"
sync_mode = "on_orderbook"
"#;

    #[test]
    fn from_str_minimal_market() {
        let manifest = MarketWorkerManifest::from_str(MINIMAL_MARKET_TOML).unwrap();
        assert_eq!(manifest.collect.exchange, "bybit");
        assert_eq!(manifest.workers.len(), 1);
        assert_eq!(manifest.workers[0].symbol, "BTCUSDT");
        assert!(manifest.metadata.is_none());
    }

    #[test]
    fn from_str_with_metadata_market() {
        let manifest = MarketWorkerManifest::from_str(WITH_METADATA_MARKET_TOML).unwrap();
        let meta = manifest.metadata.as_ref().unwrap();
        assert_eq!(meta.manifest_id.as_deref(), Some("mfst_mkt_001"));
        assert_eq!(meta.binding_id.as_deref(), Some("bnd_mkt_001"));
        assert_eq!(meta.service_id.as_deref(), Some("svc_market"));
        assert_eq!(manifest.workers.len(), 2);
    }

    #[test]
    fn from_str_resolves_with_sync_override() {
        let manifest = MarketWorkerManifest::from_str(WITH_METADATA_MARKET_TOML).unwrap();
        let configs = manifest.resolve_all();
        assert_eq!(configs.len(), 2);
        // First worker inherits sync_mode from collect
        assert_eq!(configs[0].sync.sync_mode, SyncMode::OnTrade);
        // Second worker overrides sync_mode
        assert_eq!(configs[1].sync.sync_mode, SyncMode::OnOrderbook);
    }

    #[test]
    fn from_str_invalid_toml() {
        let result = MarketWorkerManifest::from_str("not valid toml");
        assert!(result.is_err());
    }

    #[test]
    fn period_us_millis() {
        let manifest = MarketWorkerManifest::from_str(MINIMAL_MARKET_TOML).unwrap();
        // 100 Millis = 100_000 us
        assert_eq!(manifest.collect.sync.period_us(), 100_000);
    }

    #[test]
    fn framework_ingest_reads_from_collect_and_resolves_overrides() {
        // Absent in TOML → false on collect and every resolved worker.
        let m = MarketWorkerManifest::from_str(MINIMAL_MARKET_TOML).unwrap();
        assert!(!m.collect.framework_ingest);
        assert!(!m.resolve_all()[0].framework_ingest);

        // `framework_ingest = true` at the `[collect]` level (exactly where the
        // the platform manifest producer injects it) is read by the Market
        // manifest; a per-worker entry overrides it back to false.
        let toml = r#"
[collect]
exchange = "binance"
framework_ingest = true

[collect.datatypes.orderbook]
enabled = true
depth = 50

[collect.sync]
sync_mode = "on_trade"
flush_threshold = 36000

[collect.sync.update_frequency]
value = 100
unit  = "Millis"

[[workers]]
symbol = "BTCUSDT"

[[workers]]
symbol = "ETHUSDT"
framework_ingest = false
"#;
        let m = MarketWorkerManifest::from_str(toml).unwrap();
        assert!(m.collect.framework_ingest);
        let cfgs = m.resolve_all();
        assert!(cfgs[0].framework_ingest, "inherits collect");
        assert!(!cfgs[1].framework_ingest, "per-worker override wins");
    }

    /// Regression guard: a stray `framework_ingest` under `[collect.sync]` (the
    /// old, broken location) must NOT bind — `SyncSection` ignores the unknown
    /// key and the flag stays at its `[collect]`-level default of false.
    #[test]
    fn framework_ingest_under_sync_is_ignored() {
        let toml = r#"
[collect]
exchange = "binance"

[collect.datatypes.orderbook]
enabled = true
depth = 50

[collect.sync]
sync_mode = "on_trade"
flush_threshold = 36000
framework_ingest = true

[collect.sync.update_frequency]
value = 100
unit  = "Millis"

[[workers]]
symbol = "BTCUSDT"
"#;
        let m = MarketWorkerManifest::from_str(toml).unwrap();
        assert!(
            !m.collect.framework_ingest,
            "framework_ingest under [collect.sync] is no longer a recognized key"
        );
        assert!(!m.resolve_all()[0].framework_ingest);
    }
}
