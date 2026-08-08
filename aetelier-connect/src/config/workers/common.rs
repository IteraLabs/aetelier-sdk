//! Shared configuration types for all worker variants.
//!
//! [`CommonWorkerFields`] captures the configuration knobs that every worker
//! needs regardless of whether it synchronises events or not.

use std::time::Duration;

use serde::Deserialize;

use crate::clients::connection_manager::ConnectionManagerConfig;
use aetelier_types::config::markets::market_config::DataTypesSection;
use aetelier_types::config::markets::market_config::MarketSnapshotConfig;
use aetelier_types::exchanges::{MarketType, VenueEnvironment};

// ─────────────────────────────────────────────────────────────────────────────
// CommonWorkerFields
// ─────────────────────────────────────────────────────────────────────────────

/// Configuration fields shared by both [`DataWorker`](crate::workers::DataWorker) and [`MarketWorker`](crate::workers::MarketWorker).
///
/// These are the *only* fields needed to stand up an ingestion pipeline:
/// which exchange, which symbol, which datatypes, and tuning knobs for
/// reconnection / health monitoring.
#[derive(Debug, Clone, Deserialize)]
pub struct CommonWorkerFields {
    /// Exchange identifier (e.g. `"bybit"`, `"coinbase"`, `"kraken"`).
    pub exchange: String,
    /// Trading pair (e.g. `"BTCUSDT"`, `"BTC-USD"`).
    pub symbol: String,
    /// Instrument market type (spot, perpetual, inverse).
    /// Defaults to `Spot` when not specified.
    #[serde(default)]
    pub market_type: MarketType,
    #[serde(default)]
    pub environment: VenueEnvironment,
    /// Which data feeds to subscribe to.
    pub datatypes: DataTypesSection,
    /// Broadcast channel capacity per topic (default: 8192).
    #[serde(default)]
    pub channel_capacity: Option<usize>,
    /// Staleness timeout in seconds (default: 60).
    #[serde(default)]
    pub staleness_timeout_secs: Option<u64>,
    /// Gap detection silence threshold in seconds (default: 5).
    #[serde(default)]
    pub gap_threshold_secs: Option<u64>,
    /// Reconnection tuning knobs.
    #[serde(default)]
    pub reconnect: Option<ReconnectSection>,
}

/// TOML-exposed reconnection knobs.
///
/// Maps directly to [`ConnectionManagerConfig`] on the legacy path
/// (defaults 100 ms / 10 s). The framework ingest path resolves unset
/// fields to its own defaults (1 s / 30 s / infinite / 0.5) — see
/// `MarketWorker::run_framework`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ReconnectSection {
    /// Base delay before the first retry (ms). Default: 100.
    #[serde(default)]
    pub initial_delay_ms: Option<u64>,
    /// Upper bound on exponential backoff (ms). Default: 10_000.
    #[serde(default)]
    pub max_delay_ms: Option<u64>,
    /// Maximum consecutive failures before circuit breaker opens.
    /// `None` = infinite retries.
    #[serde(default)]
    pub max_attempts: Option<u32>,
    /// Fraction of the base delay added as uniform random jitter.
    /// Default: 0.5.
    #[serde(default)]
    pub jitter_factor: Option<f64>,
}

impl ReconnectSection {
    /// Convert to a [`ConnectionManagerConfig`], filling defaults for
    /// any unset fields.
    pub fn to_connection_manager_config(&self) -> ConnectionManagerConfig {
        let defaults = ConnectionManagerConfig::default();
        ConnectionManagerConfig {
            initial_delay: self
                .initial_delay_ms
                .map(Duration::from_millis)
                .unwrap_or(defaults.initial_delay),
            max_delay: self
                .max_delay_ms
                .map(Duration::from_millis)
                .unwrap_or(defaults.max_delay),
            max_attempts: self.max_attempts.or(defaults.max_attempts),
            jitter_factor: self.jitter_factor.unwrap_or(defaults.jitter_factor),
        }
    }
}

impl CommonWorkerFields {
    /// Resolved channel capacity with fallback to the library default.
    pub fn channel_capacity(&self) -> usize {
        self.channel_capacity
            .unwrap_or(crate::workers::topic_publisher::DEFAULT_CHANNEL_CAPACITY)
    }

    /// Resolved staleness timeout with fallback to 60 s.
    pub fn staleness_timeout(&self) -> Duration {
        Duration::from_secs(self.staleness_timeout_secs.unwrap_or(60))
    }

    /// Resolved gap detection threshold with fallback to 5 s.
    pub fn gap_threshold(&self) -> Duration {
        Duration::from_secs(self.gap_threshold_secs.unwrap_or(5))
    }

    /// Resolved reconnection config with fallback to library defaults.
    pub fn reconnect_config(&self) -> ConnectionManagerConfig {
        self.reconnect
            .as_ref()
            .map(|r| r.to_connection_manager_config())
            .unwrap_or_default()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// From<MarketSnapshotConfig>
// ─────────────────────────────────────────────────────────────────────────────

impl From<&MarketSnapshotConfig> for CommonWorkerFields {
    /// Convert a legacy [`MarketSnapshotConfig`] into shared worker fields.
    ///
    /// All optional tuning knobs default to `None` (= library defaults).
    fn from(cfg: &MarketSnapshotConfig) -> Self {
        Self {
            exchange: cfg.exchange.name.clone(),
            symbol: cfg.symbol.name.clone(),
            market_type: MarketType::default(),
            environment: VenueEnvironment::default(),
            datatypes: cfg.datatypes.clone(),
            channel_capacity: None,
            staleness_timeout_secs: None,
            gap_threshold_secs: None,
            reconnect: None,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ManifestMetadata — wire-received manifest identity
// ─────────────────────────────────────────────────────────────────────────────

/// Identity metadata injected into a manifest received over the wire.
///
/// These fields are **not** part of the user-authored TOML — they are
/// added by an orchestrating harness so a worker knows which binding,
/// service, and manifest it is operating under.  They are optional so
/// that locally-loaded manifests (from disk) still parse correctly.
///
/// # TOML example (as delivered by the control plane)
///
/// ```toml
/// [metadata]
/// manifest_id = "mfst_init_bnd_001"
/// binding_id  = "bnd_001"
/// service_id  = "svc_bybit_btc"
/// ```
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ManifestMetadata {
    /// The platform-assigned manifest identifier.
    #[serde(default)]
    pub manifest_id: Option<String>,
    /// The binding that this agent was assigned to.
    #[serde(default)]
    pub binding_id: Option<String>,
    /// The service definition that originated this binding.
    #[serde(default)]
    pub service_id: Option<String>,
}

// ─────────────────────────────────────────────────────────────────────────────
// OutputSinkConfig
// ─────────────────────────────────────────────────────────────────────────────

/// Describes a single output sink, deserializable from TOML.
///
/// Multiple sinks can be active simultaneously — events are fanned out
/// to every configured sink.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum OutputSinkConfig {
    /// Publish to in-memory broadcast channels (existing `TopicRegistry`).
    Channel,
    /// Print events to the terminal via `tracing::debug!`.
    Terminal,
    /// Write events to local Parquet files.
    Parquet {
        /// Base directory for Parquet output. Accept `path` (the key every
        /// platform manifest producers actually emit) as
        /// well as `dir`; previously only `dir` was accepted, so a real
        /// `type = "parquet"\npath = "…"` block failed with "missing field
        /// `dir`" and crashed the agent at manifest parse.
        #[serde(alias = "path")]
        dir: String,
    },
}
