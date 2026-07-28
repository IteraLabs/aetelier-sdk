//! Multi-worker manifest configuration.
//!
//! [`WorkerManifest`] is the top-level config for multi-symbol collection
//! runs.  It declares shared defaults and a list of per-symbol workers,
//! each of which resolves into a full [`MarketSnapshotConfig`].
//!
//! # Example TOML
//!
//! ```toml
//! [defaults]
//! sync_mode = "on_trade"
//! flush_threshold = 36000
//!
//! [defaults.update_frequency]
//! value = 100
//! unit  = "Millis"
//!
//! [defaults.datatypes.orderbook]
//! enabled = true
//! depth   = 50
//!
//! [defaults.datatypes.trades]
//! enabled = true
//!
//! [defaults.datatypes.liquidations]
//! enabled = true
//!
//! [defaults.datatypes.funding_rates]
//! enabled = true
//!
//! [defaults.datatypes.open_interest]
//! enabled = true
//!
//! [defaults.logs]
//! n_orderbooks    = 500
//! n_trades        = 500
//! n_liquidations  = 1
//! n_fundings      = 50
//! n_open_interests = 50
//!
//! [[workers]]
//! exchange = "bybit"
//! symbol   = "BTCUSDT"
//!
//! [[workers]]
//! exchange = "bybit"
//! symbol   = "ETHUSDT"
//! sync_mode = "on_orderbook"   # per-worker override
//!
//! [output]
//! base_dir = "datasets/collected"
//!
//! [session]
//! duration_hours = 8
//! ```

use serde::Deserialize;

use crate::errors::ConfigError;

#[cfg(feature = "std")]
use std::path::Path;

use super::market_config::{
    DataTypesSection, ExchangeSection, LogsSection, MarketSnapshotConfig, OutputSection,
    PipelineSection, SymbolSection, SyncMode, UpdateFrequency,
};

// ─────────────────────────────────────────────────────────────────────────────
// Root manifest
// ─────────────────────────────────────────────────────────────────────────────

/// Top-level multi-worker manifest deserialized from TOML.
///
/// Contains shared default settings, a list of per-symbol worker entries,
/// output configuration, and session parameters.  Call [`resolve_all()`] to
/// expand every [`WorkerEntry`] into a fully-specified
/// [`MarketSnapshotConfig`].
///
/// [`resolve_all()`]: WorkerManifest::resolve_all
#[derive(Debug, Clone, Deserialize)]
pub struct WorkerManifest {
    /// Shared default settings applied to all workers.
    pub defaults: DefaultsSection,
    /// Per-symbol worker definitions.
    pub workers: Vec<WorkerEntry>,
    /// Output directory configuration.
    pub output: ManifestOutputSection,
    /// Session parameters (duration, etc.).
    #[serde(default)]
    pub session: SessionSection,
}

// ─────────────────────────────────────────────────────────────────────────────
// Sections
// ─────────────────────────────────────────────────────────────────────────────

/// Shared defaults applied to every worker unless overridden.
#[derive(Debug, Clone, Deserialize)]
pub struct DefaultsSection {
    /// Default sync mode for all workers.
    pub sync_mode: SyncMode,
    /// Grid ticks before flushing to Parquet.
    pub flush_threshold: usize,
    /// Grid spacing (value + unit).
    pub update_frequency: UpdateFrequency,
    /// Which data feeds to subscribe to.
    pub datatypes: DataTypesSection,
    /// Per-event-type print frequency thresholds.
    pub logs: LogsSection,
}

/// A single worker entry — identifies an exchange + symbol pair.
///
/// All other settings come from [`DefaultsSection`] unless explicitly
/// overridden here.
#[derive(Debug, Clone, Deserialize)]
pub struct WorkerEntry {
    /// Target exchange (e.g. `"bybit"`).
    pub exchange: String,
    /// Trading pair (e.g. `"BTCUSDT"`).
    pub symbol: String,
    /// Override the default sync mode for this worker.
    pub sync_mode: Option<SyncMode>,
}

/// `[output]` — base directory for all workers.
///
/// Each worker writes to `{base_dir}/{exchange}/{symbol}/`.
#[derive(Debug, Clone, Deserialize)]
pub struct ManifestOutputSection {
    /// Root directory; per-worker subdirectories are created automatically.
    pub base_dir: String,
}

/// `[session]` — optional run-time parameters.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct SessionSection {
    /// Total collection duration in hours.  `None` = run until Ctrl-C.
    pub duration_hours: Option<f64>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Impl
// ─────────────────────────────────────────────────────────────────────────────

impl WorkerManifest {
    /// Load and parse a [`WorkerManifest`] from a TOML file (requires `std`).
    #[cfg(feature = "std")]
    pub fn from_toml(path: &Path) -> Result<Self, ConfigError> {
        let contents =
            std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
                path: format!("{path:?}"),
                source,
            })?;
        let manifest: Self =
            toml::from_str(&contents).map_err(|e| ConfigError::Parse(Box::new(e)))?;
        Ok(manifest)
    }

    /// Parse from an in-memory TOML string (works in native and WASM).
    pub fn from_toml_str(contents: &str) -> Result<Self, ConfigError> {
        let manifest: Self =
            toml::from_str(contents).map_err(|e| ConfigError::Parse(Box::new(e)))?;
        Ok(manifest)
    }

    /// Resolve every [`WorkerEntry`] into a fully-specified
    /// [`MarketSnapshotConfig`] by merging with shared defaults.
    pub fn resolve_all(&self) -> Vec<MarketSnapshotConfig> {
        self.workers
            .iter()
            .map(|w| w.resolve(&self.defaults, &self.output))
            .collect()
    }

    /// Session duration in seconds, if specified.
    pub fn duration_secs(&self) -> Option<f64> {
        self.session.duration_hours.map(|h| h * 3600.0)
    }
}

impl WorkerEntry {
    /// Merge this worker entry with shared defaults to produce a
    /// fully-specified [`MarketSnapshotConfig`].
    ///
    /// The output directory is automatically partitioned:
    /// `{base_dir}/{exchange}/{symbol}/`.
    pub fn resolve(
        &self,
        defaults: &DefaultsSection,
        output: &ManifestOutputSection,
    ) -> MarketSnapshotConfig {
        MarketSnapshotConfig {
            exchange: ExchangeSection {
                name: self.exchange.clone(),
            },
            symbol: SymbolSection {
                name: self.symbol.clone(),
                sync_mode: self.sync_mode.unwrap_or(defaults.sync_mode),
            },
            update_frequency: defaults.update_frequency.clone(),
            pipeline: PipelineSection {
                flush_threshold: defaults.flush_threshold,
            },
            datatypes: defaults.datatypes.clone(),
            output: OutputSection {
                dir: format!("{}/{}/{}", output.base_dir, self.exchange, self.symbol,),
            },
            logs: defaults.logs.clone(),
        }
    }
}
