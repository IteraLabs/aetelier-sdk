//! Configuration for market snapshot collection pipelines.
//!
//! [`MarketSnapshotConfig`] is the top-level struct deserialized from a TOML
//! file that drives the `bybit_markets` (and future exchange) examples.
//!
//! # Example TOML
//!
//! ```toml
//! [exchange]
//! name = "bybit"
//!
//! [symbol]
//! name = "BTCUSDT"
//! sync_mode = "on_trade"
//!
//! [update_frequency]
//! value = 100
//! unit = "Millis"
//!
//! [pipeline]
//! flush_threshold = 36000
//!
//! [datatypes.orderbook]
//! enabled = true
//! depth = 50
//!
//! [datatypes.trades]
//! enabled = true
//!
//! [datatypes.liquidations]
//! enabled = true
//!
//! [datatypes.funding_rates]
//! enabled = true
//!
//! [datatypes.open_interest]
//! enabled = true
//!
//! [logs]
//! n_orderbooks = 100
//! n_trades = 10
//! n_liquidations = 1
//! n_fundings = 10
//! n_open_interests = 10
//!
//! [output]
//! dir = "datasets/collected/bybit/market_snapshots"
//! ```

use serde::Deserialize;

#[cfg(feature = "std")]
use std::path::{Path, PathBuf};

use crate::errors::ConfigError;
use crate::synchronizers::ClockMode;

// ─────────────────────────────────────────────────────────────────────────────
// Root config
// ─────────────────────────────────────────────────────────────────────────────

/// Top-level market snapshot configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct MarketSnapshotConfig {
    /// Exchange identification section.
    pub exchange: ExchangeSection,
    /// Trading symbol and synchronization mode.
    pub symbol: SymbolSection,
    /// Grid update frequency configuration.
    pub update_frequency: UpdateFrequency,
    /// Pipeline flush settings.
    pub pipeline: PipelineSection,
    /// Data feed configuration.
    #[serde(default)]
    pub datatypes: DataTypesSection,
    /// Output directory settings.
    pub output: OutputSection,
    /// Event logging configuration.
    #[serde(default)]
    pub logs: LogsSection,
}

// ─────────────────────────────────────────────────────────────────────────────
// Sections
// ─────────────────────────────────────────────────────────────────────────────

/// `[exchange]` — identifies the target exchange.
#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeSection {
    /// Name of the exchange.
    pub name: String,
}

/// `[symbol]` — the instrument to collect and how to synchronize.
#[derive(Debug, Clone, Deserialize)]
pub struct SymbolSection {
    /// Trading pair (e.g. `"BTCUSDT"`).
    pub name: String,
    /// Which event drives the grid clock.
    pub sync_mode: SyncMode,
}

/// `[update_frequency]` — grid spacing expressed as value + unit.
#[derive(Debug, Clone, Deserialize)]
pub struct UpdateFrequency {
    /// Numeric part of the frequency (e.g. `100`).
    pub value: u64,
    /// Time unit for `value`.
    pub unit: TimeUnit,
}

/// `[pipeline]` — flush cadence.
///
/// The flush interval in wall-clock time is
/// `flush_threshold × update_frequency`. For example, with a 100 ms
/// grid and `flush_threshold = 36000`, one Parquet file is written
/// every hour.
///
/// The process runs continuously until interrupted (Ctrl-C).
#[derive(Debug, Clone, Deserialize)]
pub struct PipelineSection {
    /// Flush buffered snapshots to Parquet after this many grid
    /// periods accumulate. Units: number of `update_frequency` ticks.
    pub flush_threshold: usize,
}

/// `[datatypes]` — selects which data feeds to subscribe to and collect.
///
/// Each feed is its own sub-table so exchange- or feed-specific parameters
/// can be added without polluting a flat namespace.
///
/// ```toml
/// [datatypes.orderbook]
/// enabled = true
/// depth   = 50
///
/// [datatypes.trades]
/// enabled = true
///
/// [datatypes.liquidations]
/// enabled = false
/// ```
#[derive(Debug, Clone, Default, Deserialize)]
pub struct DataTypesSection {
    /// Orderbook feed configuration.
    #[serde(default)]
    pub orderbook: OrderbookConfig,
    /// Public trades feed.
    #[serde(default)]
    pub trades: FeedToggle,
    /// Liquidation events feed.
    #[serde(default)]
    pub liquidations: FeedToggle,
    /// Funding rate updates feed.
    #[serde(default)]
    pub funding_rates: FeedToggle,
    /// Open interest updates feed.
    #[serde(default)]
    pub open_interest: FeedToggle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DeclaredDatatype {
    Orderbook,
    Trades,
    Liquidations,
    FundingRates,
    OpenInterest,
}

impl DeclaredDatatype {
    pub const ALL: [DeclaredDatatype; 5] = [
        DeclaredDatatype::Orderbook,
        DeclaredDatatype::Trades,
        DeclaredDatatype::Liquidations,
        DeclaredDatatype::FundingRates,
        DeclaredDatatype::OpenInterest,
    ];

    pub fn id(&self) -> &'static str {
        match self {
            DeclaredDatatype::Orderbook => "orderbook",
            DeclaredDatatype::Trades => "trades",
            DeclaredDatatype::Liquidations => "liquidations",
            DeclaredDatatype::FundingRates => "funding_rates",
            DeclaredDatatype::OpenInterest => "open_interest",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeclaredSet {
    enabled: std::collections::BTreeSet<DeclaredDatatype>,
}

impl DeclaredSet {
    pub fn from_section(section: &DataTypesSection) -> Self {
        let mut enabled = std::collections::BTreeSet::new();
        for dt in DeclaredDatatype::ALL {
            let on = match dt {
                DeclaredDatatype::Orderbook => section.orderbook.enabled,
                DeclaredDatatype::Trades => section.trades.enabled,
                DeclaredDatatype::Liquidations => section.liquidations.enabled,
                DeclaredDatatype::FundingRates => section.funding_rates.enabled,
                DeclaredDatatype::OpenInterest => section.open_interest.enabled,
            };
            if on {
                enabled.insert(dt);
            }
        }
        Self { enabled }
    }

    pub fn all() -> Self {
        Self {
            enabled: DeclaredDatatype::ALL.into_iter().collect(),
        }
    }

    pub fn only(dt: DeclaredDatatype) -> Self {
        let mut enabled = std::collections::BTreeSet::new();
        enabled.insert(dt);
        Self { enabled }
    }

    pub fn without(&self, dt: DeclaredDatatype) -> Self {
        let mut enabled = self.enabled.clone();
        enabled.remove(&dt);
        Self { enabled }
    }

    pub fn contains(&self, dt: DeclaredDatatype) -> bool {
        self.enabled.contains(&dt)
    }

    pub fn iter(&self) -> impl Iterator<Item = DeclaredDatatype> + '_ {
        self.enabled.iter().copied()
    }

    pub fn is_empty(&self) -> bool {
        self.enabled.is_empty()
    }

    pub fn len(&self) -> usize {
        self.enabled.len()
    }
}

impl DataTypesSection {
    pub fn declared_set(&self) -> DeclaredSet {
        DeclaredSet::from_section(self)
    }
}

impl DataTypesSection {
    /// Return the names of all enabled feeds as lowercase strings.
    ///
    /// The order is deterministic: orderbook, trades, liquidations,
    /// funding_rates, open_interest.
    pub fn enabled_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        if self.orderbook.enabled {
            names.push("orderbook".into());
        }
        if self.trades.enabled {
            names.push("trades".into());
        }
        if self.liquidations.enabled {
            names.push("liquidations".into());
        }
        if self.funding_rates.enabled {
            names.push("funding_rates".into());
        }
        if self.open_interest.enabled {
            names.push("open_interest".into());
        }
        names
    }
}

/// Configuration for the orderbook data feed.
///
/// Besides the `enabled` toggle shared with all feeds, orderbook has a
/// `depth` parameter controlling how many price levels to request.
#[derive(Debug, Clone, Deserialize)]
pub struct OrderbookConfig {
    /// Whether to subscribe to orderbook deltas / snapshots.
    #[serde(default)]
    pub enabled: bool,
    /// Number of price levels to request (e.g. 25, 50).
    #[serde(default = "default_ob_depth")]
    pub depth: usize,
}

impl Default for OrderbookConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            depth: default_ob_depth(),
        }
    }
}

/// Generic toggle for a data feed.
///
/// Intentionally a struct (not a bare `bool`) so that feed-specific
/// parameters can be added later without a breaking schema change.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct FeedToggle {
    /// Whether this feed is enabled.
    #[serde(default)]
    pub enabled: bool,
}

/// `[logs]` — per-event-type print frequency thresholds.
///
/// Each field specifies how many events of that type must accumulate
/// before a status line is printed.  Set to `0` to suppress output
/// for that event type entirely.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct LogsSection {
    /// Print a status line every N orderbook events.
    #[serde(default)]
    pub n_orderbooks: usize,
    /// Print a status line every N public trade events.
    #[serde(default)]
    pub n_trades: usize,
    /// Print a status line every N liquidation events.
    #[serde(default)]
    pub n_liquidations: usize,
    /// Print a status line every N funding rate events.
    #[serde(default)]
    pub n_fundings: usize,
    /// Print a status line every N open interest events.
    #[serde(default)]
    pub n_open_interests: usize,
}

/// `[output]` — where to write Parquet files.
#[derive(Debug, Clone, Deserialize)]
pub struct OutputSection {
    /// Directory for Parquet output, relative to the workspace root.
    pub dir: String,
}

fn default_ob_depth() -> usize {
    50
}

// ─────────────────────────────────────────────────────────────────────────────
// Enums
// ─────────────────────────────────────────────────────────────────────────────

/// Which event type drives the synchronization grid clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum SyncMode {
    /// Orderbook updates advance the grid.
    #[serde(rename = "on_orderbook")]
    OnOrderbook,
    /// Trade events advance the grid.
    #[serde(rename = "on_trade")]
    OnTrade,
    /// Liquidation events advance the grid.
    #[serde(rename = "on_liquidation")]
    OnLiquidation,
    /// An external wall-clock timer advances the grid.
    #[serde(rename = "on_time")]
    OnTime,
}

/// Time unit for [`UpdateFrequency`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum TimeUnit {
    /// Nanoseconds.
    #[serde(alias = "nanos", alias = "ns", alias = "nanoseconds")]
    Nanos,
    /// Microseconds.
    #[serde(alias = "micros", alias = "us", alias = "microseconds")]
    Micros,
    /// Milliseconds.
    #[serde(alias = "millis", alias = "ms", alias = "milliseconds")]
    Millis,
    /// Seconds.
    #[serde(alias = "secs", alias = "s", alias = "seconds", alias = "Seconds")]
    Secs,
}

// ─────────────────────────────────────────────────────────────────────────────
// Impl
// ─────────────────────────────────────────────────────────────────────────────

impl MarketSnapshotConfig {
    /// Load and parse a `MarketSnapshotConfig` from a TOML file (requires `std`).
    #[cfg(feature = "std")]
    pub fn from_toml(path: &Path) -> Result<Self, ConfigError> {
        let contents =
            std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
                path: format!("{path:?}"),
                source,
            })?;
        let config: Self =
            toml::from_str(&contents).map_err(|e| ConfigError::Parse(Box::new(e)))?;
        Ok(config)
    }

    /// Parse from an in-memory TOML string (works in native and WASM).
    pub fn from_toml_str(contents: &str) -> Result<Self, ConfigError> {
        let config: Self =
            toml::from_str(contents).map_err(|e| ConfigError::Parse(Box::new(e)))?;
        Ok(config)
    }

    /// Convert [`UpdateFrequency`] to a grid period in microseconds (the
    /// platform timestamp standard; sub-microsecond configs round down).
    pub fn period_us(&self) -> u64 {
        let v = self.update_frequency.value;
        match self.update_frequency.unit {
            TimeUnit::Nanos => v / 1_000,
            TimeUnit::Micros => v,
            TimeUnit::Millis => v * 1_000,
            TimeUnit::Secs => v * 1_000_000,
        }
    }

    /// Wall-clock duration of one full flush interval in microseconds.
    ///
    /// `flush_interval_us = period_us × flush_threshold`.
    ///
    /// For a 100 ms grid with `flush_threshold = 36000` this returns
    /// 3.6 × 10⁹ µs (= 1 hour).
    pub fn flush_interval_us(&self) -> u64 {
        self.period_us() * self.pipeline.flush_threshold as u64
    }

    /// Map [`SyncMode`] to the library's [`ClockMode`].
    pub fn clock_mode(&self) -> ClockMode {
        match self.symbol.sync_mode {
            SyncMode::OnOrderbook => ClockMode::OrderbookDriven,
            SyncMode::OnTrade => ClockMode::TradeDriven,
            SyncMode::OnLiquidation => ClockMode::LiquidationDriven,
            SyncMode::OnTime => ClockMode::ExternalClock,
        }
    }

    /// Human-readable label for the active clock mode.
    pub fn clock_mode_label(&self) -> &'static str {
        match self.symbol.sync_mode {
            SyncMode::OnOrderbook => "ClockMode::OrderbookDriven",
            SyncMode::OnTrade => "ClockMode::TradeDriven",
            SyncMode::OnLiquidation => "ClockMode::LiquidationDriven",
            SyncMode::OnTime => "ClockMode::ExternalClock",
        }
    }

    /// Build WSS subscription topics / channels for the configured exchange.
    ///
    /// Returns exchange-specific topic strings that the corresponding
    /// WSS client knows how to subscribe to.
    pub fn wss_streams(&self) -> Vec<String> {
        match self.exchange.name.to_lowercase().as_str() {
            "bybit" => self.bybit_wss_streams(),
            "coinbase" => self.coinbase_wss_channels(),
            "kraken" => self.kraken_wss_channels(),
            "binance" => self.binance_wss_streams(),
            other => {
                tracing::warn!("Unknown exchange '{}'; returning empty streams", other);
                vec![]
            }
        }
    }

    /// Bybit-specific topic construction.
    ///
    /// Funding rates and open interest share the `tickers.{symbol}` topic,
    /// so it is included if *either* is enabled.
    fn bybit_wss_streams(&self) -> Vec<String> {
        let sym = &self.symbol.name;
        let mut streams = Vec::new();

        if self.datatypes.orderbook.enabled {
            streams.push(format!(
                "orderbook.{}.{}",
                self.datatypes.orderbook.depth, sym,
            ));
        }
        if self.datatypes.trades.enabled {
            streams.push(format!("publicTrade.{}", sym));
        }
        if self.datatypes.liquidations.enabled {
            streams.push(format!("allLiquidation.{}", sym));
        }
        if self.datatypes.funding_rates.enabled || self.datatypes.open_interest.enabled {
            streams.push(format!("tickers.{}", sym));
        }

        streams
    }

    /// Coinbase-specific channel list (Advanced Trade spot).
    ///
    /// Coinbase uses separate channel names and product IDs rather than
    /// combined topic strings.  Returns just the channel names; the
    /// [`CoinbaseWssClient`] handles product_id subscription separately.
    fn coinbase_wss_channels(&self) -> Vec<String> {
        let mut channels = Vec::new();
        if self.datatypes.orderbook.enabled {
            channels.push("level2".to_string());
        }
        if self.datatypes.trades.enabled {
            channels.push("market_trades".to_string());
        }
        // Note: liquidations, funding rates, and open interest are NOT
        // available on Coinbase spot.  They require Coinbase INTX.
        channels
    }

    /// Kraken-specific channel list (WebSocket v2 spot).
    ///
    /// Kraken uses `{"method": "subscribe", "params": {"channel": ..., "symbol": [...]}}`
    /// for subscription.  Returns just the channel names; the
    /// [`KrakenWssClient`] handles symbol subscription separately.
    fn kraken_wss_channels(&self) -> Vec<String> {
        let mut channels = Vec::new();
        if self.datatypes.orderbook.enabled {
            channels.push("book".to_string());
        }
        if self.datatypes.trades.enabled {
            channels.push("trade".to_string());
        }
        // Note: liquidations, funding rates, and open interest are NOT
        // available on Kraken spot.  They require Kraken Futures.
        channels
    }

    /// Binance-specific stream construction (spot public).
    ///
    /// Binance uses lowercase symbol + stream suffix format:
    /// `<symbol>@depth@100ms`, `<symbol>@trade`.
    fn binance_wss_streams(&self) -> Vec<String> {
        let sym = self.symbol.name.to_lowercase();
        let mut streams = Vec::new();

        if self.datatypes.orderbook.enabled {
            streams.push(format!("{}@depth@100ms", sym));
        }
        if self.datatypes.trades.enabled {
            streams.push(format!("{}@trade", sym));
        }
        // Note: liquidations, funding rates, and open interest are NOT
        // available on Binance spot public streams.
        streams
    }

    /// Resolve the output directory relative to the workspace root.
    #[cfg(feature = "std")]
    pub fn output_dir(&self) -> PathBuf {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let workspace_root = Path::new(manifest_dir)
            .parent()
            .expect("failed to resolve workspace root");
        workspace_root.join(&self.output.dir)
    }
}

impl std::fmt::Display for SyncMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OnOrderbook => write!(f, "on_orderbook"),
            Self::OnTrade => write!(f, "on_trade"),
            Self::OnLiquidation => write!(f, "on_liquidation"),
            Self::OnTime => write!(f, "on_time"),
        }
    }
}

impl std::fmt::Display for TimeUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Nanos => write!(f, "ns"),
            Self::Micros => write!(f, "µs"),
            Self::Millis => write!(f, "ms"),
            Self::Secs => write!(f, "s"),
        }
    }
}

#[cfg(test)]
mod time_unit_tests {
    use super::TimeUnit;

    #[derive(serde::Deserialize)]
    struct Freq {
        unit: TimeUnit,
    }

    #[test]
    fn time_unit_accepts_common_aliases() {
        for (raw, want) in [
            ("Secs", TimeUnit::Secs),
            ("secs", TimeUnit::Secs),
            ("s", TimeUnit::Secs),
            ("seconds", TimeUnit::Secs),
            ("Seconds", TimeUnit::Secs),
            ("Millis", TimeUnit::Millis),
            ("millis", TimeUnit::Millis),
            ("ms", TimeUnit::Millis),
            ("milliseconds", TimeUnit::Millis),
            ("Micros", TimeUnit::Micros),
            ("us", TimeUnit::Micros),
            ("Nanos", TimeUnit::Nanos),
            ("ns", TimeUnit::Nanos),
        ] {
            let f: Freq = toml::from_str(&format!("unit = \"{raw}\"")).unwrap();
            assert_eq!(f.unit, want, "{raw}");
        }
        assert!(toml::from_str::<Freq>("unit = \"fortnights\"").is_err());
    }
}

#[cfg(test)]
mod declared_set_tests {
    use super::*;

    #[test]
    fn declared_set_mirrors_enabled_toggles_generically() {
        let mut section = DataTypesSection::default();
        section.orderbook.enabled = true;
        section.funding_rates.enabled = true;
        let set = section.declared_set();
        assert!(set.contains(DeclaredDatatype::Orderbook));
        assert!(set.contains(DeclaredDatatype::FundingRates));
        assert!(!set.contains(DeclaredDatatype::Trades));
        assert_eq!(set.len(), 2);
        assert_eq!(
            set.iter().map(|d| d.id()).collect::<Vec<_>>(),
            vec!["orderbook", "funding_rates"]
        );
        assert_eq!(DeclaredSet::all().len(), DeclaredDatatype::ALL.len());
        assert!(DataTypesSection::default().declared_set().is_empty());
    }
}
