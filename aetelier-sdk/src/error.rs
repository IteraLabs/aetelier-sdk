//! The unified façade error, [`AetelierError`].

use thiserror::Error;

/// One error type spanning the SDK's front-door operations, so application
/// code that composes several SDK calls can `?` any of them into a single
/// type instead of juggling one error per crate.
///
/// Each variant wraps the concrete error from the crate that produced it, and
/// the `#[from]` conversions make `?` transparent:
///
/// ```
/// use aetelier_sdk::{AetelierError, Trade, TradeSide, TradingPair};
///
/// fn build_one() -> Result<Trade, AetelierError> {
///     let trade = Trade::builder()
///         .source_trade_ts_us(1_700_000_000_000_000)
///         .pair(TradingPair::new("BTC", "USDT"))
///         .side(TradeSide::Buy)
///         .amount(0.5)
///         .price(42_000.0)
///         .exchange("binance".into())
///         .id("t-1".into())
///         .build()?; // BuildError -> AetelierError
///     Ok(trade)
/// }
/// # build_one().unwrap();
/// ```
///
/// The enum is `#[non_exhaustive]`: new variants may be added without a major
/// version bump, so match on it with a `_` arm.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AetelierError {
    /// A type builder was missing a required field or received an invalid one.
    #[error(transparent)]
    Build(#[from] aetelier_types::errors::BuildError),

    /// An order-book delta could not be applied (sequence gap, crossed book, …).
    #[error(transparent)]
    Orderbook(#[from] aetelier_types::OrderbookError),

    /// A columnar read or write (Parquet, CSV, JSON) failed.
    #[error(transparent)]
    Persist(#[from] aetelier_types::PersistError),

    /// Exchange connectivity or wire decoding failed.
    #[error(transparent)]
    Exchange(#[from] aetelier_connect::errors::ExchangeError),

    /// A worker manifest or market config failed to load or parse.
    #[error(transparent)]
    Config(#[from] aetelier_types::ConfigError),

    /// A worker config failed to parse or a worker failed to build.
    #[error(transparent)]
    Connect(#[from] aetelier_connect::ConnectError),

    /// Telemetry initialization or export failed.
    #[error(transparent)]
    Telemetry(#[from] aetelier_telemetry::TelemetryError),

    /// A worker runtime path surfaced an orchestration failure. This is the
    /// bridge for the `run` paths that still return `anyhow`; typed variants
    /// above are preferred as more of the runtime is migrated.
    #[error("{0}")]
    Runtime(String),
}

impl From<anyhow::Error> for AetelierError {
    fn from(err: anyhow::Error) -> Self {
        // `anyhow::Error` does not implement `std::error::Error`, so it cannot
        // be a `#[from]`/`#[source]` field; carry its rendered chain instead.
        Self::Runtime(format!("{err:#}"))
    }
}
