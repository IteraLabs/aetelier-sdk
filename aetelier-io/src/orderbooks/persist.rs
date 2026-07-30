//! Orderbook state persistence functions
//!
//! This module provides functions to save and load orderbook snapshots
//! in CSV, JSON, and Parquet formats.

use aetelier_types::TimestampUs;
use aetelier_types::{
    errors::PersistError, exchanges::Exchange, orderbooks::OrderbookDelta,
    orderbooks::persist::OutputFormat,
};
use std::path::Path;

// -------------------------------------------------------------------------------
// Public API
// -------------------------------------------------------------------------------

/// Save orderbook state to a file
///
/// Format is inferred from file extension, or can be specified explicitly.
///
/// # Example
/// ```no_run
/// use aetelier_io::orderbooks::persist;
/// use aetelier_types::orderbooks::OrderbookDelta;
/// use aetelier_types::trading_pair::TradingPair;
///
/// let eg_ob = OrderbookDelta::new(TradingPair::new("BTC", "USDT"));
///
/// persist::save_orderbook_state(&eg_ob, "snapshot.csv", None)?;
/// persist::save_orderbook_state(&eg_ob, "snapshot.json", None)?;
/// // This next one requires "parquet" feature
/// persist::save_orderbook_state(&eg_ob, "snapshot.parquet", None)?;
/// # Ok::<(), aetelier_types::errors::PersistError>(())
/// ```
pub fn save_orderbook_state(
    ob: &OrderbookDelta,
    path: impl AsRef<Path>,
    format: Option<OutputFormat>,
) -> Result<(), PersistError> {
    let path = path.as_ref();
    let format = format
        .or_else(|| OutputFormat::from_path(path))
        .ok_or_else(|| {
            PersistError::UnsupportedFormat(
                path.extension()
                    .and_then(|e| e.to_str())
                    .unwrap_or("unknown")
                    .to_string(),
            )
        })?;

    match format {
        OutputFormat::Csv => crate::orderbooks::write_csv(ob, path),
        OutputFormat::Json => crate::orderbooks::write_json(ob, path),
        #[cfg(feature = "parquet")]
        OutputFormat::Parquet => crate::orderbooks::write_ob_delta_parquet(ob, path),
        #[cfg(not(feature = "parquet"))]
        OutputFormat::Parquet => Err(PersistError::UnsupportedFormat(
            "parquet support not compiled in (enable 'parquet' feature)".to_string(),
        )),
    }
}

/// Save orderbook state with a timestamp-based filename
///
/// Returns the path to the created file.
pub fn save_orderbook_timestamped(
    ob: &OrderbookDelta,
    ex: &Exchange,
    dir: impl AsRef<Path>,
    format: OutputFormat,
) -> Result<std::path::PathBuf, PersistError> {
    let timestamp = TimestampUs::now().as_micros();
    let ext = match format {
        OutputFormat::Csv => "csv",
        OutputFormat::Json => "json",
        OutputFormat::Parquet => "parquet",
    };

    // `Display` already lowercases every venue id, including the framework-only
    // venues — keep this future-proof rather than re-matching each variant.
    let exchange_name: String = ex.to_string();

    let filename = format!(
        "{}_{}_{:?}.{}",
        exchange_name.to_lowercase(),
        ob.pair().to_canonical().to_lowercase(),
        timestamp,
        ext
    );

    let path = dir.as_ref().join(filename);
    save_orderbook_state(ob, &path, Some(format))?;
    Ok(path)
}
