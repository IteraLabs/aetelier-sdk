//! Parquet I/O for [`Liquidation`] data.
//!
//! Provides write and read functions for persisting liquidation data in Apache
//! Parquet columnar format with Snappy compression.
//!
//! # Feature Flag
//!
//! Requires the `parquet` feature:
//!
//! ```toml
//! [dependencies]
//! aetelier_io = { version = "...", features = ["parquet"] }
//! ```

use aetelier_types::errors::PersistError;
use aetelier_types::liquidations::Liquidation;
use aetelier_types::trading_pair::TradingPair;
use std::path::Path;

/// Write a batch of [`Liquidation`] records to a Parquet file with Snappy compression.
///
/// # Output Schema
///
/// | Column | Arrow Type | Description |
/// |--------|------------|-------------|
/// | `liquidation_ts_us` | `UInt64` | Unix timestamp in UTC epoch microseconds |
/// | `symbol` | `Utf8` | Trading pair symbol |
/// | `side` | `Utf8` | "Buy" or "Sell" |
/// | `price` | `Float64` | Liquidation price |
/// | `amount` | `Float64` | Liquidation quantity |
/// | `exchange` | `Utf8` | Exchange name |
#[cfg(feature = "parquet")]
pub fn write_liquidations_parquet(
    liquidations: &[Liquidation],
    path: &Path,
) -> Result<(), PersistError> {
    use aetelier_types::orderbooks::decimal_to_f64;
    use arrow::{
        array::{Float64Array, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use parquet::{
        arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties,
    };
    use std::{fs::File, sync::Arc};

    let n = liquidations.len();

    let mut timestamps = Vec::with_capacity(n);
    let mut symbols: Vec<String> = Vec::with_capacity(n);
    let mut sides: Vec<&str> = Vec::with_capacity(n);
    let mut prices = Vec::with_capacity(n);
    let mut amounts = Vec::with_capacity(n);
    let mut exchanges: Vec<&str> = Vec::with_capacity(n);

    for liq in liquidations {
        timestamps.push(liq.liquidation_ts_us);
        symbols.push(liq.pair.to_canonical());
        sides.push(liq.side.as_str());
        prices.push(decimal_to_f64(liq.price));
        amounts.push(decimal_to_f64(liq.amount));
        exchanges.push(&liq.exchange);
    }

    let schema = Schema::new(vec![
        Field::new("liquidation_ts_us", DataType::UInt64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("side", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("exchange", DataType::Utf8, false),
    ]);

    let symbols_refs: Vec<&str> = symbols.iter().map(|s| s.as_str()).collect();
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt64Array::from(timestamps)),
            Arc::new(StringArray::from(symbols_refs)),
            Arc::new(StringArray::from(sides)),
            Arc::new(Float64Array::from(prices)),
            Arc::new(Float64Array::from(amounts)),
            Arc::new(StringArray::from(exchanges)),
        ],
    )
    .map_err(crate::parquet_err::from_arrow)?;

    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
        .map_err(crate::parquet_err::from_parquet)?;
    writer
        .write(&batch)
        .map_err(crate::parquet_err::from_parquet)?;
    writer.close().map_err(crate::parquet_err::from_parquet)?;

    Ok(())
}

/// Stub for when the `parquet` feature is not enabled.
#[cfg(not(feature = "parquet"))]
pub fn write_liquidations_parquet(
    _liquidations: &[Liquidation],
    _path: &Path,
) -> Result<(), PersistError> {
    Err(PersistError::UnsupportedFormat(
        "parquet support not compiled in (enable 'parquet' feature)".to_string(),
    ))
}

/// Read [`Liquidation`] records from a Parquet file.
#[cfg(feature = "parquet")]
pub fn read_liquidations_parquet(path: &Path) -> Result<Vec<Liquidation>, PersistError> {
    use aetelier_types::orderbooks::f64_to_decimal;
    use arrow::array::AsArray;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(crate::parquet_err::from_parquet)?;
    let reader = builder.build().map_err(crate::parquet_err::from_parquet)?;

    let mut liquidations = Vec::new();

    for batch_result in reader {
        let batch = batch_result.map_err(crate::parquet_err::from_arrow)?;

        let timestamps = batch
            .column(0)
            .as_primitive::<arrow::datatypes::UInt64Type>();
        let symbols = batch.column(1).as_string::<i32>();
        let sides = batch.column(2).as_string::<i32>();
        let prices = batch
            .column(3)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let amounts = batch
            .column(4)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let exchanges = batch.column(5).as_string::<i32>();

        for i in 0..batch.num_rows() {
            liquidations.push(Liquidation {
                liquidation_ts_us: timestamps.value(i),
                pair: symbols.value(i).parse::<TradingPair>().map_err(|_| {
                    PersistError::Parse(format!(
                        "row {i}: malformed symbol '{}' in {}",
                        symbols.value(i),
                        path.display()
                    ))
                })?,
                // Never coerce an unknown side: a fabricated default would
                // silently bias liquidation-flow computed from replayed data.
                side: sides
                    .value(i)
                    .parse::<aetelier_types::TradeSide>()
                    .map_err(|_| {
                        PersistError::Parse(format!(
                            "row {i}: unknown liquidation side '{}' in {}",
                            sides.value(i),
                            path.display()
                        ))
                    })?,
                price: f64_to_decimal(prices.value(i)),
                amount: f64_to_decimal(amounts.value(i)),
                exchange: exchanges.value(i).to_string(),
            });
        }
    }

    Ok(liquidations)
}

/// Stub for when the `parquet` feature is not enabled.
#[cfg(not(feature = "parquet"))]
pub fn read_liquidations_parquet(_path: &Path) -> Result<Vec<Liquidation>, PersistError> {
    Err(PersistError::UnsupportedFormat(
        "parquet support not compiled in (enable 'parquet' feature)".to_string(),
    ))
}

/// Write a batch of liquidations to a timestamped Parquet file in `output_dir`.
///
/// # Filename convention
///
/// Output file: `{SYMBOL}_liquidations_{MODE}_{TIMESTAMP}.parquet`
///
/// The symbol is sanitised for filesystem safety (`/` → `-`), so Kraken's
/// `BTC/USDT` becomes `BTC-USDT` in the filename while the Parquet data
/// retains the original symbol string.
///
/// Examples: `BTCUSDT_liquidations_sync_20260226_153000.123.parquet`,
///           `BTC-USDT_liquidations_sync_20260226_153000.123.parquet`
///
/// # Arguments
///
/// * `liquidations` — Liquidation records to write (must not be empty).
/// * `output_dir` — Target directory (must exist).
/// * `mode` — Tag embedded in the filename, typically `"sync"` for
///   grid-aligned data or `"raw"` for unprocessed captures.
///
/// Returns the full path to the written file.
#[cfg(feature = "parquet")]
pub fn write_liquidations_parquet_timestamped(
    liquidations: &[Liquidation],
    output_dir: &Path,
    mode: &str,
) -> Result<std::path::PathBuf, PersistError> {
    let file_ts =
        crate::naming::batch_stamp(liquidations.iter().map(|l| l.liquidation_ts_us));

    let raw_symbol = liquidations
        .first()
        .map(|l| l.pair.to_canonical())
        .unwrap_or_else(|| "unknown".to_string());
    let exchange = liquidations
        .first()
        .map(|l| l.exchange.as_str())
        .unwrap_or("unknown");
    let symbol = raw_symbol.replace('/', "-");

    let filename = format!(
        "{}_{}_liquidations_{}_{}.parquet",
        exchange, symbol, mode, file_ts
    );
    let path = crate::naming::unique_path(output_dir, &filename);

    write_liquidations_parquet(liquidations, &path)?;
    Ok(path)
}

/// Stub for when the `parquet` feature is not enabled.
#[cfg(not(feature = "parquet"))]
pub fn write_liquidations_parquet_timestamped(
    _liquidations: &[Liquidation],
    _output_dir: &Path,
    _mode: &str,
) -> Result<std::path::PathBuf, PersistError> {
    Err(PersistError::UnsupportedFormat(
        "parquet support not compiled in (enable 'parquet' feature)".to_string(),
    ))
}
