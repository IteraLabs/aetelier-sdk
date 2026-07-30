//! Parquet I/O for [`FundingRate`] data.

use aetelier_types::errors::PersistError;
use aetelier_types::funding::FundingRate;
use aetelier_types::trading_pair::TradingPair;
use std::path::Path;

/// Write a batch of [`FundingRate`] records to a Parquet file with Snappy compression.
///
/// # Output Schema
///
/// | Column | Arrow Type | Description |
/// |--------|------------|-------------|
/// | `ts` | `UInt64` | Observation timestamp (Unix ms) |
/// | `symbol` | `Utf8` | Trading pair symbol |
/// | `funding_rate` | `Float64` | Signed funding rate (decimal) |
/// | `next_funding_ts_us` | `UInt64` | Next settlement timestamp (Unix ms) |
/// | `exchange` | `Utf8` | Exchange name |
#[cfg(feature = "parquet")]
pub fn write_funding_parquet(
    rates: &[FundingRate],
    path: &Path,
) -> Result<(), PersistError> {
    use arrow::{
        array::{Float64Array, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use parquet::{
        arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties,
    };
    use std::{fs::File, sync::Arc};

    let n = rates.len();

    let mut timestamps = Vec::with_capacity(n);
    let mut symbols: Vec<String> = Vec::with_capacity(n);
    let mut funding_rates = Vec::with_capacity(n);
    let mut next_funding_timestamps = Vec::with_capacity(n);
    let mut exchanges: Vec<&str> = Vec::with_capacity(n);

    for fr in rates {
        timestamps.push(fr.funding_rate_ts_us);
        symbols.push(fr.pair.to_canonical());
        funding_rates.push(fr.funding_rate);
        next_funding_timestamps.push(fr.next_funding_ts_us);
        exchanges.push(&fr.exchange);
    }

    let schema = Schema::new(vec![
        Field::new("funding_rate_ts_us", DataType::UInt64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("funding_rate", DataType::Float64, false),
        Field::new("next_funding_ts_us", DataType::UInt64, false),
        Field::new("exchange", DataType::Utf8, false),
    ]);

    let symbols_refs: Vec<&str> = symbols.iter().map(|s| s.as_str()).collect();
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt64Array::from(timestamps)),
            Arc::new(StringArray::from(symbols_refs)),
            Arc::new(Float64Array::from(funding_rates)),
            Arc::new(UInt64Array::from(next_funding_timestamps)),
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

#[cfg(not(feature = "parquet"))]
pub fn write_funding_parquet(
    _rates: &[FundingRate],
    _path: &Path,
) -> Result<(), PersistError> {
    Err(PersistError::UnsupportedFormat(
        "parquet support not compiled in (enable 'parquet' feature)".to_string(),
    ))
}

/// Read [`FundingRate`] records from a Parquet file.
#[cfg(feature = "parquet")]
pub fn read_funding_parquet(path: &Path) -> Result<Vec<FundingRate>, PersistError> {
    use arrow::array::AsArray;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(crate::parquet_err::from_parquet)?;
    let reader = builder.build().map_err(crate::parquet_err::from_parquet)?;

    let mut rates = Vec::new();

    for batch_result in reader {
        let batch = batch_result.map_err(crate::parquet_err::from_arrow)?;

        let timestamps = batch
            .column(0)
            .as_primitive::<arrow::datatypes::UInt64Type>();
        let symbols = batch.column(1).as_string::<i32>();
        let funding_rates = batch
            .column(2)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let next_funding_timestamps = batch
            .column(3)
            .as_primitive::<arrow::datatypes::UInt64Type>();
        let exchanges = batch.column(4).as_string::<i32>();

        for i in 0..batch.num_rows() {
            rates.push(FundingRate {
                funding_rate_ts_us: timestamps.value(i),
                pair: symbols.value(i).parse::<TradingPair>().map_err(|_| {
                    PersistError::Parse(format!(
                        "row {i}: malformed symbol '{}' in {}",
                        symbols.value(i),
                        path.display()
                    ))
                })?,
                funding_rate: funding_rates.value(i),
                next_funding_ts_us: next_funding_timestamps.value(i),
                exchange: exchanges.value(i).to_string(),
            });
        }
    }

    Ok(rates)
}

#[cfg(not(feature = "parquet"))]
pub fn read_funding_parquet(_path: &Path) -> Result<Vec<FundingRate>, PersistError> {
    Err(PersistError::UnsupportedFormat(
        "parquet support not compiled in (enable 'parquet' feature)".to_string(),
    ))
}

/// Write funding rates to a timestamped Parquet file in `output_dir`.
///
/// # Filename convention
///
/// Output file: `{SYMBOL}_funding_{MODE}_{TIMESTAMP}.parquet`
///
/// The symbol is sanitised for filesystem safety (`/` → `-`), so Kraken's
/// `BTC/USDT` becomes `BTC-USDT` in the filename while the Parquet data
/// retains the original symbol string.
///
/// Examples: `BTCUSDT_funding_sync_20260226_153000.123.parquet`,
///           `BTC-USDT_funding_sync_20260226_153000.123.parquet`
///
/// # Arguments
///
/// * `rates` — Funding rate records to write (must not be empty).
/// * `output_dir` — Target directory (must exist).
/// * `mode` — Tag embedded in the filename, typically `"sync"` for
///   grid-aligned data or `"raw"` for unprocessed captures.
///
/// Returns the full path to the written file.
#[cfg(feature = "parquet")]
pub fn write_funding_parquet_timestamped(
    rates: &[FundingRate],
    output_dir: &Path,
    mode: &str,
) -> Result<std::path::PathBuf, PersistError> {
    let file_ts = chrono::Utc::now().format("%Y%m%d_%H%M%S%.3f");
    let raw_symbol = rates
        .first()
        .map(|r| r.pair.to_canonical())
        .unwrap_or_else(|| "unknown".to_string());
    let exchange = rates
        .first()
        .map(|r| r.exchange.as_str())
        .unwrap_or("unknown");
    let symbol = raw_symbol.replace('/', "-");
    let filename = format!(
        "{}_{}_funding_{}_{}.parquet",
        exchange, symbol, mode, file_ts
    );
    let path = output_dir.join(filename);
    write_funding_parquet(rates, &path)?;
    Ok(path)
}

#[cfg(not(feature = "parquet"))]
pub fn write_funding_parquet_timestamped(
    _rates: &[FundingRate],
    _output_dir: &Path,
    _mode: &str,
) -> Result<std::path::PathBuf, PersistError> {
    Err(PersistError::UnsupportedFormat(
        "parquet support not compiled in (enable 'parquet' feature)".to_string(),
    ))
}
