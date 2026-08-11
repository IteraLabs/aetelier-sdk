//! Parquet I/O for [`Trade`] data.
//!
//! Provides write and read functions for persisting trade data in Apache Parquet
//! columnar format with Snappy compression.
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
use aetelier_types::trades::Trade;
use aetelier_types::trading_pair::TradingPair;
use std::path::Path;

/// Write a batch of [`Trade`] records to a Parquet file with Snappy compression.
///
/// # Output Schema
///
/// | Column | Arrow Type | Description |
/// |--------|------------|-------------|
/// | `source_trade_ts_us` | `UInt64` | Exchange trade (match) time, UTC epoch µs |
/// | `symbol` | `Utf8` | Trading pair symbol |
/// | `side` | `Utf8` | "buy" or "sell" (lowercase — `TradeSide::as_str`) |
/// | `price` | `Float64` | Execution price |
/// | `amount` | `Float64` | Execution quantity |
/// | `exchange` | `Utf8` | Exchange name |
/// | `id` | `Utf8` | Trade identifier |
/// | `local_trade_ts_us` | `UInt64` | Local receipt time, UTC epoch µs |
/// | `source_trade_rtt_us` | `UInt64` | Connection ping/pong round-trip, µs |
#[cfg(feature = "parquet")]
pub fn write_trades_parquet(trades: &[Trade], path: &Path) -> Result<(), PersistError> {
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

    let n = trades.len();

    let mut timestamps = Vec::with_capacity(n);
    let mut symbols: Vec<String> = Vec::with_capacity(n);
    let mut sides: Vec<&str> = Vec::with_capacity(n);
    let mut prices = Vec::with_capacity(n);
    let mut amounts = Vec::with_capacity(n);
    let mut exchanges: Vec<&str> = Vec::with_capacity(n);
    let mut ids: Vec<&str> = Vec::with_capacity(n);
    let mut local_ts = Vec::with_capacity(n);
    let mut rtts = Vec::with_capacity(n);
    let mut origins: Vec<&str> = Vec::with_capacity(n);

    for t in trades {
        timestamps.push(t.source_trade_ts_us);
        symbols.push(t.pair.to_canonical());
        sides.push(t.side.as_str());
        prices.push(decimal_to_f64(t.price));
        amounts.push(decimal_to_f64(t.amount));
        exchanges.push(&t.exchange);
        ids.push(&t.id);
        local_ts.push(t.local_trade_ts_us);
        rtts.push(t.source_trade_rtt_us);
        origins.push(t.origin.as_str());
    }

    let schema = Schema::new(vec![
        Field::new("source_trade_ts_us", DataType::UInt64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("side", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("id", DataType::Utf8, false),
        Field::new("local_trade_ts_us", DataType::UInt64, false),
        Field::new("source_trade_rtt_us", DataType::UInt64, false),
        // Provenance (`ws` | `rest`) — APPENDED LAST: the reader's positional
        // tolerance is the schema-migration mechanism, so new columns only
        // ever go at the end.
        Field::new("origin", DataType::Utf8, false),
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
            Arc::new(StringArray::from(ids)),
            Arc::new(UInt64Array::from(local_ts)),
            Arc::new(UInt64Array::from(rtts)),
            Arc::new(StringArray::from(origins)),
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
pub fn write_trades_parquet(_trades: &[Trade], _path: &Path) -> Result<(), PersistError> {
    Err(PersistError::UnsupportedFormat(
        "parquet support not compiled in (enable 'parquet' feature)".to_string(),
    ))
}

/// Read [`Trade`] records from a Parquet file.
///
/// Returns trades sorted by their original write order (typically ascending by
/// `trade_ts`).
#[cfg(feature = "parquet")]
pub fn read_trades_parquet(path: &Path) -> Result<Vec<Trade>, PersistError> {
    use aetelier_types::orderbooks::f64_to_decimal;
    use arrow::array::AsArray;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(crate::parquet_err::from_parquet)?;
    let reader = builder.build().map_err(crate::parquet_err::from_parquet)?;

    let mut trades = Vec::new();

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
        let ids = batch.column(6).as_string::<i32>();
        // local_trade_ts_us / source_trade_rtt_us are appended columns; tolerate
        // files written before the timestamp-model schema by reading 0.
        let local_ts = (batch.num_columns() > 7).then(|| {
            batch
                .column(7)
                .as_primitive::<arrow::datatypes::UInt64Type>()
        });
        let rtts = (batch.num_columns() > 8).then(|| {
            batch
                .column(8)
                .as_primitive::<arrow::datatypes::UInt64Type>()
        });
        // Provenance column (appended 2026-07-17); files written before it
        // read as the default `ws` — an unknown value is an error, never a
        // silent coercion (same policy as the side column).
        let origins =
            (batch.num_columns() > 9).then(|| batch.column(9).as_string::<i32>());

        for i in 0..batch.num_rows() {
            trades.push(Trade {
                source_trade_ts_us: timestamps.value(i),
                local_trade_ts_us: local_ts.map(|a| a.value(i)).unwrap_or(0),
                source_trade_rtt_us: rtts.map(|a| a.value(i)).unwrap_or(0),
                pair: symbols.value(i).parse::<TradingPair>().map_err(|_| {
                    PersistError::Parse(format!(
                        "row {i}: malformed symbol '{}' in {}",
                        symbols.value(i),
                        path.display()
                    ))
                })?,
                // Never coerce an unknown side: a fabricated default would
                // silently bias signed-flow computed from the replayed data.
                side: sides
                    .value(i)
                    .parse::<aetelier_types::TradeSide>()
                    .map_err(|_| {
                        PersistError::Parse(format!(
                            "row {i}: unknown trade side '{}' in {}",
                            sides.value(i),
                            path.display()
                        ))
                    })?,
                price: f64_to_decimal(prices.value(i)),
                amount: f64_to_decimal(amounts.value(i)),
                exchange: exchanges.value(i).to_string(),
                id: ids.value(i).to_string(),
                origin: match origins {
                    Some(col) => col.value(i).parse().map_err(|_| {
                        PersistError::Parse(format!(
                            "row {i}: unknown trade origin '{}' in {}",
                            col.value(i),
                            path.display()
                        ))
                    })?,
                    None => Default::default(),
                },
            });
        }
    }

    Ok(trades)
}

/// Stub for when the `parquet` feature is not enabled.
#[cfg(not(feature = "parquet"))]
pub fn read_trades_parquet(_path: &Path) -> Result<Vec<Trade>, PersistError> {
    Err(PersistError::UnsupportedFormat(
        "parquet support not compiled in (enable 'parquet' feature)".to_string(),
    ))
}

/// Write a batch of trades to a timestamped Parquet file in `output_dir`.
///
/// # Filename convention
///
/// Output file: `{SYMBOL}_trades_{MODE}_{TIMESTAMP}.parquet`
///
/// The symbol is sanitised for filesystem safety (`/` → `-`), so Kraken's
/// `BTC/USDT` becomes `BTC-USDT` in the filename while the Parquet data
/// retains the original symbol string.
///
/// Examples: `BTCUSDT_trades_sync_20260226_153000.123.parquet`,
///           `BTC-USDT_trades_sync_20260226_153000.123.parquet`
///
/// # Arguments
///
/// * `trades` — Trade records to write (must not be empty).
/// * `output_dir` — Target directory (must exist).
/// * `mode` — Tag embedded in the filename, typically `"sync"` for
///   grid-aligned data or `"raw"` for unprocessed captures.
///
/// Returns the full path to the written file.
#[cfg(feature = "parquet")]
pub fn write_trades_parquet_timestamped(
    trades: &[Trade],
    output_dir: &Path,
    mode: &str,
) -> Result<std::path::PathBuf, PersistError> {
    let file_ts =
        crate::naming::batch_stamp(trades.iter().map(|t| {
            crate::naming::effective_us(t.source_trade_ts_us, t.local_trade_ts_us)
        }));

    let raw_symbol = trades
        .first()
        .map(|t| t.pair.to_canonical())
        .unwrap_or_else(|| "unknown".to_string());

    let exchange = trades
        .first()
        .map(|t| t.exchange.as_str())
        .unwrap_or("unknown");

    let symbol = raw_symbol.replace('/', "-");

    let filename = format!(
        "{}_{}_trades_{}_{}.parquet",
        exchange, symbol, mode, file_ts
    );
    let path = crate::naming::unique_path(output_dir, &filename);

    write_trades_parquet(trades, &path)?;
    Ok(path)
}

/// Stub for when the `parquet` feature is not enabled.
#[cfg(not(feature = "parquet"))]
pub fn write_trades_parquet_timestamped(
    _trades: &[Trade],
    _output_dir: &Path,
    _mode: &str,
) -> Result<std::path::PathBuf, PersistError> {
    Err(PersistError::UnsupportedFormat(
        "parquet support not compiled in (enable 'parquet' feature)".to_string(),
    ))
}
