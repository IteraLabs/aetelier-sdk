use aetelier_types::errors::PersistError;
use aetelier_types::orderbooks::{
    Orderbook, OrderbookDelta, OrderbookTarget, OrderbookTargetData,
};
use std::path::Path;

/// Saves an orderbook snapshot to a Parquet file with Snappy compression.
///
/// Writes the orderbook in Apache Parquet columnar format, optimized for
/// analytical queries and efficient storage. Uses Snappy compression for
/// a good balance between compression ratio and read/write speed.
///
/// This format is ideal for:
/// - Large-scale historical analysis
/// - Integration with data processing frameworks (Spark, DuckDB, Polars)
/// - Long-term storage with ~10x compression vs CSV
///
/// # Feature Flag
///
/// This function requires the `parquet` feature:
///
/// ```toml
/// [dependencies]
/// aetelier_io = { version = "...", features = ["parquet"] }
/// ```
///
/// # Output Schema
///
/// | Column | Arrow Type | Description |
/// |--------|------------|-------------|
/// | `timestamp_us` | `UInt64` | Unix timestamp in milliseconds |
/// | `symbol` | `Utf8` | Trading pair symbol |
/// | `exchange` | `Utf8` | Exchange where the symbol is traded |
/// | `side` | `Utf8` | "bid" or "ask" |
/// | `level` | `UInt64` | Depth level (0 = best price) |
/// | `price` | `Float64` | Price at this level |
/// | `size` | `Float64` | Quantity at this level |
///
/// # Arguments
///
/// * `ob` - Reference to the [`OrderbookDelta`] containing the current book state
/// * `path` - Destination file path. Parent directories must exist.
///
/// # Returns
///
/// * `Ok(())` - File was written successfully
/// * `Err(PersistError::Io)` - Failed to create file
/// * `Err(PersistError::Arrow)` - Failed to build Arrow record batch
/// * `Err(PersistError::Parquet)` - Failed to write Parquet file
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
/// use aetelier_io::orderbooks::ob_parquet::write_ob_delta_parquet;
/// use aetelier_types::orderbooks::OrderbookDelta;
/// use aetelier_types::trading_pair::TradingPair;
///
/// let ob = OrderbookDelta::new(TradingPair::new("SOL", "USDT"));
/// // ... process some snapshots/deltas ...
///
/// write_ob_delta_parquet(&ob, Path::new("./snapshots/sol_orderbook.parquet"))?;
/// # Ok::<(), aetelier_types::errors::PersistError>(())
/// ```
///
/// # Reading the Output
///
/// The Parquet file can be read with various tools:
///
/// ```python
/// # Python with pandas
/// import pandas as pd
/// df = pd.read_parquet("sol_orderbook.parquet")
///
/// # Python with polars
/// import polars as pl
/// df = pl.read_parquet("sol_orderbook.parquet")
/// ```
///
/// ```sql
/// -- DuckDB
/// SELECT * FROM read_parquet('sol_orderbook.parquet');
/// ```
///
/// # Performance Notes
///
/// - **Compression**: Snappy provides ~3-5x compression with minimal CPU overhead
/// - **Precision**: Prices/sizes are stored as `f64`, which may lose precision
///   for values requiring more than 15-16 significant digits
/// - **Batch size**: Writes entire orderbook as a single row group
///
/// # See Also
///
/// - [`write_csv`](super::ob_csv::write_csv) — Human-readable format for debugging
/// - [`write_json`](super::ob_json::write_json) — Full metadata preservation
/// - [`load_parquet_to_delta`] — Load back into `OrderbookDelta`
#[cfg(feature = "parquet")]
pub fn write_ob_delta_parquet(
    ob: &OrderbookDelta,
    path: &Path,
) -> Result<(), PersistError> {
    use aetelier_types::TimestampUs;
    use aetelier_types::utils::decimal_to_f64;
    use arrow::{
        array::{Float64Array, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use parquet::{
        arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties,
    };
    use std::{fs::File, sync::Arc};

    use aetelier_types::OrderSide;

    let timestamp = TimestampUs::now().as_micros();
    let symbol = ob.pair().to_canonical();
    let exchange = ob.exchange();

    // To collect all levels into vectors
    let mut timestamps: Vec<u64> = Vec::new();
    let mut symbols: Vec<String> = Vec::new();
    let mut exchanges: Vec<String> = Vec::new();
    let mut sides: Vec<&str> = Vec::new();
    let mut levels: Vec<u64> = Vec::new();
    let mut prices: Vec<f64> = Vec::new();
    let mut sizes: Vec<f64> = Vec::new();

    // Bids
    for (level, (price, size)) in ob.top_bids(ob.bid_depth()).iter().enumerate() {
        timestamps.push(timestamp);
        symbols.push(symbol.clone());
        exchanges.push(exchange.to_string());
        sides.push(OrderSide::Bids.as_str());
        levels.push(level as u64);
        prices.push(decimal_to_f64(*price));
        sizes.push(decimal_to_f64(*size));
    }

    // Asks
    for (level, (price, size)) in ob.top_asks(ob.ask_depth()).iter().enumerate() {
        timestamps.push(timestamp);
        symbols.push(symbol.clone());
        exchanges.push(exchange.to_string());
        sides.push(OrderSide::Asks.as_str());
        levels.push(level as u64);
        prices.push(decimal_to_f64(*price));
        sizes.push(decimal_to_f64(*size));
    }

    // Build Arrow arrays
    let schema = Schema::new(vec![
        Field::new("timestamp_us", DataType::UInt64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("side", DataType::Utf8, false),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("level", DataType::UInt64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("size", DataType::Float64, false),
    ]);

    let symbols_refs: Vec<&str> = symbols.iter().map(|s| s.as_str()).collect();
    let exchanges_refs: Vec<&str> = exchanges.iter().map(|e| e.as_str()).collect();
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt64Array::from(timestamps)),
            Arc::new(StringArray::from(symbols_refs)),
            Arc::new(StringArray::from(exchanges_refs)),
            Arc::new(StringArray::from(sides)),
            Arc::new(UInt64Array::from(levels)),
            Arc::new(Float64Array::from(prices)),
            Arc::new(Float64Array::from(sizes)),
        ],
    )
    .map_err(crate::parquet_err::from_arrow)?;

    // Write to file with snappy compression
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
///
/// Returns an error indicating that Parquet support must be explicitly enabled.
///
/// # Feature Flag
///
/// Enable Parquet support in your `Cargo.toml`:
///
/// ```toml
/// [dependencies]
/// aetelier_io = { version = "...", features = ["parquet"] }
/// ```
#[cfg(not(feature = "parquet"))]
pub fn write_ob_delta_parquet(
    _ob: &OrderbookDelta,
    _path: &Path,
) -> Result<(), PersistError> {
    Err(PersistError::UnsupportedFormat(
        "parquet support not compiled in (enable 'parquet' feature)".to_string(),
    ))
}

/// Stub for when the `parquet` feature is not enabled.
#[cfg(not(feature = "parquet"))]
pub fn load_parquet_to_delta(_path: &Path) -> Result<OrderbookDelta, PersistError> {
    Err(PersistError::UnsupportedFormat(
        "parquet support not compiled in (enable 'parquet' feature)".to_string(),
    ))
}

/// Stub for when the `parquet` feature is not enabled.
#[cfg(not(feature = "parquet"))]
pub fn load_parquet_to_ob(_path: &Path) -> Result<Vec<Orderbook>, PersistError> {
    Err(PersistError::UnsupportedFormat(
        "parquet support not compiled in (enable 'parquet' feature)".to_string(),
    ))
}

/// Stub for when the `parquet` feature is not enabled.
#[cfg(not(feature = "parquet"))]
pub fn read_ob_parquet(
    _path: &Path,
    _target: OrderbookTarget,
) -> Result<OrderbookTargetData, PersistError> {
    Err(PersistError::UnsupportedFormat(
        "parquet support not compiled in (enable 'parquet' feature)".to_string(),
    ))
}

/// Reads a Parquet file into the specified orderbook format.
///
/// Dispatches to the appropriate loader based on the target format:
///
/// - [`OrderbookTarget::Delta`] → single [`OrderbookDelta`] (collapses all
///   timestamps into one BTreeMap — use only for single-snapshot files)
/// - [`OrderbookTarget::Snapshot`] → `Vec<Orderbook>` grouped by timestamp,
///   preserving the full time series from synced Parquet files
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
/// use aetelier_io::orderbooks::ob_parquet::read_ob_parquet;
/// use aetelier_types::orderbooks::{OrderbookTarget, OrderbookTargetData};
///
/// let data = read_ob_parquet(Path::new("synced_ob.parquet"), OrderbookTarget::Snapshot)?;
/// if let OrderbookTargetData::Snapshot(orderbooks) = data {
///     println!("Loaded {} snapshots", orderbooks.len());
///     for ob in &orderbooks {
///         println!("  ts={} pair={} exchange={} bids={} asks={}",
///             ob.orderbook_ts_us, ob.pair.to_canonical(), ob.exchange,
///             ob.bids.len(), ob.asks.len());
///     }
/// }
/// # Ok::<(), aetelier_types::errors::PersistError>(())
/// ```
#[cfg(feature = "parquet")]
pub fn read_ob_parquet(
    path: &Path,
    target: OrderbookTarget,
) -> Result<OrderbookTargetData, PersistError> {
    match target {
        OrderbookTarget::Delta => {
            let delta = load_parquet_to_delta(path)?;
            Ok(OrderbookTargetData::Delta(delta))
        }
        OrderbookTarget::Snapshot => {
            let orderbooks = load_parquet_to_ob(path)?;
            Ok(OrderbookTargetData::Snapshot(orderbooks))
        }
    }
}

/// Load a Parquet file into a single [`OrderbookDelta`].
///
/// **Warning**: This collapses all rows (across all timestamps) into one
/// `BTreeMap`-based book. For multi-snapshot Parquet files produced by the
/// time-synchronized writer, use [`load_parquet_to_ob`] instead to
/// preserve the temporal structure.
#[cfg(feature = "parquet")]
pub fn load_parquet_to_delta(path: &Path) -> Result<OrderbookDelta, PersistError> {
    use aetelier_types::trading_pair::TradingPair;
    use arrow::array::AsArray;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use rust_decimal::Decimal;
    use std::{collections::BTreeMap, fs::File, str::FromStr};

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(crate::parquet_err::from_parquet)?;
    let reader = builder.build().map_err(crate::parquet_err::from_parquet)?;

    let mut bids: BTreeMap<Decimal, Decimal> = BTreeMap::new();
    let mut asks: BTreeMap<Decimal, Decimal> = BTreeMap::new();
    let mut symbol = String::new();
    let mut exchange = String::new();

    for batch_result in reader {
        let batch = batch_result.map_err(crate::parquet_err::from_arrow)?;

        let symbols = batch.column(1).as_string::<i32>();
        let exchanges = batch.column(2).as_string::<i32>();

        let sides = batch.column(3).as_string::<i32>();
        let prices = batch
            .column(5)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let sizes = batch
            .column(6)
            .as_primitive::<arrow::datatypes::Float64Type>();

        for i in 0..batch.num_rows() {
            if symbol.is_empty() {
                symbol = symbols.value(i).to_string();
            }

            if exchange.is_empty() {
                exchange = exchanges.value(i).to_string();
            }

            let side = sides.value(i);
            let price = Decimal::from_f64_retain(prices.value(i))
                .ok_or_else(|| PersistError::Parse("price conversion failed".into()))?;
            let size = Decimal::from_f64_retain(sizes.value(i))
                .ok_or_else(|| PersistError::Parse("size conversion failed".into()))?;

            match side {
                "bid" => {
                    bids.insert(price, size);
                }
                "ask" => {
                    asks.insert(price, size);
                }
                _ => {}
            }
        }
    }

    let pair = TradingPair::from_str(&symbol)
        .map_err(|e| PersistError::Parse(format!("invalid trading pair: {}", e)))?;
    Ok(OrderbookDelta::from_maps(pair, exchange, bids, asks))
}

/// Load a synced Parquet file into a `Vec<Orderbook>`, one per unique timestamp.
///
/// Rows are grouped by `orderbook_ts_us` (column 0), preserving the temporal
/// structure written by the time-synchronized snapshot writer. Within each
/// group, bid and ask levels are sorted by level index.
///
/// # Output Ordering
///
/// The returned vector is sorted by ascending `orderbook_ts_us`.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
/// use aetelier_io::orderbooks::ob_parquet::load_parquet_to_ob;
///
/// let orderbooks = load_parquet_to_ob(Path::new("synced_ob.parquet"))?;
/// println!("Loaded {} snapshots", orderbooks.len());
///
/// let first = &orderbooks[0];
/// println!("First snapshot: ts={} bids={} asks={}",
///     first.orderbook_ts_us, first.bids.len(), first.asks.len());
/// # Ok::<(), aetelier_types::errors::PersistError>(())
/// ```
#[cfg(feature = "parquet")]
pub fn load_parquet_to_ob(path: &Path) -> Result<Vec<Orderbook>, PersistError> {
    use aetelier_types::{Level, OrderSide};
    use arrow::array::AsArray;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::{collections::BTreeMap, fs::File};

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(crate::parquet_err::from_parquet)?;
    let reader = builder.build().map_err(crate::parquet_err::from_parquet)?;

    /// (timestamp, price, volume) per level
    type LevelRow = (u64, f64, f64);

    /// (exchange, symbol, bids, asks) grouped by grid timestamp
    type SnapshotOrderbook =
        (String, String, u64, u64, u64, Vec<LevelRow>, Vec<LevelRow>);

    // Accumulate per-timestamp: (symbol, bid_levels, ask_levels)
    // BTreeMap gives us sorted-by-timestamp iteration for free.
    let mut groups: BTreeMap<u64, SnapshotOrderbook> = BTreeMap::new();

    for batch_result in reader {
        let batch = batch_result.map_err(crate::parquet_err::from_arrow)?;

        let timestamps = batch
            .column(0)
            .as_primitive::<arrow::datatypes::UInt64Type>();
        let symbols = batch.column(1).as_string::<i32>();
        let exchanges = batch.column(2).as_string::<i32>();
        let sides = batch.column(3).as_string::<i32>();
        let levels = batch
            .column(4)
            .as_primitive::<arrow::datatypes::UInt64Type>();
        let prices = batch
            .column(5)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let sizes = batch
            .column(6)
            .as_primitive::<arrow::datatypes::Float64Type>();
        // Appended timestamp-model columns; tolerate older 7-column files.
        let source_ts_col = (batch.num_columns() > 7).then(|| {
            batch
                .column(7)
                .as_primitive::<arrow::datatypes::UInt64Type>()
        });
        let local_ts_col = (batch.num_columns() > 8).then(|| {
            batch
                .column(8)
                .as_primitive::<arrow::datatypes::UInt64Type>()
        });
        let rtt_col = (batch.num_columns() > 9).then(|| {
            batch
                .column(9)
                .as_primitive::<arrow::datatypes::UInt64Type>()
        });

        for i in 0..batch.num_rows() {
            let ts = timestamps.value(i);
            let symbol = symbols.value(i).to_string();
            let exchange = exchanges.value(i).to_string();
            let side = sides.value(i);
            let level = levels.value(i);
            let price = prices.value(i);
            let size = sizes.value(i);
            let src = source_ts_col.map(|a| a.value(i)).unwrap_or(0);
            let loc = local_ts_col.map(|a| a.value(i)).unwrap_or(0);
            let rtt = rtt_col.map(|a| a.value(i)).unwrap_or(0);

            let entry = groups.entry(ts).or_insert_with(|| {
                (symbol, exchange, src, loc, rtt, Vec::new(), Vec::new())
            });

            match side {
                "bid" => entry.5.push((level, price, size)),
                "ask" => entry.6.push((level, price, size)),
                other => {
                    return Err(PersistError::Parse(format!(
                        "row {i}: unknown book side '{other}' in {}",
                        path.display()
                    )));
                }
            }
        }
    }

    // Convert each group into an Orderbook
    let orderbooks: Vec<Orderbook> =
        groups
            .into_iter()
            .map(
                |(
                    ts,
                    (symbol, exchange, src, loc, rtt, mut bid_levels, mut ask_levels),
                )|
                 -> Result<Orderbook, PersistError> {
                    use aetelier_types::orderbooks::f64_to_decimal;
                    use aetelier_types::trading_pair::TradingPair;
                    use std::str::FromStr;

                    // Levels
                    bid_levels.sort_by_key(|(l, _, _)| *l);
                    let bids: Vec<Level> = bid_levels
                        .into_iter()
                        .map(|(idx, price, volume)| {
                            Level::new(
                                idx as u32,
                                OrderSide::Bids,
                                f64_to_decimal(price),
                                f64_to_decimal(volume),
                                vec![],
                            )
                        })
                        .collect();

                    ask_levels.sort_by_key(|(l, _, _)| *l);
                    let asks: Vec<Level> = ask_levels
                        .into_iter()
                        .map(|(idx, price, volume)| {
                            Level::new(
                                idx as u32,
                                OrderSide::Asks,
                                f64_to_decimal(price),
                                f64_to_decimal(volume),
                                vec![],
                            )
                        })
                        .collect();

                    let pair = TradingPair::from_str(&symbol).map_err(|_| {
                        PersistError::Parse(format!(
                            "malformed symbol '{symbol}' in {}",
                            path.display()
                        ))
                    })?;
                    let mut ob =
                        Orderbook::from_levels(0, ts, pair, exchange, bids, asks);
                    ob.source_orderbook_ts_us = src;
                    ob.local_orderbook_ts_us = loc;
                    ob.source_orderbook_rtt_us = rtt;
                    Ok(ob)
                },
            )
            .collect::<Result<Vec<_>, _>>()?;

    Ok(orderbooks)
}

// -----------------------------------------------------------------------
// Parquet Batch Writer
// -----------------------------------------------------------------------

/// Stub for when the `parquet` feature is not enabled.
#[cfg(not(feature = "parquet"))]
pub fn write_ob_parquet(
    _snapshots: &[Orderbook],
    _output_dir: &Path,
    _mode: &str,
) -> Result<OrderbookTargetData, PersistError> {
    Err(PersistError::UnsupportedFormat(
        "parquet support not compiled in (enable 'parquet' feature)".to_string(),
    ))
}

/// Write a batch of synchronized `Orderbook` snapshots to a single Parquet file.
///
/// Schema: `orderbook_ts_us | symbol | exchange | side | level | price | size`
///
/// Reads directly from `Orderbook.bids` and `Orderbook.asks` (`Vec<Level>`),
/// using `Level.price` and `Level.volume`. All timestamps come from
/// `Orderbook.orderbook_ts_us` which holds the grid-aligned nanosecond value.
///
/// # Filename convention
///
/// Output file: `{EXCHANGE}_{SYMBOL}_ob_{MODE}_{TIMESTAMP}.parquet`
///
/// The symbol is sanitised for filesystem safety (`/` → `-`), so Kraken's
/// `BTC/USDT` becomes `BTC-USDT` in the filename while the Parquet data
/// retains the original symbol string.
///
/// Examples: `bybit_BTCUSDT_ob_sync_20260226_153000.123.parquet`,
///           `kraken_BTC-USDT_ob_sync_20260226_153000.123.parquet`
///
/// # Arguments
///
/// * `snapshots` — Orderbook snapshots to write (must not be empty).
/// * `output_dir` — Target directory (must exist).
/// * `mode` — Tag embedded in the filename, typically `"sync"` for
///   grid-aligned data or `"raw"` for unprocessed captures.
#[cfg(feature = "parquet")]
pub fn write_ob_parquet(
    snapshots: &[Orderbook],
    output_dir: &Path,
    mode: &str,
) -> Result<std::path::PathBuf, PersistError> {
    use arrow::{
        array::{Float64Array, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use parquet::{
        arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties,
    };

    use std::{fs::File, sync::Arc};

    use aetelier_types::OrderSide;
    use aetelier_types::orderbooks::decimal_to_f64;

    let total_rows: usize = snapshots
        .iter()
        .map(|ob| ob.bids.len() + ob.asks.len())
        .sum();

    let file_symbol = snapshots[0].pair.to_canonical().replace('/', "-");
    let file_exchange = snapshots[0].exchange.clone();

    let mut timestamps = Vec::with_capacity(total_rows);
    let mut symbols: Vec<String> = Vec::with_capacity(total_rows);
    let mut exchanges: Vec<String> = Vec::with_capacity(total_rows);
    let mut sides: Vec<&str> = Vec::with_capacity(total_rows);
    let mut levels: Vec<u64> = Vec::with_capacity(total_rows);
    let mut prices = Vec::with_capacity(total_rows);
    let mut sizes = Vec::with_capacity(total_rows);
    let mut source_ts = Vec::with_capacity(total_rows);
    let mut local_ts = Vec::with_capacity(total_rows);
    let mut rtts = Vec::with_capacity(total_rows);

    for ob in snapshots {
        // BTreeMap iterates in ascending price order.
        // For bids, reverse to get highest-first (traditional L2 ordering).
        let pair_str = ob.pair.to_canonical();
        for (idx, (_price, bid)) in ob.bids.iter().rev().enumerate() {
            timestamps.push(ob.orderbook_ts_us);
            symbols.push(pair_str.clone());
            exchanges.push(ob.exchange.clone());
            sides.push(OrderSide::Bids.as_str());
            levels.push(idx as u64);
            prices.push(decimal_to_f64(bid.price));
            sizes.push(decimal_to_f64(bid.volume));
            source_ts.push(ob.source_orderbook_ts_us);
            local_ts.push(ob.local_orderbook_ts_us);
            rtts.push(ob.source_orderbook_rtt_us);
        }
        for (idx, (_price, ask)) in ob.asks.iter().enumerate() {
            timestamps.push(ob.orderbook_ts_us);
            symbols.push(pair_str.clone());
            exchanges.push(ob.exchange.clone());
            sides.push(OrderSide::Asks.as_str());
            levels.push(idx as u64);
            prices.push(decimal_to_f64(ask.price));
            sizes.push(decimal_to_f64(ask.volume));
            source_ts.push(ob.source_orderbook_ts_us);
            local_ts.push(ob.local_orderbook_ts_us);
            rtts.push(ob.source_orderbook_rtt_us);
        }
    }

    let schema = Schema::new(vec![
        Field::new("orderbook_ts_us", DataType::UInt64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("side", DataType::Utf8, false),
        Field::new("level", DataType::UInt64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("size", DataType::Float64, false),
        Field::new("source_orderbook_ts_us", DataType::UInt64, false),
        Field::new("local_orderbook_ts_us", DataType::UInt64, false),
        Field::new("source_orderbook_rtt_us", DataType::UInt64, false),
    ]);

    let symbols_refs: Vec<&str> = symbols.iter().map(|s| s.as_str()).collect();
    let exchanges_refs: Vec<&str> = exchanges.iter().map(|e| e.as_str()).collect();
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt64Array::from(timestamps)),
            Arc::new(StringArray::from(symbols_refs)),
            Arc::new(StringArray::from(exchanges_refs)),
            Arc::new(StringArray::from(sides)),
            Arc::new(UInt64Array::from(levels)),
            Arc::new(Float64Array::from(prices)),
            Arc::new(Float64Array::from(sizes)),
            Arc::new(UInt64Array::from(source_ts)),
            Arc::new(UInt64Array::from(local_ts)),
            Arc::new(UInt64Array::from(rtts)),
        ],
    )
    .map_err(crate::parquet_err::from_arrow)?;

    let file_ts = chrono::Utc::now().format("%Y%m%d_%H%M%S%.3f");
    let filename = format!(
        "{}_{}_ob_{}_{}.parquet",
        file_exchange, file_symbol, mode, file_ts
    );
    let path = output_dir.join(filename);
    let file = File::create(&path)?;

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
        .map_err(crate::parquet_err::from_parquet)?;
    writer
        .write(&batch)
        .map_err(crate::parquet_err::from_parquet)?;
    writer.close().map_err(crate::parquet_err::from_parquet)?;

    Ok(path)
}
