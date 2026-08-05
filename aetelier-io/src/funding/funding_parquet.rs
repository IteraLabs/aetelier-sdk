use aetelier_types::errors::PersistError;
use aetelier_types::funding::FundingRate;
#[cfg(feature = "parquet")]
use aetelier_types::orderbooks::{decimal_to_f64, f64_to_decimal};
#[cfg(feature = "parquet")]
use aetelier_types::trading_pair::TradingPair;
use std::path::Path;

#[cfg(feature = "parquet")]
pub fn write_funding_parquet(
    rates: &[FundingRate],
    path: &Path,
) -> Result<(), PersistError> {
    use arrow::{
        array::{Float64Array, StringArray, UInt32Array, UInt64Array},
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
    let mut local_timestamps = Vec::with_capacity(n);
    let mut recv_seqs = Vec::with_capacity(n);
    let mut conn_epochs = Vec::with_capacity(n);
    let mut premiums: Vec<Option<f64>> = Vec::with_capacity(n);
    let mut intervals = Vec::with_capacity(n);

    for fr in rates {
        timestamps.push(fr.funding_rate_ts_us);
        symbols.push(fr.pair.to_canonical());
        funding_rates.push(decimal_to_f64(fr.funding_rate));
        next_funding_timestamps.push(fr.next_funding_ts_us);
        exchanges.push(&fr.exchange);
        local_timestamps.push(fr.local_funding_ts_us);
        recv_seqs.push(fr.recv_seq);
        conn_epochs.push(fr.conn_epoch);
        premiums.push(fr.premium.map(decimal_to_f64));
        intervals.push(fr.interval_hours);
    }

    let schema = Schema::new(vec![
        Field::new("funding_rate_ts_us", DataType::UInt64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("funding_rate", DataType::Float64, false),
        Field::new("next_funding_ts_us", DataType::UInt64, false),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("local_funding_ts_us", DataType::UInt64, false),
        Field::new("recv_seq", DataType::UInt64, false),
        Field::new("conn_epoch", DataType::UInt32, false),
        Field::new("premium", DataType::Float64, true),
        Field::new("interval_hours", DataType::UInt32, false),
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
            Arc::new(UInt64Array::from(local_timestamps)),
            Arc::new(UInt64Array::from(recv_seqs)),
            Arc::new(UInt32Array::from(conn_epochs)),
            Arc::new(Float64Array::from(premiums)),
            Arc::new(UInt32Array::from(intervals)),
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

#[cfg(feature = "parquet")]
pub fn read_funding_parquet(path: &Path) -> Result<Vec<FundingRate>, PersistError> {
    use arrow::array::{Array, AsArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(crate::parquet_err::from_parquet)?;
    let reader = builder.build().map_err(crate::parquet_err::from_parquet)?;

    let mut rates = Vec::new();

    for batch_result in reader {
        let batch = batch_result.map_err(crate::parquet_err::from_arrow)?;
        let extended = batch.num_columns() >= 10;

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
            let (local_ts, recv_seq, conn_epoch, premium, interval_hours) = if extended {
                let locals = batch
                    .column(5)
                    .as_primitive::<arrow::datatypes::UInt64Type>();
                let seqs = batch
                    .column(6)
                    .as_primitive::<arrow::datatypes::UInt64Type>();
                let epochs = batch
                    .column(7)
                    .as_primitive::<arrow::datatypes::UInt32Type>();
                let premiums = batch
                    .column(8)
                    .as_primitive::<arrow::datatypes::Float64Type>();
                let intervals = batch
                    .column(9)
                    .as_primitive::<arrow::datatypes::UInt32Type>();
                (
                    locals.value(i),
                    seqs.value(i),
                    epochs.value(i),
                    premiums
                        .is_valid(i)
                        .then(|| f64_to_decimal(premiums.value(i))),
                    intervals.value(i),
                )
            } else {
                (0, 0, 0, None, 8)
            };
            rates.push(FundingRate {
                funding_rate_ts_us: timestamps.value(i),
                local_funding_ts_us: local_ts,
                recv_seq,
                conn_epoch,
                pair: symbols.value(i).parse::<TradingPair>().map_err(|_| {
                    PersistError::Parse(format!(
                        "row {i}: malformed symbol '{}' in {}",
                        symbols.value(i),
                        path.display()
                    ))
                })?,
                funding_rate: f64_to_decimal(funding_rates.value(i)),
                premium,
                interval_hours,
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
