use aetelier_types::errors::PersistError;
use aetelier_types::open_interest::OpenInterest;
#[cfg(feature = "parquet")]
use aetelier_types::orderbooks::{decimal_to_f64, f64_to_decimal};
#[cfg(feature = "parquet")]
use aetelier_types::trading_pair::TradingPair;
use std::path::Path;

#[cfg(feature = "parquet")]
pub fn write_oi_parquet(
    records: &[OpenInterest],
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

    let n = records.len();

    let mut timestamps = Vec::with_capacity(n);
    let mut symbols: Vec<String> = Vec::with_capacity(n);
    let mut oi_values = Vec::with_capacity(n);
    let mut oi_usd_values: Vec<Option<f64>> = Vec::with_capacity(n);
    let mut exchanges: Vec<&str> = Vec::with_capacity(n);
    let mut local_timestamps = Vec::with_capacity(n);
    let mut recv_seqs = Vec::with_capacity(n);
    let mut conn_epochs = Vec::with_capacity(n);
    let mut mark_pxs: Vec<Option<f64>> = Vec::with_capacity(n);

    for oi in records {
        timestamps.push(oi.open_interest_ts_us);
        symbols.push(oi.pair.to_canonical());
        oi_values.push(decimal_to_f64(oi.open_interest));
        oi_usd_values.push(oi.open_interest_value.map(decimal_to_f64));
        exchanges.push(&oi.exchange);
        local_timestamps.push(oi.local_oi_ts_us);
        recv_seqs.push(oi.recv_seq);
        conn_epochs.push(oi.conn_epoch);
        mark_pxs.push(oi.mark_px.map(decimal_to_f64));
    }

    let schema = Schema::new(vec![
        Field::new("open_interest_ts_us", DataType::UInt64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("open_interest", DataType::Float64, false),
        Field::new("open_interest_value", DataType::Float64, true),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("local_oi_ts_us", DataType::UInt64, false),
        Field::new("recv_seq", DataType::UInt64, false),
        Field::new("conn_epoch", DataType::UInt32, false),
        Field::new("mark_px", DataType::Float64, true),
    ]);

    let symbols_refs: Vec<&str> = symbols.iter().map(|s| s.as_str()).collect();
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt64Array::from(timestamps)),
            Arc::new(StringArray::from(symbols_refs)),
            Arc::new(Float64Array::from(oi_values)),
            Arc::new(Float64Array::from(oi_usd_values)),
            Arc::new(StringArray::from(exchanges)),
            Arc::new(UInt64Array::from(local_timestamps)),
            Arc::new(UInt64Array::from(recv_seqs)),
            Arc::new(UInt32Array::from(conn_epochs)),
            Arc::new(Float64Array::from(mark_pxs)),
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
pub fn write_oi_parquet(
    _records: &[OpenInterest],
    _path: &Path,
) -> Result<(), PersistError> {
    Err(PersistError::UnsupportedFormat(
        "parquet support not compiled in (enable 'parquet' feature)".to_string(),
    ))
}

#[cfg(feature = "parquet")]
pub fn read_oi_parquet(path: &Path) -> Result<Vec<OpenInterest>, PersistError> {
    use arrow::array::{Array, AsArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(crate::parquet_err::from_parquet)?;
    let reader = builder.build().map_err(crate::parquet_err::from_parquet)?;

    let mut records = Vec::new();

    for batch_result in reader {
        let batch = batch_result.map_err(crate::parquet_err::from_arrow)?;
        let extended = batch.num_columns() >= 9;

        let timestamps = batch
            .column(0)
            .as_primitive::<arrow::datatypes::UInt64Type>();
        let symbols = batch.column(1).as_string::<i32>();
        let oi_values = batch
            .column(2)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let oi_usd_values = batch
            .column(3)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let exchanges = batch.column(4).as_string::<i32>();

        for i in 0..batch.num_rows() {
            let (local_ts, recv_seq, conn_epoch, mark_px) = if extended {
                let locals = batch
                    .column(5)
                    .as_primitive::<arrow::datatypes::UInt64Type>();
                let seqs = batch
                    .column(6)
                    .as_primitive::<arrow::datatypes::UInt64Type>();
                let epochs = batch
                    .column(7)
                    .as_primitive::<arrow::datatypes::UInt32Type>();
                let marks = batch
                    .column(8)
                    .as_primitive::<arrow::datatypes::Float64Type>();
                (
                    locals.value(i),
                    seqs.value(i),
                    epochs.value(i),
                    marks.is_valid(i).then(|| f64_to_decimal(marks.value(i))),
                )
            } else {
                (0, 0, 0, None)
            };
            records.push(OpenInterest {
                open_interest_ts_us: timestamps.value(i),
                local_oi_ts_us: local_ts,
                recv_seq,
                conn_epoch,
                pair: symbols.value(i).parse::<TradingPair>().map_err(|_| {
                    PersistError::Parse(format!(
                        "row {i}: malformed symbol '{}' in {}",
                        symbols.value(i),
                        path.display()
                    ))
                })?,
                open_interest: f64_to_decimal(oi_values.value(i)),
                open_interest_value: oi_usd_values
                    .is_valid(i)
                    .then(|| f64_to_decimal(oi_usd_values.value(i))),
                mark_px,
                exchange: exchanges.value(i).to_string(),
            });
        }
    }

    Ok(records)
}

#[cfg(not(feature = "parquet"))]
pub fn read_oi_parquet(_path: &Path) -> Result<Vec<OpenInterest>, PersistError> {
    Err(PersistError::UnsupportedFormat(
        "parquet support not compiled in (enable 'parquet' feature)".to_string(),
    ))
}

#[cfg(feature = "parquet")]
pub fn write_oi_parquet_timestamped(
    records: &[OpenInterest],
    output_dir: &Path,
    mode: &str,
) -> Result<std::path::PathBuf, PersistError> {
    let file_ts = chrono::Utc::now().format("%Y%m%d_%H%M%S%.3f");
    let raw_symbol = records
        .first()
        .map(|r| r.pair.to_canonical())
        .unwrap_or_else(|| "unknown".to_string());
    let exchange = records
        .first()
        .map(|r| r.exchange.as_str())
        .unwrap_or("unknown");
    let symbol = raw_symbol.replace('/', "-");
    let filename = format!("{}_{}_oi_{}_{}.parquet", exchange, symbol, mode, file_ts);
    let path = output_dir.join(filename);
    write_oi_parquet(records, &path)?;
    Ok(path)
}

#[cfg(not(feature = "parquet"))]
pub fn write_oi_parquet_timestamped(
    _records: &[OpenInterest],
    _output_dir: &Path,
    _mode: &str,
) -> Result<std::path::PathBuf, PersistError> {
    Err(PersistError::UnsupportedFormat(
        "parquet support not compiled in (enable 'parquet' feature)".to_string(),
    ))
}
