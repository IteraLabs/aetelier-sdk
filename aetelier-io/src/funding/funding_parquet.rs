use aetelier_types::errors::PersistError;
use aetelier_types::funding::FundingRate;
#[cfg(feature = "parquet")]
use aetelier_types::orderbooks::{decimal_to_f64, f64_to_decimal};
#[cfg(feature = "parquet")]
use aetelier_types::trading_pair::TradingPair;
use std::path::Path;

#[cfg(feature = "parquet")]
/// Read the connection-epoch column, tolerating the pre-microsecond archive.
///
/// New files carry `conn_epoch_us` as `UInt64` (Unix microseconds). Files
/// written before the widen carry a `UInt32` `conn_epoch` at the same ordinal,
/// in seconds and — nothing ever assigned it — always zero. Those read as `0`
/// rather than being widened, so a seconds magnitude can never be mistaken for
/// a microsecond instant.
pub(crate) fn conn_epoch_us_at(
    batch: &arrow::record_batch::RecordBatch,
    ordinal: usize,
    i: usize,
) -> u64 {
    use arrow::array::AsArray;
    let col = batch.column(ordinal);
    match col.data_type() {
        arrow::datatypes::DataType::UInt64 => {
            col.as_primitive::<arrow::datatypes::UInt64Type>().value(i)
        }
        _ => 0,
    }
}

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
    let mut conn_epochs_us = Vec::with_capacity(n);
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
        conn_epochs_us.push(fr.conn_epoch_us);
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
        Field::new("conn_epoch_us", DataType::UInt64, false),
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
            Arc::new(UInt64Array::from(conn_epochs_us)),
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
            let (local_ts, recv_seq, conn_epoch_us, premium, interval_hours) = if extended
            {
                let locals = batch
                    .column(5)
                    .as_primitive::<arrow::datatypes::UInt64Type>();
                let seqs = batch
                    .column(6)
                    .as_primitive::<arrow::datatypes::UInt64Type>();
                let premiums = batch
                    .column(8)
                    .as_primitive::<arrow::datatypes::Float64Type>();
                let intervals = batch
                    .column(9)
                    .as_primitive::<arrow::datatypes::UInt32Type>();
                (
                    locals.value(i),
                    seqs.value(i),
                    conn_epoch_us_at(&batch, 7, i),
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
                conn_epoch_us,
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
    let file_ts = crate::naming::batch_stamp(rates.iter().map(|r| {
        crate::naming::effective_us(r.funding_rate_ts_us, r.local_funding_ts_us)
    }));
    let raw_symbol = rates
        .first()
        .map(|r| r.pair.to_canonical())
        .unwrap_or_else(|| "unknown".to_string());
    let exchange = rates
        .first()
        .map(|r| r.exchange.as_str())
        .unwrap_or("unknown");
    let symbol = raw_symbol.replace('/', "-").replace(':', "_");
    let filename = format!(
        "{}_{}_funding_{}_{}.parquet",
        exchange, symbol, mode, file_ts
    );
    let path = crate::naming::unique_path(output_dir, &filename);
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

#[cfg(all(test, feature = "parquet"))]
mod tests {
    use super::*;

    #[test]
    fn dex_prefixed_pairs_sanitize_colon_in_filenames_only() {
        let dir = tempfile::tempdir().unwrap();
        let rate = aetelier_types::funding::FundingRate {
            funding_rate_ts_us: 1_786_399_200_000_000,
            local_funding_ts_us: 1_786_399_200_000_000,
            recv_seq: 1,
            conn_epoch_us: 0,
            pair: aetelier_types::trading_pair::TradingPair::new("xyz:TSLA", "USDC"),
            funding_rate: rust_decimal::Decimal::new(-894, 10),
            premium: None,
            interval_hours: 1,
            next_funding_ts_us: 0,
            exchange: "hyperliquid".to_string(),
        };
        let path =
            write_funding_parquet_timestamped(&[rate], dir.path(), "sync").unwrap();
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        assert!(
            name.starts_with("hyperliquid_xyz_TSLA-USDC_funding_sync_"),
            "filename must sanitize ':' to '_', got {name}"
        );
        let rows = read_funding_parquet(&path).unwrap();
        assert_eq!(rows[0].pair.base(), "xyz:TSLA");
    }
}
