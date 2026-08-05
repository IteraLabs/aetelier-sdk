use aetelier_types::errors::PersistError;
use aetelier_types::funding::FundingSettlement;
#[cfg(feature = "parquet")]
use aetelier_types::orderbooks::{decimal_to_f64, f64_to_decimal};
#[cfg(feature = "parquet")]
use aetelier_types::trading_pair::TradingPair;
use std::path::Path;

#[cfg(feature = "parquet")]
pub fn write_funding_settlement_parquet(
    settlements: &[FundingSettlement],
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

    let n = settlements.len();

    let mut funding_times = Vec::with_capacity(n);
    let mut symbols: Vec<String> = Vec::with_capacity(n);
    let mut rates = Vec::with_capacity(n);
    let mut premiums: Vec<Option<f64>> = Vec::with_capacity(n);
    let mut exchanges: Vec<&str> = Vec::with_capacity(n);
    let mut local_timestamps = Vec::with_capacity(n);
    let mut rtts = Vec::with_capacity(n);

    for fs in settlements {
        funding_times.push(fs.funding_time_us);
        symbols.push(fs.pair.to_canonical());
        rates.push(decimal_to_f64(fs.funding_rate));
        premiums.push(fs.premium.map(decimal_to_f64));
        exchanges.push(&fs.exchange);
        local_timestamps.push(fs.local_ts_us);
        rtts.push(fs.rtt_us);
    }

    let schema = Schema::new(vec![
        Field::new("funding_time_us", DataType::UInt64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("funding_rate", DataType::Float64, false),
        Field::new("premium", DataType::Float64, true),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("local_ts_us", DataType::UInt64, false),
        Field::new("rtt_us", DataType::UInt64, false),
    ]);

    let symbols_refs: Vec<&str> = symbols.iter().map(|s| s.as_str()).collect();
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt64Array::from(funding_times)),
            Arc::new(StringArray::from(symbols_refs)),
            Arc::new(Float64Array::from(rates)),
            Arc::new(Float64Array::from(premiums)),
            Arc::new(StringArray::from(exchanges)),
            Arc::new(UInt64Array::from(local_timestamps)),
            Arc::new(UInt64Array::from(rtts)),
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
pub fn write_funding_settlement_parquet(
    _settlements: &[FundingSettlement],
    _path: &Path,
) -> Result<(), PersistError> {
    Err(PersistError::UnsupportedFormat(
        "parquet support not compiled in (enable 'parquet' feature)".to_string(),
    ))
}

#[cfg(feature = "parquet")]
pub fn read_funding_settlement_parquet(
    path: &Path,
) -> Result<Vec<FundingSettlement>, PersistError> {
    use arrow::array::{Array, AsArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(crate::parquet_err::from_parquet)?;
    let reader = builder.build().map_err(crate::parquet_err::from_parquet)?;

    let mut settlements = Vec::new();

    for batch_result in reader {
        let batch = batch_result.map_err(crate::parquet_err::from_arrow)?;

        let funding_times = batch
            .column(0)
            .as_primitive::<arrow::datatypes::UInt64Type>();
        let symbols = batch.column(1).as_string::<i32>();
        let rates = batch
            .column(2)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let premiums = batch
            .column(3)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let exchanges = batch.column(4).as_string::<i32>();
        let locals = batch
            .column(5)
            .as_primitive::<arrow::datatypes::UInt64Type>();
        let rtts = batch
            .column(6)
            .as_primitive::<arrow::datatypes::UInt64Type>();

        for i in 0..batch.num_rows() {
            settlements.push(FundingSettlement {
                funding_time_us: funding_times.value(i),
                local_ts_us: locals.value(i),
                rtt_us: rtts.value(i),
                pair: symbols.value(i).parse::<TradingPair>().map_err(|_| {
                    PersistError::Parse(format!(
                        "row {i}: malformed symbol '{}' in {}",
                        symbols.value(i),
                        path.display()
                    ))
                })?,
                funding_rate: f64_to_decimal(rates.value(i)),
                premium: premiums
                    .is_valid(i)
                    .then(|| f64_to_decimal(premiums.value(i))),
                exchange: exchanges.value(i).to_string(),
            });
        }
    }

    Ok(settlements)
}

#[cfg(not(feature = "parquet"))]
pub fn read_funding_settlement_parquet(
    _path: &Path,
) -> Result<Vec<FundingSettlement>, PersistError> {
    Err(PersistError::UnsupportedFormat(
        "parquet support not compiled in (enable 'parquet' feature)".to_string(),
    ))
}

#[cfg(feature = "parquet")]
pub fn write_funding_settlement_parquet_timestamped(
    settlements: &[FundingSettlement],
    output_dir: &Path,
    mode: &str,
) -> Result<std::path::PathBuf, PersistError> {
    let file_ts = chrono::Utc::now().format("%Y%m%d_%H%M%S%.3f");
    let raw_symbol = settlements
        .first()
        .map(|r| r.pair.to_canonical())
        .unwrap_or_else(|| "unknown".to_string());
    let exchange = settlements
        .first()
        .map(|r| r.exchange.as_str())
        .unwrap_or("unknown");
    let symbol = raw_symbol.replace('/', "-");
    let filename = format!(
        "{}_{}_funding_settlement_{}_{}.parquet",
        exchange, symbol, mode, file_ts
    );
    let path = output_dir.join(filename);
    write_funding_settlement_parquet(settlements, &path)?;
    Ok(path)
}

#[cfg(not(feature = "parquet"))]
pub fn write_funding_settlement_parquet_timestamped(
    _settlements: &[FundingSettlement],
    _output_dir: &Path,
    _mode: &str,
) -> Result<std::path::PathBuf, PersistError> {
    Err(PersistError::UnsupportedFormat(
        "parquet support not compiled in (enable 'parquet' feature)".to_string(),
    ))
}
