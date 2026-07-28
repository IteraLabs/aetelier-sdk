//! Parquet I/O for aggregated market statistics.
//!
//! Write and read [`MarketAggregate`] records to/from Parquet files.
//! The struct definition lives in [`aetelier_types`].

use aetelier_types::errors::PersistError;
use aetelier_types::snapshots::MarketAggregate;

/// Write aggregated market statistics to a Parquet file.
///
/// # Output Schema
///
/// | Column | Arrow Type | Description |
/// |--------|------------|-------------|
/// | `ts_us` | `UInt64` | Timestamp in microseconds |
/// | `ob_mid_price` | `Float64` | Mid-price |
/// | `ob_spread` | `Float64` | Bid-ask spread |
/// | `ob_imbalance` | `Float64` | Volume imbalance [-1, 1] |
/// | `trade_volume` | `Float64` | Total trade volume |
/// | `trade_vwap` | `Float64` | VWAP |
/// | `trade_count` | `UInt64` | Number of trades |
/// | `liq_notional` | `Float64` | Liquidation notional |
/// | `liq_count` | `UInt64` | Number of liquidations |
/// | `liq_imbalance` | `Float64` | Liquidation directional imbalance |
/// | `fr_rate` | `Float64` | Funding rate |
/// | `fr_annualized` | `Float64` | Annualized funding |
/// | `fr_next_settlement_delta` | `Int64` | Time to next settlement (µs) |
/// | `oi_contracts` | `Float64` | Open interest (contracts) |
/// | `oi_value` | `Float64` | Open interest (quote) |
/// | `oi_change` | `Float64` | OI delta from previous |
/// | `trade_notional` | `Float64` | Total traded notional |
/// | `trade_buy_volume` | `Float64` | Buy-side volume |
/// | `trade_buy_notional` | `Float64` | Buy-side notional |
/// | `trade_buy_count` | `UInt64` | Buy-side trade count |
/// | `trade_sell_volume` | `Float64` | Sell-side volume |
/// | `trade_sell_notional` | `Float64` | Sell-side notional |
/// | `trade_sell_count` | `UInt64` | Sell-side trade count |
/// | `trade_imbalance` | `Float64` | Order-flow imbalance [-1, 1] |
/// | `trade_px_first` | `Float64` | First trade price in period |
/// | `trade_px_last` | `Float64` | Last trade price in period |
///
/// The ten trade-side columns are appended after `oi_change`, so a reader of
/// an older 16-column file must be upgraded (schema change, re-ingest).
#[cfg(feature = "parquet")]
pub fn write_market_aggregate_parquet(
    aggregates: &[MarketAggregate],
    output_dir: &std::path::Path,
) -> Result<std::path::PathBuf, PersistError> {
    use arrow::{
        array::{Float64Array, Int64Array, UInt64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use parquet::{
        arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties,
    };
    use std::{fs::File, sync::Arc};

    let n = aggregates.len();

    let mut ts_us_vec = Vec::with_capacity(n);
    let mut ob_mid_vec = Vec::with_capacity(n);
    let mut ob_spread_vec = Vec::with_capacity(n);
    let mut ob_imb_vec = Vec::with_capacity(n);
    let mut tv_vec = Vec::with_capacity(n);
    let mut vwap_vec = Vec::with_capacity(n);
    let mut tc_vec = Vec::with_capacity(n);
    let mut ln_vec = Vec::with_capacity(n);
    let mut lc_vec = Vec::with_capacity(n);
    let mut li_vec = Vec::with_capacity(n);
    let mut fr_vec = Vec::with_capacity(n);
    let mut fra_vec = Vec::with_capacity(n);
    let mut frd_vec = Vec::with_capacity(n);
    let mut oic_vec = Vec::with_capacity(n);
    let mut oiv_vec = Vec::with_capacity(n);
    let mut oid_vec = Vec::with_capacity(n);
    let mut tn_vec = Vec::with_capacity(n);
    let mut tbv_vec = Vec::with_capacity(n);
    let mut tbn_vec = Vec::with_capacity(n);
    let mut tbc_vec = Vec::with_capacity(n);
    let mut tsv_vec = Vec::with_capacity(n);
    let mut tsn_vec = Vec::with_capacity(n);
    let mut tsc_vec = Vec::with_capacity(n);
    let mut timb_vec = Vec::with_capacity(n);
    let mut tpf_vec = Vec::with_capacity(n);
    let mut tpl_vec = Vec::with_capacity(n);

    for agg in aggregates {
        ts_us_vec.push(agg.ts_us);
        ob_mid_vec.push(agg.ob_mid_price);
        ob_spread_vec.push(agg.ob_spread);
        ob_imb_vec.push(agg.ob_imbalance);
        tv_vec.push(agg.trade_volume);
        vwap_vec.push(agg.trade_vwap);
        tc_vec.push(agg.trade_count);
        ln_vec.push(agg.liq_notional);
        lc_vec.push(agg.liq_count);
        li_vec.push(agg.liq_imbalance);
        fr_vec.push(agg.fr_rate);
        fra_vec.push(agg.fr_annualized);
        frd_vec.push(agg.fr_next_settlement_delta);
        oic_vec.push(agg.oi_contracts);
        oiv_vec.push(agg.oi_value);
        oid_vec.push(agg.oi_change);
        tn_vec.push(agg.trade_notional);
        tbv_vec.push(agg.trade_buy_volume);
        tbn_vec.push(agg.trade_buy_notional);
        tbc_vec.push(agg.trade_buy_count);
        tsv_vec.push(agg.trade_sell_volume);
        tsn_vec.push(agg.trade_sell_notional);
        tsc_vec.push(agg.trade_sell_count);
        timb_vec.push(agg.trade_imbalance);
        tpf_vec.push(agg.trade_px_first);
        tpl_vec.push(agg.trade_px_last);
    }

    let schema = Schema::new(vec![
        Field::new("ts_us", DataType::UInt64, false),
        Field::new("ob_mid_price", DataType::Float64, false),
        Field::new("ob_spread", DataType::Float64, false),
        Field::new("ob_imbalance", DataType::Float64, false),
        Field::new("trade_volume", DataType::Float64, false),
        Field::new("trade_vwap", DataType::Float64, false),
        Field::new("trade_count", DataType::UInt64, false),
        Field::new("liq_notional", DataType::Float64, false),
        Field::new("liq_count", DataType::UInt64, false),
        Field::new("liq_imbalance", DataType::Float64, false),
        Field::new("fr_rate", DataType::Float64, false),
        Field::new("fr_annualized", DataType::Float64, false),
        Field::new("fr_next_settlement_delta", DataType::Int64, false),
        Field::new("oi_contracts", DataType::Float64, false),
        Field::new("oi_value", DataType::Float64, false),
        Field::new("oi_change", DataType::Float64, false),
        Field::new("trade_notional", DataType::Float64, false),
        Field::new("trade_buy_volume", DataType::Float64, false),
        Field::new("trade_buy_notional", DataType::Float64, false),
        Field::new("trade_buy_count", DataType::UInt64, false),
        Field::new("trade_sell_volume", DataType::Float64, false),
        Field::new("trade_sell_notional", DataType::Float64, false),
        Field::new("trade_sell_count", DataType::UInt64, false),
        Field::new("trade_imbalance", DataType::Float64, false),
        Field::new("trade_px_first", DataType::Float64, false),
        Field::new("trade_px_last", DataType::Float64, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt64Array::from(ts_us_vec)),
            Arc::new(Float64Array::from(ob_mid_vec)),
            Arc::new(Float64Array::from(ob_spread_vec)),
            Arc::new(Float64Array::from(ob_imb_vec)),
            Arc::new(Float64Array::from(tv_vec)),
            Arc::new(Float64Array::from(vwap_vec)),
            Arc::new(UInt64Array::from(tc_vec)),
            Arc::new(Float64Array::from(ln_vec)),
            Arc::new(UInt64Array::from(lc_vec)),
            Arc::new(Float64Array::from(li_vec)),
            Arc::new(Float64Array::from(fr_vec)),
            Arc::new(Float64Array::from(fra_vec)),
            Arc::new(Int64Array::from(frd_vec)),
            Arc::new(Float64Array::from(oic_vec)),
            Arc::new(Float64Array::from(oiv_vec)),
            Arc::new(Float64Array::from(oid_vec)),
            Arc::new(Float64Array::from(tn_vec)),
            Arc::new(Float64Array::from(tbv_vec)),
            Arc::new(Float64Array::from(tbn_vec)),
            Arc::new(UInt64Array::from(tbc_vec)),
            Arc::new(Float64Array::from(tsv_vec)),
            Arc::new(Float64Array::from(tsn_vec)),
            Arc::new(UInt64Array::from(tsc_vec)),
            Arc::new(Float64Array::from(timb_vec)),
            Arc::new(Float64Array::from(tpf_vec)),
            Arc::new(Float64Array::from(tpl_vec)),
        ],
    )
    .map_err(crate::parquet_err::from_arrow)?;

    let file_ts = chrono::Utc::now().format("%Y%m%d_%H%M%S%.3f");
    let filename = format!("market_aggregate_{}.parquet", file_ts);
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

/// Stub for when the `parquet` feature is not enabled.
#[cfg(not(feature = "parquet"))]
pub fn write_market_aggregate_parquet(
    _aggregates: &[MarketAggregate],
    _output_dir: &std::path::Path,
) -> Result<std::path::PathBuf, PersistError> {
    Err(PersistError::UnsupportedFormat(
        "parquet support not compiled in (enable 'parquet' feature)".to_string(),
    ))
}

/// Read aggregated market statistics from a Parquet file.
#[cfg(feature = "parquet")]
pub fn read_market_aggregate_parquet(
    path: &std::path::Path,
) -> Result<Vec<MarketAggregate>, PersistError> {
    use arrow::array::AsArray;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(crate::parquet_err::from_parquet)?;
    let reader = builder.build().map_err(crate::parquet_err::from_parquet)?;

    let mut aggregates = Vec::new();

    for batch_result in reader {
        let batch = batch_result.map_err(crate::parquet_err::from_arrow)?;

        let ts_us = batch
            .column(0)
            .as_primitive::<arrow::datatypes::UInt64Type>();
        let ob_mid = batch
            .column(1)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let ob_spread = batch
            .column(2)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let ob_imb = batch
            .column(3)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let tv = batch
            .column(4)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let vwap = batch
            .column(5)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let tc = batch
            .column(6)
            .as_primitive::<arrow::datatypes::UInt64Type>();
        let ln = batch
            .column(7)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let lc = batch
            .column(8)
            .as_primitive::<arrow::datatypes::UInt64Type>();
        let li = batch
            .column(9)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let fr = batch
            .column(10)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let fra = batch
            .column(11)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let frd = batch
            .column(12)
            .as_primitive::<arrow::datatypes::Int64Type>();
        let oic = batch
            .column(13)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let oiv = batch
            .column(14)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let oid = batch
            .column(15)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let tn = batch
            .column(16)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let tbv = batch
            .column(17)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let tbn = batch
            .column(18)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let tbc = batch
            .column(19)
            .as_primitive::<arrow::datatypes::UInt64Type>();
        let tsv = batch
            .column(20)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let tsn = batch
            .column(21)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let tsc = batch
            .column(22)
            .as_primitive::<arrow::datatypes::UInt64Type>();
        let timb = batch
            .column(23)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let tpf = batch
            .column(24)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let tpl = batch
            .column(25)
            .as_primitive::<arrow::datatypes::Float64Type>();

        for i in 0..batch.num_rows() {
            aggregates.push(MarketAggregate {
                ts_us: ts_us.value(i),
                ob_mid_price: ob_mid.value(i),
                ob_spread: ob_spread.value(i),
                ob_imbalance: ob_imb.value(i),
                trade_volume: tv.value(i),
                trade_vwap: vwap.value(i),
                trade_count: tc.value(i),
                trade_notional: tn.value(i),
                trade_buy_volume: tbv.value(i),
                trade_buy_notional: tbn.value(i),
                trade_buy_count: tbc.value(i),
                trade_sell_volume: tsv.value(i),
                trade_sell_notional: tsn.value(i),
                trade_sell_count: tsc.value(i),
                trade_imbalance: timb.value(i),
                trade_px_first: tpf.value(i),
                trade_px_last: tpl.value(i),
                liq_notional: ln.value(i),
                liq_count: lc.value(i),
                liq_imbalance: li.value(i),
                fr_rate: fr.value(i),
                fr_annualized: fra.value(i),
                fr_next_settlement_delta: frd.value(i),
                oi_contracts: oic.value(i),
                oi_value: oiv.value(i),
                oi_change: oid.value(i),
            });
        }
    }

    Ok(aggregates)
}

/// Stub for when the `parquet` feature is not enabled.
#[cfg(not(feature = "parquet"))]
pub fn read_market_aggregate_parquet(
    _path: &std::path::Path,
) -> Result<Vec<MarketAggregate>, PersistError> {
    Err(PersistError::UnsupportedFormat(
        "parquet support not compiled in (enable 'parquet' feature)".to_string(),
    ))
}

#[cfg(all(test, feature = "parquet"))]
mod tests {
    use super::*;
    use aetelier_types::snapshots::MarketAggregate;

    /// Every field distinct so a column swap in the 26-column schema shows up
    /// as a mismatched value on read-back.
    fn sample() -> MarketAggregate {
        MarketAggregate {
            ts_us: 1_700_000_000_000_000,
            ob_mid_price: 1.0,
            ob_spread: 2.0,
            ob_imbalance: 3.0,
            trade_volume: 4.0,
            trade_vwap: 5.0,
            trade_count: 6,
            trade_notional: 7.0,
            trade_buy_volume: 8.0,
            trade_buy_notional: 9.0,
            trade_buy_count: 10,
            trade_sell_volume: 11.0,
            trade_sell_notional: 12.0,
            trade_sell_count: 13,
            trade_imbalance: 14.0,
            trade_px_first: 15.0,
            trade_px_last: 16.0,
            liq_notional: 17.0,
            liq_count: 18,
            liq_imbalance: 19.0,
            fr_rate: 20.0,
            fr_annualized: 21.0,
            fr_next_settlement_delta: 22,
            oi_contracts: 23.0,
            oi_value: 24.0,
            oi_change: 25.0,
        }
    }

    #[test]
    fn parquet_roundtrip_preserves_every_field() {
        let dir = std::env::temp_dir().join(format!("aggio_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        let orig = sample();
        let path =
            write_market_aggregate_parquet(std::slice::from_ref(&orig), &dir).unwrap();
        let back = read_market_aggregate_parquet(&path).unwrap();
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);

        assert_eq!(back.len(), 1);
        // PartialEq over all 26 fields catches any column misordering.
        assert_eq!(back[0], orig);
    }
}
