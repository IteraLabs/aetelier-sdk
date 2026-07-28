//! Corrupted-file goldens for the trades Parquet reader.
//!
//! A malformed row must surface as a typed [`PersistError::Parse`] naming the
//! row, the offending value, and the file — never as a panic and never as a
//! fabricated default (an unknown side coerced to `Buy` silently biases any
//! signed-flow computation done on the replayed data).
//!
//! These tests require the `parquet` feature:
//!   cargo test --features parquet -p aetelier-io --test test_trades_parquet_corrupt

#[cfg(test)]
#[cfg(feature = "parquet")]
mod tests {
    use std::fs::File;
    use std::sync::Arc;

    use aetelier_io::trades::trades_parquet::read_trades_parquet;
    use aetelier_types::errors::PersistError;
    use arrow::array::{ArrayRef, Float64Array, StringArray, UInt64Array};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use tempfile::tempdir;

    /// Write a single-row trades file with the given symbol and side strings,
    /// bypassing the typed writer (which cannot produce invalid values).
    fn write_raw_trades_file(
        dir: &std::path::Path,
        symbol: &str,
        side: &str,
    ) -> std::path::PathBuf {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(vec![1672304484932u64])),
            Arc::new(StringArray::from(vec![symbol])),
            Arc::new(StringArray::from(vec![side])),
            Arc::new(Float64Array::from(vec![23536.30f64])),
            Arc::new(Float64Array::from(vec![0.001f64])),
            Arc::new(StringArray::from(vec!["bybit"])),
            Arc::new(StringArray::from(vec!["trade_1"])),
        ];
        let batch = RecordBatch::try_from_iter(vec![
            ("trade_ts", columns[0].clone()),
            ("symbol", columns[1].clone()),
            ("side", columns[2].clone()),
            ("price", columns[3].clone()),
            ("amount", columns[4].clone()),
            ("exchange", columns[5].clone()),
            ("id", columns[6].clone()),
        ])
        .expect("valid record batch");

        let path = dir.join("corrupt_trades.parquet");
        let file = File::create(&path).expect("create parquet file");
        let mut writer =
            ArrowWriter::try_new(file, batch.schema(), None).expect("create writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");
        path
    }

    #[test]
    fn unknown_side_is_a_typed_error_not_a_fabricated_buy() {
        let dir = tempdir().expect("tempdir");
        let path = write_raw_trades_file(dir.path(), "BTC/USDT", "hold");

        let err =
            read_trades_parquet(&path).expect_err("unknown side must not be coerced");
        match err {
            PersistError::Parse(msg) => {
                assert!(msg.contains("unknown trade side"), "message: {msg}");
                assert!(msg.contains("hold"), "message names the value: {msg}");
            }
            other => panic!("expected PersistError::Parse, got {other:?}"),
        }
    }

    #[test]
    fn malformed_symbol_is_a_typed_error_not_a_panic() {
        let dir = tempdir().expect("tempdir");
        let path = write_raw_trades_file(dir.path(), "", "buy");

        let err =
            read_trades_parquet(&path).expect_err("malformed symbol must not panic");
        match err {
            PersistError::Parse(msg) => {
                assert!(msg.contains("malformed symbol"), "message: {msg}");
            }
            other => panic!("expected PersistError::Parse, got {other:?}"),
        }
    }

    #[test]
    fn valid_rows_still_read_back() {
        let dir = tempdir().expect("tempdir");
        let path = write_raw_trades_file(dir.path(), "BTC/USDT", "sell");

        let trades = read_trades_parquet(&path).expect("valid file reads");
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].side, aetelier_types::TradeSide::Sell);
    }
}
