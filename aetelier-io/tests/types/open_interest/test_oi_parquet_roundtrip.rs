#[cfg(test)]
#[cfg(feature = "parquet")]
mod tests {
    use aetelier_io::open_interest::oi_parquet::{
        read_oi_parquet, write_oi_parquet, write_oi_parquet_timestamped,
    };
    use aetelier_types::open_interest::OpenInterest;
    use aetelier_types::trading_pair::TradingPair;
    use rust_decimal::Decimal;
    use tempfile::tempdir;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn make_oi_records(exchange: &str) -> Vec<OpenInterest> {
        let pair = TradingPair::new("BTC", "USDT");
        vec![
            OpenInterest {
                open_interest_ts_us: 1_672_304_484_000_000,
                local_oi_ts_us: 1_672_304_484_010_000,
                recv_seq: 1,
                conn_epoch_us: 1,
                pair: pair.clone(),
                open_interest: d("32000.5"),
                open_interest_value: Some(d("752000000")),
                mark_px: Some(d("23500.25")),
                exchange: exchange.to_string(),
            },
            OpenInterest {
                open_interest_ts_us: 0,
                local_oi_ts_us: 1_672_304_784_020_000,
                recv_seq: 2,
                conn_epoch_us: 1,
                pair: pair.clone(),
                open_interest: d("32100"),
                open_interest_value: None,
                mark_px: None,
                exchange: exchange.to_string(),
            },
        ]
    }

    #[test]
    fn roundtrip_preserves_every_field_exactly() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bybit_oi.parquet");
        let records = make_oi_records("bybit");

        write_oi_parquet(&records, &path).unwrap();
        let loaded = read_oi_parquet(&path).unwrap();

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].open_interest_ts_us, 1_672_304_484_000_000);
        assert_eq!(loaded[0].local_oi_ts_us, 1_672_304_484_010_000);
        assert_eq!(loaded[0].recv_seq, 1);
        assert_eq!(loaded[0].conn_epoch_us, 1);
        assert_eq!(loaded[0].open_interest, d("32000.5"));
        assert_eq!(loaded[0].open_interest_value, Some(d("752000000")));
        assert_eq!(loaded[0].mark_px, Some(d("23500.25")));
        assert_eq!(loaded[0].pair, TradingPair::new("BTC", "USDT"));
        assert_eq!(loaded[0].exchange, "bybit");

        assert_eq!(loaded[1].open_interest_ts_us, 0);
        assert_eq!(loaded[1].effective_ts_us(), 1_672_304_784_020_000);
        assert_eq!(loaded[1].open_interest_value, None);
        assert_eq!(loaded[1].mark_px, None);
    }

    #[test]
    fn multi_exchange_roundtrip() {
        let dir = tempdir().unwrap();
        for exchange in ["bybit", "coinbase", "hyperliquid"] {
            let path = dir.path().join(format!("{}_oi.parquet", exchange));
            let records = make_oi_records(exchange);

            write_oi_parquet(&records, &path).unwrap();
            let loaded = read_oi_parquet(&path).unwrap();

            assert_eq!(loaded.len(), 2);
            assert_eq!(loaded[0].exchange, exchange);
            assert_eq!(loaded[0].open_interest, d("32000.5"));
        }
    }

    #[test]
    fn timestamps_preserved_in_order() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ts_test.parquet");

        let records: Vec<OpenInterest> = (1u64..=3)
            .map(|i| OpenInterest {
                open_interest_ts_us: i * 1_000_000,
                local_oi_ts_us: i * 1_000_000 + 5,
                recv_seq: i,
                conn_epoch_us: 1,
                pair: TradingPair::new("BTC", "USDT"),
                open_interest: d("100") * Decimal::from(i),
                open_interest_value: None,
                mark_px: None,
                exchange: "bybit".to_string(),
            })
            .collect();

        write_oi_parquet(&records, &path).unwrap();
        let loaded = read_oi_parquet(&path).unwrap();

        assert_eq!(loaded.len(), 3);
        for (i, oi) in loaded.iter().enumerate() {
            let n = i as u64 + 1;
            assert_eq!(oi.open_interest_ts_us, n * 1_000_000);
            assert_eq!(oi.recv_seq, n);
        }
    }

    #[test]
    fn legacy_five_column_file_reads_with_defaults() {
        use arrow::{
            array::{Float64Array, StringArray, UInt64Array},
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        use parquet::{
            arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties,
        };
        use std::{fs::File, sync::Arc};

        let dir = tempdir().unwrap();
        let path = dir.path().join("legacy_oi.parquet");

        let schema = Schema::new(vec![
            Field::new("open_interest_ts_us", DataType::UInt64, false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("open_interest", DataType::Float64, false),
            Field::new("open_interest_value", DataType::Float64, false),
            Field::new("exchange", DataType::Utf8, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(UInt64Array::from(vec![1_672_304_484_000_000u64])),
                Arc::new(StringArray::from(vec!["BTC/USDT"])),
                Arc::new(Float64Array::from(vec![32000.5])),
                Arc::new(Float64Array::from(vec![752_000_000.0])),
                Arc::new(StringArray::from(vec!["bybit"])),
            ],
        )
        .unwrap();
        let file = File::create(&path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let loaded = read_oi_parquet(&path).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].open_interest, d("32000.5"));
        assert_eq!(loaded[0].open_interest_value, Some(d("752000000")));
        assert_eq!(loaded[0].local_oi_ts_us, 0);
        assert_eq!(loaded[0].recv_seq, 0);
        assert_eq!(loaded[0].conn_epoch_us, 0);
        assert_eq!(loaded[0].mark_px, None);
    }

    #[test]
    fn timestamped_writer_filename_and_mode() {
        let dir = tempdir().unwrap();
        let records = make_oi_records("bybit");

        let path = write_oi_parquet_timestamped(&records, dir.path(), "sync").unwrap();
        let fname = path.file_name().unwrap().to_str().unwrap();

        assert!(
            fname.starts_with("bybit_BTC-USDT_oi_sync_"),
            "expected filename starting with 'bybit_BTC-USDT_oi_sync_', got: {}",
            fname
        );
        assert!(fname.ends_with(".parquet"));

        let loaded = read_oi_parquet(&path).unwrap();
        assert_eq!(loaded.len(), 2);

        let path_raw = write_oi_parquet_timestamped(&records, dir.path(), "raw").unwrap();
        let fname_raw = path_raw.file_name().unwrap().to_str().unwrap();
        assert!(
            fname_raw.starts_with("bybit_BTC-USDT_oi_raw_"),
            "expected filename starting with 'bybit_BTC-USDT_oi_raw_', got: {}",
            fname_raw
        );
    }
}
