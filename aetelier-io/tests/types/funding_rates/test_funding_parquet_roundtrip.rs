#[cfg(test)]
#[cfg(feature = "parquet")]
mod tests {
    use aetelier_io::funding::funding_parquet::{
        read_funding_parquet, write_funding_parquet, write_funding_parquet_timestamped,
    };
    use aetelier_types::funding::FundingRate;
    use aetelier_types::trading_pair::TradingPair;
    use rust_decimal::Decimal;
    use tempfile::tempdir;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn make_funding_rates(exchange: &str) -> Vec<FundingRate> {
        let pair = TradingPair::new("BTC", "USDT");
        vec![
            FundingRate {
                funding_rate_ts_us: 1_672_304_484_000_000,
                local_funding_ts_us: 1_672_304_484_015_000,
                recv_seq: 10,
                conn_epoch_us: 1,
                pair: pair.clone(),
                funding_rate: d("0.0001"),
                premium: Some(d("0.00002")),
                interval_hours: 8,
                next_funding_ts_us: 1_672_308_000_000_000,
                exchange: exchange.to_string(),
            },
            FundingRate {
                funding_rate_ts_us: 0,
                local_funding_ts_us: 1_672_308_000_030_000,
                recv_seq: 11,
                conn_epoch_us: 2,
                pair: pair.clone(),
                funding_rate: d("-0.00015"),
                premium: None,
                interval_hours: 1,
                next_funding_ts_us: 0,
                exchange: exchange.to_string(),
            },
        ]
    }

    #[test]
    fn roundtrip_preserves_every_field_exactly() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bybit_funding.parquet");
        let rates = make_funding_rates("bybit");

        write_funding_parquet(&rates, &path).unwrap();
        let loaded = read_funding_parquet(&path).unwrap();

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].funding_rate_ts_us, 1_672_304_484_000_000);
        assert_eq!(loaded[0].local_funding_ts_us, 1_672_304_484_015_000);
        assert_eq!(loaded[0].recv_seq, 10);
        assert_eq!(loaded[0].conn_epoch_us, 1);
        assert_eq!(loaded[0].funding_rate, d("0.0001"));
        assert_eq!(loaded[0].premium, Some(d("0.00002")));
        assert_eq!(loaded[0].interval_hours, 8);
        assert_eq!(loaded[0].next_funding_ts_us, 1_672_308_000_000_000);
        assert_eq!(loaded[0].pair, TradingPair::new("BTC", "USDT"));
        assert_eq!(loaded[0].exchange, "bybit");

        assert_eq!(loaded[1].funding_rate_ts_us, 0);
        assert_eq!(loaded[1].effective_ts_us(), 1_672_308_000_030_000);
        assert_eq!(loaded[1].funding_rate, d("-0.00015"));
        assert_eq!(loaded[1].premium, None);
        assert_eq!(loaded[1].interval_hours, 1);
    }

    #[test]
    fn negative_rate_precision_survives() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("neg.parquet");
        let mut rates = make_funding_rates("hyperliquid");
        rates[0].funding_rate = d("-0.0000125");

        write_funding_parquet(&rates, &path).unwrap();
        let loaded = read_funding_parquet(&path).unwrap();
        assert_eq!(loaded[0].funding_rate, d("-0.0000125"));
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
        let path = dir.path().join("legacy_funding.parquet");

        let schema = Schema::new(vec![
            Field::new("funding_rate_ts_us", DataType::UInt64, false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("funding_rate", DataType::Float64, false),
            Field::new("next_funding_ts_us", DataType::UInt64, false),
            Field::new("exchange", DataType::Utf8, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(UInt64Array::from(vec![1_672_304_484_000_000u64])),
                Arc::new(StringArray::from(vec!["BTC/USDT"])),
                Arc::new(Float64Array::from(vec![0.0001])),
                Arc::new(UInt64Array::from(vec![1_672_308_000_000_000u64])),
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

        let loaded = read_funding_parquet(&path).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].funding_rate, d("0.0001"));
        assert_eq!(loaded[0].local_funding_ts_us, 0);
        assert_eq!(loaded[0].recv_seq, 0);
        assert_eq!(loaded[0].conn_epoch_us, 0);
        assert_eq!(loaded[0].premium, None);
        assert_eq!(loaded[0].interval_hours, 8);
    }

    #[test]
    fn timestamps_preserved_in_order() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ts_test.parquet");

        let rates: Vec<FundingRate> = (1u64..=3)
            .map(|i| FundingRate {
                funding_rate_ts_us: i * 1_000_000,
                local_funding_ts_us: i * 1_000_000 + 7,
                recv_seq: i,
                conn_epoch_us: 1,
                pair: TradingPair::new("BTC", "USDT"),
                funding_rate: d("0.0001"),
                premium: None,
                interval_hours: 8,
                next_funding_ts_us: 0,
                exchange: "bybit".to_string(),
            })
            .collect();

        write_funding_parquet(&rates, &path).unwrap();
        let loaded = read_funding_parquet(&path).unwrap();

        assert_eq!(loaded.len(), 3);
        for (i, fr) in loaded.iter().enumerate() {
            let n = i as u64 + 1;
            assert_eq!(fr.funding_rate_ts_us, n * 1_000_000);
            assert_eq!(fr.recv_seq, n);
        }
    }

    #[test]
    fn timestamped_writer_filename_and_mode() {
        let dir = tempdir().unwrap();
        let rates = make_funding_rates("bybit");

        let path = write_funding_parquet_timestamped(&rates, dir.path(), "sync").unwrap();
        let fname = path.file_name().unwrap().to_str().unwrap();

        assert!(
            fname.starts_with("bybit_BTC-USDT_funding_sync_"),
            "expected filename starting with 'bybit_BTC-USDT_funding_sync_', got: {}",
            fname
        );
        assert!(fname.ends_with(".parquet"));

        let loaded = read_funding_parquet(&path).unwrap();
        assert_eq!(loaded.len(), 2);

        let path_raw =
            write_funding_parquet_timestamped(&rates, dir.path(), "raw").unwrap();
        let fname_raw = path_raw.file_name().unwrap().to_str().unwrap();
        assert!(
            fname_raw.starts_with("bybit_BTC-USDT_funding_raw_"),
            "expected filename starting with 'bybit_BTC-USDT_funding_raw_', got: {}",
            fname_raw
        );
    }

    #[test]
    fn test_zero_venue_timestamps_stamp_from_local_receipt_never_epoch() {
        let dir = tempdir().unwrap();
        let mut rates = make_funding_rates("hyperliquid");
        for r in &mut rates {
            r.funding_rate_ts_us = 0;
        }
        let path = write_funding_parquet_timestamped(&rates, dir.path(), "sync").unwrap();
        let fname = path.file_name().unwrap().to_str().unwrap();
        assert!(
            fname.starts_with("hyperliquid_BTC-USDT_funding_sync_20221229"),
            "expected the local-receipt date, got: {fname}"
        );
        assert!(!fname.contains("_19700101"));
    }
}
