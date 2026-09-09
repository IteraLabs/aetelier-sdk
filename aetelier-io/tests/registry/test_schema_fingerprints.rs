//! Registry schema-fingerprint conformance (gticket_0038 D7, Option 2).
//!
//! Each canonical datatype's REAL parquet writer produces a sample file; the
//! footer's parquet schema is canonicalized through the parquet schema
//! printer and sha256-hashed. The six fingerprints must equal the committed
//! artifact `aetelier-io/schemas.fingerprints.json`, which the registry
//! (`aetelier-vault .../txy/registry/registry.json`) mirrors and the infra
//! producer-walk diffs. A writer layout change — including a positional
//! append — changes the fingerprint and fails here until a new schema_id
//! version is minted.

#[cfg(feature = "parquet")]
mod fingerprints {
    use std::collections::BTreeMap;
    use std::path::Path;

    use aetelier_types::TradeSide;
    use aetelier_types::funding::{FundingRate, FundingSettlement};
    use aetelier_types::liquidations::Liquidation;
    use aetelier_types::open_interest::OpenInterest;
    use aetelier_types::orderbooks::delta::NormalizedDelta;
    use aetelier_types::orderbooks::{Orderbook, OrderbookDelta, f64_to_decimal};
    use aetelier_types::trades::Trade;
    use aetelier_types::trading_pair::TradingPair;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use rust_decimal::Decimal;
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn fingerprint_of(path: &Path) -> String {
        let reader =
            SerializedFileReader::new(std::fs::File::open(path).unwrap()).unwrap();
        let schema = reader.metadata().file_metadata().schema();
        let mut canonical = Vec::new();
        parquet::schema::printer::print_schema(&mut canonical, schema);
        format!("{:x}", Sha256::digest(&canonical))
    }

    fn sample_orderbook() -> Orderbook {
        let pair = TradingPair::new("BTC", "USDT");
        let normalized = NormalizedDelta {
            symbol: pair.to_canonical(),
            bids: vec![("21921.73".to_string(), "0.063".to_string())],
            asks: vec![("21922.00".to_string(), "0.500".to_string())],
            update_id: 1000,
            sequence: 5000,
            source_orderbook_ts_us: 0,
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            checksum: None,
            orders: Vec::new(),
            is_snapshot: true,
        };
        let mut delta = OrderbookDelta::new(pair.clone());
        delta.process(&normalized).unwrap();
        Orderbook::from_levels(
            0,
            1_672_304_484_978_000_000,
            pair,
            "kraken".to_string(),
            vec![],
            vec![],
        )
        .capture_levels(&delta)
    }

    fn sample_trade() -> Trade {
        Trade {
            source_trade_ts_us: 1_672_304_484_932_000,
            local_trade_ts_us: 0,
            source_trade_rtt_us: 0,
            pair: TradingPair::new("BTC", "USDT"),
            side: TradeSide::Buy,
            amount: f64_to_decimal(0.001),
            price: f64_to_decimal(23536.30),
            exchange: "kraken".to_string(),
            id: "kraken_fp_1".to_string(),
            origin: Default::default(),
        }
    }

    fn sample_liquidation() -> Liquidation {
        Liquidation {
            liquidation_ts_us: 1_672_304_484_932,
            pair: TradingPair::new("BTC", "USDT"),
            side: TradeSide::Buy,
            amount: f64_to_decimal(0.125),
            price: f64_to_decimal(23500.00),
            exchange: "kraken".to_string(),
        }
    }

    fn sample_funding_rate() -> FundingRate {
        FundingRate {
            funding_rate_ts_us: 1_672_304_484_000_000,
            local_funding_ts_us: 1_672_304_484_015_000,
            recv_seq: 10,
            conn_epoch_us: 1,
            pair: TradingPair::new("BTC", "USDT"),
            funding_rate: d("0.0001"),
            premium: Some(d("0.00002")),
            interval_hours: 8,
            next_funding_ts_us: 1_672_308_000_000_000,
            exchange: "kraken".to_string(),
        }
    }

    fn sample_open_interest() -> OpenInterest {
        OpenInterest {
            open_interest_ts_us: 1_672_304_484_000_000,
            local_oi_ts_us: 1_672_304_484_010_000,
            recv_seq: 1,
            conn_epoch_us: 1,
            pair: TradingPair::new("BTC", "USDT"),
            open_interest: d("32000.5"),
            open_interest_value: Some(d("752000000")),
            mark_px: Some(d("23500.25")),
            exchange: "kraken".to_string(),
        }
    }

    fn sample_settlement() -> FundingSettlement {
        FundingSettlement {
            funding_time_us: 1_672_304_484_000_000,
            local_ts_us: 1_672_304_484_050_000,
            rtt_us: 1_000,
            pair: TradingPair::new("BTC", "USDC"),
            funding_rate: d("0.0000125"),
            premium: None,
            exchange: "hyperliquid".to_string(),
        }
    }

    fn computed_fingerprints() -> BTreeMap<String, String> {
        let dir = tempdir().unwrap();
        let mut out = BTreeMap::new();

        let p = aetelier_io::orderbooks::write_ob_parquet(
            &[sample_orderbook()],
            dir.path(),
            "sync",
        )
        .unwrap();
        out.insert("orderbooks".to_string(), fingerprint_of(&p));

        let p = aetelier_io::trades::write_trades_parquet_timestamped(
            &[sample_trade()],
            dir.path(),
            "sync",
        )
        .unwrap();
        out.insert("trades".to_string(), fingerprint_of(&p));

        let p = aetelier_io::liquidations::write_liquidations_parquet_timestamped(
            &[sample_liquidation()],
            dir.path(),
            "sync",
        )
        .unwrap();
        out.insert("liquidations".to_string(), fingerprint_of(&p));

        let p = aetelier_io::funding::write_funding_parquet_timestamped(
            &[sample_funding_rate()],
            dir.path(),
            "sync",
        )
        .unwrap();
        out.insert("funding_rates".to_string(), fingerprint_of(&p));

        let p = aetelier_io::open_interest::write_oi_parquet_timestamped(
            &[sample_open_interest()],
            dir.path(),
            "sync",
        )
        .unwrap();
        out.insert("open_interests".to_string(), fingerprint_of(&p));

        let p = aetelier_io::funding::write_funding_settlement_parquet_timestamped(
            &[sample_settlement()],
            dir.path(),
            "sync",
        )
        .unwrap();
        out.insert("funding_settlements".to_string(), fingerprint_of(&p));

        out
    }

    #[test]
    fn writer_schemas_match_the_committed_fingerprints() {
        let computed = computed_fingerprints();
        let artifact: serde_json::Value =
            serde_json::from_str(include_str!("../../schemas.fingerprints.json"))
                .unwrap();
        let committed: BTreeMap<String, String> =
            serde_json::from_value(artifact["fingerprints"].clone()).unwrap();
        assert_eq!(
            committed,
            computed,
            "writer parquet schemas drifted from schemas.fingerprints.json; if the layout \
             change is intended, mint the next schema_id version in the registry and update \
             the artifact to:\n{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "canonicalization": "parquet_schema_printer.sha256",
                "fingerprints": computed
            }))
            .unwrap()
        );
    }
}
