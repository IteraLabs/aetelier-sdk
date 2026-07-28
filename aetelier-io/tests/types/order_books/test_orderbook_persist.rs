//! Integration tests for orderbook persistence (CSV, JSON, format inference).
//!
//! Promoted from an inline module test to a dedicated integration test.
//! The original test lived inside `orderbooks::persist`.  Now it imports types
//! from `aetelier_types` and I/O functions from `aetelier_io`.

#[cfg(test)]
mod tests {
    use aetelier_connect::sources::bybit::responses::orderbooks::BybitOrderbookResponse;
    use aetelier_io::orderbooks::persist::save_orderbook_state;
    use aetelier_types::orderbooks::OrderbookDelta;
    use aetelier_types::orderbooks::persist::{OrderbookSnapshot, OutputFormat};
    use aetelier_types::trading_pair::TradingPair;
    use std::path::Path;
    use tempfile::tempdir;

    fn mock_orderbook() -> OrderbookDelta {
        let pair = TradingPair::new("BTC", "USDT");
        let mut ob = OrderbookDelta::from_levels(pair, "test", vec![], vec![]);
        let snapshot: BybitOrderbookResponse = serde_json::from_str(
            r#"{
            "topic": "orderbook.50.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304484978,
            "data": {
                "s": "BTCUSDT",
                "b": [["100.00", "1.0"], ["99.00", "2.0"]],
                "a": [["101.00", "1.5"], ["102.00", "2.5"]],
                "u": 1000,
                "seq": 5000
            }
        }"#,
        )
        .unwrap();
        ob.process(&snapshot.to_normalized().unwrap()).unwrap();
        ob
    }

    #[test]
    fn test_write_csv() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.csv");
        let ob = mock_orderbook();

        save_orderbook_state(&ob, &path, None).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("timestamp_us,symbol,side,level,price,size"));
        assert!(content.contains("BTC/USDT,bid,0,100"));
        assert!(content.contains("BTC/USDT,ask,0,101"));
    }

    #[test]
    fn test_write_json() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.json");
        let ob = mock_orderbook();

        save_orderbook_state(&ob, &path, None).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let snapshot: OrderbookSnapshot = serde_json::from_str(&content).unwrap();
        assert_eq!(snapshot.pair.to_canonical(), "BTC/USDT");
        assert_eq!(snapshot.bids.len(), 2);
        assert_eq!(snapshot.asks.len(), 2);
    }

    #[test]
    fn test_format_inference() {
        assert_eq!(
            OutputFormat::from_path(Path::new("foo.csv")),
            Some(OutputFormat::Csv)
        );
        assert_eq!(
            OutputFormat::from_path(Path::new("foo.json")),
            Some(OutputFormat::Json)
        );
        assert_eq!(
            OutputFormat::from_path(Path::new("foo.parquet")),
            Some(OutputFormat::Parquet)
        );
        assert_eq!(
            OutputFormat::from_path(Path::new("foo.pq")),
            Some(OutputFormat::Parquet)
        );
        assert_eq!(OutputFormat::from_path(Path::new("foo.txt")), None);
    }
}
