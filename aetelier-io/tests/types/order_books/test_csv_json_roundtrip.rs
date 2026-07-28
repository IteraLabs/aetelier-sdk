//! Integration tests for CSV and JSON write functions in `aetelier_io`.
//!
//! Uses `OrderbookDelta::from_levels()` to construct test books without
//! needing exchange-specific response types from `aetelier_connect`.

#[cfg(test)]
mod tests {
    use aetelier_io::orderbooks::{write_csv, write_json};
    use aetelier_types::orderbooks::delta::{OrderbookDelta, OrderbookSnapshot};
    use aetelier_types::trading_pair::TradingPair;
    use rust_decimal::Decimal;
    use std::str::FromStr;
    use tempfile::tempdir;

    /// Build a deterministic 3-level orderbook.
    fn make_ob() -> OrderbookDelta {
        let bids = vec![
            (
                Decimal::from_str("100.50").unwrap(),
                Decimal::from_str("1.0").unwrap(),
            ),
            (
                Decimal::from_str("100.00").unwrap(),
                Decimal::from_str("2.5").unwrap(),
            ),
            (
                Decimal::from_str("99.50").unwrap(),
                Decimal::from_str("0.75").unwrap(),
            ),
        ];
        let asks = vec![
            (
                Decimal::from_str("101.00").unwrap(),
                Decimal::from_str("1.5").unwrap(),
            ),
            (
                Decimal::from_str("101.50").unwrap(),
                Decimal::from_str("3.0").unwrap(),
            ),
            (
                Decimal::from_str("102.00").unwrap(),
                Decimal::from_str("0.25").unwrap(),
            ),
        ];
        let pair = TradingPair::new("BTC", "USDT");
        OrderbookDelta::from_levels(pair, "test", bids, asks)
    }

    // ── CSV tests ───────────────────────────────────────────────────────

    #[test]
    fn csv_has_correct_header() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ob.csv");
        write_csv(&make_ob(), &path).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let header = content.lines().next().unwrap();
        assert_eq!(header, "timestamp_us,symbol,side,level,price,size");
    }

    #[test]
    fn csv_bid_rows_ordered_best_first() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ob.csv");
        write_csv(&make_ob(), &path).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let bid_rows: Vec<&str> =
            content.lines().filter(|l| l.contains(",bid,")).collect();
        assert_eq!(bid_rows.len(), 3, "expected 3 bid rows");

        // Level 0 should be the best bid (100.50)
        assert!(bid_rows[0].contains(",bid,0,100.50,"));
        // Level 1 should be 100.00
        assert!(bid_rows[1].contains(",bid,1,100.00,"));
        // Level 2 should be 99.50
        assert!(bid_rows[2].contains(",bid,2,99.50,"));
    }

    #[test]
    fn csv_ask_rows_ordered_best_first() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ob.csv");
        write_csv(&make_ob(), &path).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let ask_rows: Vec<&str> =
            content.lines().filter(|l| l.contains(",ask,")).collect();
        assert_eq!(ask_rows.len(), 3, "expected 3 ask rows");

        // Level 0 = best ask (101.00)
        assert!(ask_rows[0].contains(",ask,0,101.00,"));
        assert!(ask_rows[1].contains(",ask,1,101.50,"));
        assert!(ask_rows[2].contains(",ask,2,102.00,"));
    }

    #[test]
    fn csv_total_row_count() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ob.csv");
        write_csv(&make_ob(), &path).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        // 1 header + 3 bids + 3 asks = 7 lines
        let non_empty_lines: Vec<&str> =
            content.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(non_empty_lines.len(), 7);
    }

    #[test]
    fn csv_symbol_column_matches() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ob.csv");
        write_csv(&make_ob(), &path).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        for line in content.lines().skip(1) {
            if !line.is_empty() {
                assert!(line.contains(",BTC/USDT,"), "symbol missing in: {line}");
            }
        }
    }

    #[test]
    fn csv_empty_book() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.csv");
        let pair = TradingPair::new("EMPTY", "BOOK");
        let ob = OrderbookDelta::from_levels(pair, "test", vec![], vec![]);
        write_csv(&ob, &path).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 1, "only the header line for an empty book");
    }

    // ── JSON tests ──────────────────────────────────────────────────────

    #[test]
    fn json_deserializes_to_snapshot() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ob.json");
        write_json(&make_ob(), &path).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let snap: OrderbookSnapshot =
            serde_json::from_str(&content).expect("valid JSON snapshot");

        assert_eq!(snap.pair, TradingPair::new("BTC", "USDT"));
        assert_eq!(snap.bids.len(), 3);
        assert_eq!(snap.asks.len(), 3);
        assert_eq!(snap.bid_depth, 3);
        assert_eq!(snap.ask_depth, 3);
    }

    #[test]
    fn json_preserves_price_precision() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ob.json");
        write_json(&make_ob(), &path).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let snap: OrderbookSnapshot = serde_json::from_str(&content).unwrap();

        // Best bid is [price, size] = ["100.50", "1.0"]
        assert_eq!(snap.bids[0][0], "100.50");
        assert_eq!(snap.bids[0][1], "1.0");
        // Best ask is ["101.00", "1.5"]
        assert_eq!(snap.asks[0][0], "101.00");
        assert_eq!(snap.asks[0][1], "1.5");
    }

    #[test]
    fn json_has_derived_metrics() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("ob.json");
        write_json(&make_ob(), &path).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let snap: OrderbookSnapshot = serde_json::from_str(&content).unwrap();

        // mid_price = (100.50 + 101.00) / 2 = 100.75
        assert!(snap.mid_price.is_some());
        let mid: f64 = snap.mid_price.unwrap().parse().unwrap();
        assert!((mid - 100.75).abs() < 0.01);

        // spread = 101.00 - 100.50 = 0.50
        assert!(snap.spread.is_some());
        let spread: f64 = snap.spread.unwrap().parse().unwrap();
        assert!((spread - 0.50).abs() < 0.01);

        // volume_imbalance should be set
        assert!(snap.volume_imbalance.is_some());
    }

    #[test]
    fn json_empty_book() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.json");
        let pair = TradingPair::new("EMPTY", "BOOK");
        let ob = OrderbookDelta::from_levels(pair, "test", vec![], vec![]);
        write_json(&ob, &path).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let snap: OrderbookSnapshot = serde_json::from_str(&content).unwrap();
        assert_eq!(snap.pair, TradingPair::new("EMPTY", "BOOK"));
        assert!(snap.bids.is_empty());
        assert!(snap.asks.is_empty());
        assert!(snap.mid_price.is_none());
        assert!(snap.spread.is_none());
    }
}
