//! Tests for Binance orderbook WebSocket response parsing, NormalizedDelta
//! conversion, and OrderbookDelta integration.
//!
//! Parsing / normalization tests are driven by REAL frames captured from the
//! live stream (`datasets/binance/`). The apply-algorithm and edge-case tests
//! keep hand-authored input on purpose: they assert exact resulting bid/ask
//! counts, which require controlled level sets a live market can't guarantee.

#[cfg(test)]
mod tests {
    use aetelier_connect::sources::binance::responses::orderbooks::{
        BinanceDepthSnapshot, BinanceDepthUpdate,
    };
    use aetelier_types::orderbooks::OrderbookDelta;
    use aetelier_types::trading_pair::TradingPair;

    const WS_CAPTURE: &str = "binance/btcusdt_depth_trade.jsonl";
    const REST_CAPTURE: &str = "binance/btcusdt_rest_snapshot.json";

    /// First line of a committed real capture that contains every needle.
    fn real_frame(rel: &str, needles: &[&str]) -> String {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("datasets")
            .join(rel);
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {rel}: {e}"));
        text.lines()
            .find(|l| needles.iter().all(|n| l.contains(n)))
            .unwrap_or_else(|| panic!("no frame matching {needles:?} in {rel}"))
            .to_string()
    }

    /// A whole committed real capture file (e.g. a REST snapshot body).
    fn real_file(rel: &str) -> String {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("datasets")
            .join(rel);
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {rel}: {e}"))
    }

    /// Every level is a `[price, qty]` pair of 8-decimal Binance strings.
    fn assert_binance_levels(levels: &[[String; 2]]) {
        assert!(!levels.is_empty(), "level set should be non-empty");
        for lvl in levels {
            let (_, frac) = lvl[0].split_once('.').expect("price has a decimal point");
            assert_eq!(frac.len(), 8, "price {} not 8-decimal", lvl[0]);
            assert!(lvl[1].parse::<f64>().unwrap() >= 0.0, "qty parses");
        }
    }

    // ── Deserialization (real frames) ────────────────────────────────────

    #[test]
    fn parses_a_real_depth_update() {
        let raw = real_frame(WS_CAPTURE, &[r#""e":"depthUpdate""#]);
        let upd: BinanceDepthUpdate =
            serde_json::from_str(&raw).expect("parse real depth update");

        assert_eq!(upd.event_type, "depthUpdate");
        assert_eq!(upd.symbol, "BTCUSDT");
        // Ids pinned to the first depthUpdate in the 2026-07 capture.
        assert_eq!(upd.first_update_id, 96_005_695_588);
        assert_eq!(upd.last_update_id, 96_005_695_616);
        assert!(
            upd.last_update_id >= upd.first_update_id,
            "u must not precede U"
        );
        assert_eq!(upd.bids[0][0], "63108.00000000");
        assert_binance_levels(&upd.bids);
        assert_binance_levels(&upd.asks);
    }

    #[test]
    fn parses_the_real_rest_snapshot() {
        let snap: BinanceDepthSnapshot =
            serde_json::from_str(&real_file(REST_CAPTURE)).expect("parse real snapshot");

        assert_eq!(snap.last_update_id, 96_005_695_793);
        assert_eq!(snap.bids[0][0], "63108.00000000");
        assert_eq!(snap.asks[0][0], "63108.01000000");
        assert_binance_levels(&snap.bids);
        assert_binance_levels(&snap.asks);
    }

    // ── NormalizedDelta conversion (real frames) ─────────────────────────

    #[test]
    fn real_depth_update_to_normalized() {
        let upd: BinanceDepthUpdate =
            serde_json::from_str(&real_frame(WS_CAPTURE, &[r#""e":"depthUpdate""#]))
                .unwrap();
        let norm = upd.to_normalized();

        assert_eq!(norm.symbol, "BTCUSDT");
        assert!(!norm.is_snapshot, "a depth update is not a snapshot");
        // Binance maps update_id=u (last), sequence=U (first) for gap detection.
        assert_eq!(norm.update_id, upd.last_update_id);
        assert_eq!(norm.sequence, upd.first_update_id);
        assert!(!norm.bids.is_empty() && !norm.asks.is_empty());
    }

    #[test]
    fn real_depth_snapshot_to_normalized() {
        let snap: BinanceDepthSnapshot =
            serde_json::from_str(&real_file(REST_CAPTURE)).unwrap();
        let norm = snap.to_normalized("BTCUSDT");

        assert_eq!(norm.symbol, "BTCUSDT");
        assert!(norm.is_snapshot, "a REST snapshot is a snapshot");
        assert_eq!(norm.update_id, snap.last_update_id);
        assert_eq!(norm.sequence, snap.last_update_id);
        assert!(!norm.bids.is_empty() && !norm.asks.is_empty());
    }

    #[test]
    fn real_delta_before_snapshot_is_rejected() {
        // The reject-before-seed invariant holds for any real delta.
        let upd: BinanceDepthUpdate =
            serde_json::from_str(&real_frame(WS_CAPTURE, &[r#""e":"depthUpdate""#]))
                .unwrap();
        let mut ob = OrderbookDelta::new(TradingPair::new("BTC", "USDT"));
        assert!(
            ob.process(&upd.to_normalized()).is_err(),
            "applying a delta before any snapshot must be rejected"
        );
    }

    // ── Apply-algorithm + edge cases (controlled input on purpose) ───────

    #[test]
    fn test_snapshot_then_delta_integration() {
        // Controlled levels so the resulting counts are exact.
        let snap_json = r#"{
            "lastUpdateId": 18521290,
            "bids": [["21921.73","0.063"],["21920.00","3.000"]],
            "asks": [["21922.00","0.500"],["21923.50","2.000"]]
        }"#;
        let snap: BinanceDepthSnapshot = serde_json::from_str(snap_json).unwrap();
        let mut ob = OrderbookDelta::new(TradingPair::new("BTC", "USDT"));
        ob.process(&snap.to_normalized("BTCUSDT")).unwrap();
        assert_eq!(ob.top_bids(10).len(), 2);
        assert_eq!(ob.top_asks(10).len(), 2);

        // Add a new bid, remove an ask via qty="0".
        let delta_json = r#"{
            "e": "depthUpdate", "E": 1672304485100, "s": "BTCUSDT",
            "U": 18521291, "u": 18521292,
            "b": [["21919.00","1.500"]], "a": [["21922.00","0"]]
        }"#;
        let delta: BinanceDepthUpdate = serde_json::from_str(delta_json).unwrap();
        ob.process(&delta.to_normalized()).unwrap();
        assert_eq!(ob.top_bids(10).len(), 3);
        assert_eq!(ob.top_asks(10).len(), 1);
    }

    #[test]
    fn test_zero_quantity_deletes_level() {
        let snap_json = r#"{
            "lastUpdateId": 100,
            "bids": [["50000.00","1.000"]],
            "asks": [["50001.00","2.000"]]
        }"#;
        let snap: BinanceDepthSnapshot = serde_json::from_str(snap_json).unwrap();
        let mut ob = OrderbookDelta::new(TradingPair::new("BTC", "USDT"));
        ob.process(&snap.to_normalized("BTCUSDT")).unwrap();
        assert_eq!(ob.top_bids(10).len(), 1);

        let delta_json = r#"{
            "e": "depthUpdate", "E": 1672304485100, "s": "BTCUSDT",
            "U": 101, "u": 102,
            "b": [["50000.00","0"]], "a": []
        }"#;
        let delta: BinanceDepthUpdate = serde_json::from_str(delta_json).unwrap();
        ob.process(&delta.to_normalized()).unwrap();
        assert_eq!(ob.top_bids(10).len(), 0, "bid removed by qty=0");
        assert_eq!(ob.top_asks(10).len(), 1, "ask still exists");
    }

    #[test]
    fn test_empty_snapshot() {
        let snap: BinanceDepthSnapshot =
            serde_json::from_str(r#"{"lastUpdateId":0,"bids":[],"asks":[]}"#).unwrap();
        let norm = snap.to_normalized("BTCUSDT");
        assert!(norm.is_snapshot);
        assert!(norm.bids.is_empty());
        assert!(norm.asks.is_empty());
    }
}
