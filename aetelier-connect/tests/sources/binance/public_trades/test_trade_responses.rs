//! Tests for Binance trade WebSocket response parsing, driven by REAL frames
//! captured from the live stream (`datasets/binance/`) rather than hand-authored
//! JSON — so the parser is exercised against bytes Binance actually sent.

#[cfg(test)]
mod tests {
    use aetelier_connect::sources::binance::responses::trades::BinanceTradeData;

    const CAPTURE: &str = "binance/btcusdt_depth_trade.jsonl";

    /// The first line of a committed real capture that contains every needle.
    /// Panics (rather than silently skipping) if the capture lacks such a frame,
    /// so a recapture that drops a message shape fails loudly.
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

    #[test]
    fn parses_a_real_trade_frame() {
        let raw = real_frame(CAPTURE, &[r#""e":"trade""#]);
        let trade: BinanceTradeData =
            serde_json::from_str(&raw).expect("parse real binance trade");

        // Values pinned to the first trade in the 2026-07 BTCUSDT capture.
        assert_eq!(trade.event_type, "trade");
        assert_eq!(trade.symbol, "BTCUSDT");
        assert_eq!(trade.event_time, 1_781_816_957_585);
        assert_eq!(trade.trade_id, 6_422_336_709);
        assert_eq!(trade.price, "63108.00000000");
        assert_eq!(trade.quantity, "0.02792000");
        assert_eq!(trade.trade_time, 1_781_816_957_585);
        assert!(trade.is_buyer_maker);
    }

    #[test]
    fn taker_side_from_a_real_maker_trade() {
        // A real frame where the buyer was the maker → the taker sold.
        let raw = real_frame(CAPTURE, &[r#""e":"trade""#, r#""m":true"#]);
        let trade: BinanceTradeData = serde_json::from_str(&raw).unwrap();
        assert!(trade.is_buyer_maker);
        assert_eq!(trade.taker_side(), "sell");
    }

    #[test]
    fn taker_side_from_a_real_taker_trade() {
        // A real frame where the buyer was the taker → the taker bought.
        let raw = real_frame(CAPTURE, &[r#""e":"trade""#, r#""m":false"#]);
        let trade: BinanceTradeData = serde_json::from_str(&raw).unwrap();
        assert!(!trade.is_buyer_maker);
        assert_eq!(trade.taker_side(), "buy");
    }

    #[test]
    fn real_trade_preserves_price_precision() {
        // Binance quotes price/qty as 8-decimal strings; the parser must keep
        // them verbatim (no float coercion that would round or drop zeros).
        let raw = real_frame(CAPTURE, &[r#""e":"trade""#]);
        let trade: BinanceTradeData = serde_json::from_str(&raw).unwrap();
        for field in [&trade.price, &trade.quantity] {
            let (_, frac) = field.split_once('.').expect("decimal point present");
            assert_eq!(frac.len(), 8, "expected 8-decimal string, got {field}");
        }
        // And they round-trip through f64 to finite, non-negative numbers.
        assert!(trade.price.parse::<f64>().unwrap() > 0.0);
        assert!(trade.quantity.parse::<f64>().unwrap() >= 0.0);
    }
}
