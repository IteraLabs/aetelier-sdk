//! Unit tests for `TradeBuilder` validation.

#[cfg(test)]
mod tests {
    use aetelier_types::errors::BuildError;
    use aetelier_types::orderbooks::decimal_to_f64;
    use aetelier_types::trades::Trade;
    use aetelier_types::{TradeSide, TradingPair};

    fn full_trade() -> Trade {
        Trade::builder()
            .source_trade_ts_us(1_700_000_000_000)
            .pair(TradingPair::new("BTC", "USDT"))
            .side(TradeSide::Buy)
            .amount(0.5)
            .price(42_000.0)
            .exchange("bybit".into())
            .id("t-001".into())
            .build()
            .expect("all fields set")
    }

    // ── Happy path ──────────────────────────────────────────────────────

    #[test]
    fn build_happy_path() {
        let t = full_trade();
        assert_eq!(t.source_trade_ts_us, 1_700_000_000_000);
        assert_eq!(t.pair, TradingPair::new("BTC", "USDT"));
        assert_eq!(t.side, TradeSide::Buy);
        assert!((decimal_to_f64(t.amount) - 0.5).abs() < f64::EPSILON);
        assert!((decimal_to_f64(t.price) - 42_000.0).abs() < f64::EPSILON);
        assert_eq!(t.exchange, "bybit");
        assert_eq!(t.id, "t-001");
    }

    // ── Missing-field errors (one per required field) ───────────────────

    #[test]
    fn missing_trade_ts() {
        let err = Trade::builder()
            .pair(TradingPair::new("X", "USDT"))
            .side(TradeSide::Buy)
            .amount(1.0)
            .price(1.0)
            .exchange("e".into())
            .id("i".into())
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("source_trade_ts_us"));
    }

    #[test]
    fn missing_symbol() {
        let err = Trade::builder()
            .source_trade_ts_us(1)
            .side(TradeSide::Buy)
            .amount(1.0)
            .price(1.0)
            .exchange("e".into())
            .id("i".into())
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("pair"));
    }

    #[test]
    fn missing_side() {
        let err = Trade::builder()
            .source_trade_ts_us(1)
            .pair(TradingPair::new("X", "USDT"))
            .amount(1.0)
            .price(1.0)
            .exchange("e".into())
            .id("i".into())
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("side"));
    }

    #[test]
    fn missing_amount() {
        let err = Trade::builder()
            .source_trade_ts_us(1)
            .pair(TradingPair::new("X", "USDT"))
            .side(TradeSide::Buy)
            .price(1.0)
            .exchange("e".into())
            .id("i".into())
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("amount"));
    }

    #[test]
    fn missing_price() {
        let err = Trade::builder()
            .source_trade_ts_us(1)
            .pair(TradingPair::new("X", "USDT"))
            .side(TradeSide::Buy)
            .amount(1.0)
            .exchange("e".into())
            .id("i".into())
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("price"));
    }

    #[test]
    fn missing_exchange() {
        let err = Trade::builder()
            .source_trade_ts_us(1)
            .pair(TradingPair::new("X", "USDT"))
            .side(TradeSide::Buy)
            .amount(1.0)
            .price(1.0)
            .id("i".into())
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("exchange"));
    }

    #[test]
    fn missing_id() {
        let err = Trade::builder()
            .source_trade_ts_us(1)
            .pair(TradingPair::new("X", "USDT"))
            .side(TradeSide::Buy)
            .amount(1.0)
            .price(1.0)
            .exchange("e".into())
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("id"));
    }

    #[test]
    fn empty_builder() {
        let err = Trade::builder().build().unwrap_err();
        assert_eq!(err, BuildError::MissingField("source_trade_ts_us"));
    }
}
