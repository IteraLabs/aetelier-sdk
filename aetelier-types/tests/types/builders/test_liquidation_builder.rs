//! Unit tests for `LiquidationBuilder` validation.

#[cfg(test)]
mod tests {
    use aetelier_types::errors::BuildError;
    use aetelier_types::liquidations::Liquidation;
    use aetelier_types::orderbooks::decimal_to_f64;
    use aetelier_types::{TradeSide, TradingPair};

    // ── Happy path ──────────────────────────────────────────────────────

    #[test]
    fn build_happy_path() {
        let liq = Liquidation::builder()
            .ts(1_700_000_000_000)
            .pair(TradingPair::new("BTC", "USDT"))
            .side(TradeSide::Sell)
            .amount(1.5)
            .price(42_000.0)
            .exchange("bybit".into())
            .build()
            .expect("all required fields set");

        assert_eq!(liq.liquidation_ts_us, 1_700_000_000_000);
        assert_eq!(liq.pair, TradingPair::new("BTC", "USDT"));
        assert_eq!(liq.side, TradeSide::Sell);
        assert!((decimal_to_f64(liq.amount) - 1.5).abs() < f64::EPSILON);
        assert!((decimal_to_f64(liq.price) - 42_000.0).abs() < f64::EPSILON);
        assert_eq!(liq.exchange, "bybit");
    }

    // ── Missing-field errors ────────────────────────────────────────────

    #[test]
    fn missing_ts() {
        let err = Liquidation::builder()
            .pair(TradingPair::new("X", "USDT"))
            .side(TradeSide::Buy)
            .amount(1.0)
            .price(1.0)
            .exchange("e".into())
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("ts"));
    }

    #[test]
    fn missing_symbol() {
        let err = Liquidation::builder()
            .ts(1)
            .side(TradeSide::Buy)
            .amount(1.0)
            .price(1.0)
            .exchange("e".into())
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("pair"));
    }

    #[test]
    fn missing_side() {
        let err = Liquidation::builder()
            .ts(1)
            .pair(TradingPair::new("X", "USDT"))
            .amount(1.0)
            .price(1.0)
            .exchange("e".into())
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("side"));
    }

    #[test]
    fn missing_amount() {
        let err = Liquidation::builder()
            .ts(1)
            .pair(TradingPair::new("X", "USDT"))
            .side(TradeSide::Buy)
            .price(1.0)
            .exchange("e".into())
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("amount"));
    }

    #[test]
    fn missing_price() {
        let err = Liquidation::builder()
            .ts(1)
            .pair(TradingPair::new("X", "USDT"))
            .side(TradeSide::Buy)
            .amount(1.0)
            .exchange("e".into())
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("price"));
    }

    #[test]
    fn missing_exchange() {
        let err = Liquidation::builder()
            .ts(1)
            .pair(TradingPair::new("X", "USDT"))
            .side(TradeSide::Buy)
            .amount(1.0)
            .price(1.0)
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("exchange"));
    }

    #[test]
    fn empty_builder() {
        let err = Liquidation::builder().build().unwrap_err();
        assert_eq!(err, BuildError::MissingField("ts"));
    }
}
