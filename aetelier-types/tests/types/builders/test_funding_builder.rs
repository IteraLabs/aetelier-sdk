//! Unit tests for `FundingRateBuilder` validation and defaults.

#[cfg(test)]
mod tests {
    use aetelier_types::TradingPair;
    use aetelier_types::errors::BuildError;
    use aetelier_types::funding::FundingRate;

    // ── Happy path ──────────────────────────────────────────────────────

    #[test]
    fn build_happy_path() {
        let fr = FundingRate::builder()
            .funding_rate_ts_us(1_700_000_000_000)
            .pair(TradingPair::new("BTC", "USDT"))
            .funding_rate(0.0001)
            .exchange("bybit".into())
            .next_funding_ts_us(1_700_028_800_000)
            .build()
            .expect("all fields set");

        assert_eq!(fr.funding_rate_ts_us, 1_700_000_000_000);
        assert_eq!(fr.pair, TradingPair::new("BTC", "USDT"));
        assert!((fr.funding_rate - 0.0001).abs() < f64::EPSILON);
        assert_eq!(fr.next_funding_ts_us, 1_700_028_800_000);
        assert_eq!(fr.exchange, "bybit");
    }

    #[test]
    fn next_funding_ts_defaults_to_zero() {
        let fr = FundingRate::builder()
            .funding_rate_ts_us(1)
            .pair(TradingPair::new("X", "USDT"))
            .funding_rate(0.01)
            .exchange("e".into())
            .build()
            .unwrap();

        assert_eq!(fr.next_funding_ts_us, 0);
    }

    // ── Missing-field errors ────────────────────────────────────────────

    #[test]
    fn missing_ts() {
        let err = FundingRate::builder()
            .pair(TradingPair::new("X", "USDT"))
            .funding_rate(0.01)
            .exchange("e".into())
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("ts"));
    }

    #[test]
    fn missing_symbol() {
        let err = FundingRate::builder()
            .funding_rate_ts_us(1)
            .funding_rate(0.01)
            .exchange("e".into())
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("pair"));
    }

    #[test]
    fn missing_funding_rate() {
        let err = FundingRate::builder()
            .funding_rate_ts_us(1)
            .pair(TradingPair::new("X", "USDT"))
            .exchange("e".into())
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("funding_rate"));
    }

    #[test]
    fn missing_exchange() {
        let err = FundingRate::builder()
            .funding_rate_ts_us(1)
            .pair(TradingPair::new("X", "USDT"))
            .funding_rate(0.01)
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("exchange"));
    }

    #[test]
    fn empty_builder() {
        let err = FundingRate::builder().build().unwrap_err();
        assert_eq!(err, BuildError::MissingField("ts"));
    }
}
