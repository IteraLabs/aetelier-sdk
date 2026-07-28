//! Unit tests for `OpenInterestBuilder` validation and defaults.

#[cfg(test)]
mod tests {
    use aetelier_types::TradingPair;
    use aetelier_types::errors::BuildError;
    use aetelier_types::open_interest::OpenInterest;

    // ── Happy path ──────────────────────────────────────────────────────

    #[test]
    fn build_happy_path() {
        let oi = OpenInterest::builder()
            .open_interest_ts_us(1_700_000_000_000)
            .pair(TradingPair::new("BTC", "USDT"))
            .open_interest(50_000.0)
            .open_interest_value(2_100_000_000.0)
            .exchange("bybit".into())
            .build()
            .expect("all fields set");

        assert_eq!(oi.open_interest_ts_us, 1_700_000_000_000);
        assert_eq!(oi.pair, TradingPair::new("BTC", "USDT"));
        assert!((oi.open_interest - 50_000.0).abs() < f64::EPSILON);
        assert!((oi.open_interest_value - 2_100_000_000.0).abs() < f64::EPSILON);
        assert_eq!(oi.exchange, "bybit");
    }

    #[test]
    fn open_interest_value_defaults_to_zero() {
        let oi = OpenInterest::builder()
            .open_interest_ts_us(1)
            .pair(TradingPair::new("X", "USDT"))
            .open_interest(100.0)
            .exchange("e".into())
            .build()
            .unwrap();

        assert!((oi.open_interest_value - 0.0).abs() < f64::EPSILON);
    }

    // ── Missing-field errors ────────────────────────────────────────────

    #[test]
    fn missing_ts() {
        let err = OpenInterest::builder()
            .pair(TradingPair::new("X", "USDT"))
            .open_interest(1.0)
            .exchange("e".into())
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("ts"));
    }

    #[test]
    fn missing_symbol() {
        let err = OpenInterest::builder()
            .open_interest_ts_us(1)
            .open_interest(1.0)
            .exchange("e".into())
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("pair"));
    }

    #[test]
    fn missing_open_interest() {
        let err = OpenInterest::builder()
            .open_interest_ts_us(1)
            .pair(TradingPair::new("X", "USDT"))
            .exchange("e".into())
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("open_interest"));
    }

    #[test]
    fn missing_exchange() {
        let err = OpenInterest::builder()
            .open_interest_ts_us(1)
            .pair(TradingPair::new("X", "USDT"))
            .open_interest(1.0)
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("exchange"));
    }

    #[test]
    fn empty_builder() {
        let err = OpenInterest::builder().build().unwrap_err();
        assert_eq!(err, BuildError::MissingField("ts"));
    }
}
