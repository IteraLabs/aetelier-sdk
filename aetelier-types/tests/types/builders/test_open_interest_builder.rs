#[cfg(test)]
mod tests {
    use aetelier_types::TradingPair;
    use aetelier_types::errors::BuildError;
    use aetelier_types::open_interest::OpenInterest;
    use rust_decimal::Decimal;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    #[test]
    fn build_happy_path() {
        let oi = OpenInterest::builder()
            .open_interest_ts_us(1_700_000_000_000_000)
            .pair(TradingPair::new("BTC", "USDT"))
            .open_interest(d("50000"))
            .open_interest_value(d("2100000000"))
            .exchange("bybit".into())
            .build()
            .expect("all fields set");

        assert_eq!(oi.open_interest_ts_us, 1_700_000_000_000_000);
        assert_eq!(oi.pair, TradingPair::new("BTC", "USDT"));
        assert_eq!(oi.open_interest, d("50000"));
        assert_eq!(oi.open_interest_value, Some(d("2100000000")));
        assert_eq!(oi.exchange, "bybit");
        assert_eq!(oi.effective_ts_us(), 1_700_000_000_000_000);
    }

    #[test]
    fn open_interest_value_defaults_to_absent_not_zero() {
        let oi = OpenInterest::builder()
            .open_interest_ts_us(1)
            .pair(TradingPair::new("X", "USDT"))
            .open_interest(d("100"))
            .exchange("e".into())
            .build()
            .unwrap();

        assert_eq!(oi.open_interest_value, None);
        assert_eq!(oi.mark_px, None);
    }

    #[test]
    fn local_ts_alone_is_sufficient_and_effective() {
        let oi = OpenInterest::builder()
            .local_oi_ts_us(1_700_000_000_000_000)
            .recv_seq(7)
            .conn_epoch(1)
            .pair(TradingPair::new("BTC", "USDC"))
            .open_interest(d("12345.678"))
            .mark_px(d("50000.5"))
            .exchange("hyperliquid".into())
            .build()
            .unwrap();

        assert_eq!(oi.open_interest_ts_us, 0);
        assert_eq!(oi.effective_ts_us(), 1_700_000_000_000_000);
        assert_eq!(oi.recv_seq, 7);
        assert_eq!(oi.conn_epoch, 1);
        assert_eq!(oi.mark_px, Some(d("50000.5")));
        assert_eq!(oi.open_interest_value, None);
    }

    #[test]
    fn missing_both_timestamps() {
        let err = OpenInterest::builder()
            .pair(TradingPair::new("X", "USDT"))
            .open_interest(d("1"))
            .exchange("e".into())
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("ts"));
    }

    #[test]
    fn missing_symbol() {
        let err = OpenInterest::builder()
            .open_interest_ts_us(1)
            .open_interest(d("1"))
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
            .open_interest(d("1"))
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
