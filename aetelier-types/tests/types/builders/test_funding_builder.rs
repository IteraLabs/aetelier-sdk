#[cfg(test)]
mod tests {
    use aetelier_types::TradingPair;
    use aetelier_types::errors::BuildError;
    use aetelier_types::funding::{FundingRate, FundingSettlement};
    use rust_decimal::Decimal;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    #[test]
    fn build_happy_path() {
        let fr = FundingRate::builder()
            .funding_rate_ts_us(1_700_000_000_000_000)
            .pair(TradingPair::new("BTC", "USDT"))
            .funding_rate(d("0.0001"))
            .interval_hours(8)
            .exchange("bybit".into())
            .next_funding_ts_us(1_700_028_800_000_000)
            .build()
            .expect("all fields set");

        assert_eq!(fr.funding_rate_ts_us, 1_700_000_000_000_000);
        assert_eq!(fr.pair, TradingPair::new("BTC", "USDT"));
        assert_eq!(fr.funding_rate, d("0.0001"));
        assert_eq!(fr.next_funding_ts_us, 1_700_028_800_000_000);
        assert_eq!(fr.exchange, "bybit");
        assert_eq!(fr.interval_hours, 8);
        assert_eq!(fr.premium, None);
        assert_eq!(fr.effective_ts_us(), 1_700_000_000_000_000);
    }

    #[test]
    fn next_funding_ts_defaults_to_zero() {
        let fr = FundingRate::builder()
            .funding_rate_ts_us(1)
            .pair(TradingPair::new("X", "USDT"))
            .funding_rate(d("0.01"))
            .interval_hours(8)
            .exchange("e".into())
            .build()
            .unwrap();

        assert_eq!(fr.next_funding_ts_us, 0);
    }

    #[test]
    fn local_ts_alone_is_sufficient_and_effective() {
        let fr = FundingRate::builder()
            .local_funding_ts_us(1_700_000_000_000_000)
            .recv_seq(42)
            .conn_epoch_us(2)
            .pair(TradingPair::new("BTC", "USDC"))
            .funding_rate(d("0.0000125"))
            .premium(d("0.00001"))
            .interval_hours(1)
            .exchange("hyperliquid".into())
            .build()
            .unwrap();

        assert_eq!(fr.funding_rate_ts_us, 0);
        assert_eq!(fr.effective_ts_us(), 1_700_000_000_000_000);
        assert_eq!(fr.recv_seq, 42);
        assert_eq!(fr.conn_epoch_us, 2);
        assert_eq!(fr.premium, Some(d("0.00001")));
        assert_eq!(fr.interval_hours, 1);
    }

    #[test]
    fn missing_both_timestamps() {
        let err = FundingRate::builder()
            .pair(TradingPair::new("X", "USDT"))
            .funding_rate(d("0.01"))
            .interval_hours(8)
            .exchange("e".into())
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("ts"));
    }

    #[test]
    fn missing_symbol() {
        let err = FundingRate::builder()
            .funding_rate_ts_us(1)
            .funding_rate(d("0.01"))
            .interval_hours(8)
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
            .interval_hours(8)
            .exchange("e".into())
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("funding_rate"));
    }

    #[test]
    fn missing_or_zero_interval_is_rejected() {
        let err = FundingRate::builder()
            .funding_rate_ts_us(1)
            .pair(TradingPair::new("X", "USDT"))
            .funding_rate(d("0.01"))
            .exchange("e".into())
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("interval_hours"));

        let err = FundingRate::builder()
            .funding_rate_ts_us(1)
            .pair(TradingPair::new("X", "USDT"))
            .funding_rate(d("0.01"))
            .interval_hours(0)
            .exchange("e".into())
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("interval_hours"));
    }

    #[test]
    fn missing_exchange() {
        let err = FundingRate::builder()
            .funding_rate_ts_us(1)
            .pair(TradingPair::new("X", "USDT"))
            .funding_rate(d("0.01"))
            .interval_hours(8)
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("exchange"));
    }

    #[test]
    fn empty_builder() {
        let err = FundingRate::builder().build().unwrap_err();
        assert_eq!(err, BuildError::MissingField("ts"));
    }

    #[test]
    fn settlement_happy_path() {
        let fs = FundingSettlement::builder()
            .funding_time_us(1_700_000_000_000_000)
            .local_ts_us(1_700_000_000_100_000)
            .rtt_us(2_000)
            .pair(TradingPair::new("BTC", "USDC"))
            .funding_rate(d("0.0000125"))
            .premium(d("0.00001"))
            .exchange("hyperliquid".into())
            .build()
            .unwrap();

        assert_eq!(fs.funding_time_us, 1_700_000_000_000_000);
        assert_eq!(fs.funding_rate, d("0.0000125"));
        assert_eq!(fs.premium, Some(d("0.00001")));
    }

    #[test]
    fn settlement_requires_venue_time() {
        let err = FundingSettlement::builder()
            .pair(TradingPair::new("BTC", "USDC"))
            .funding_rate(d("0.0000125"))
            .exchange("hyperliquid".into())
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("funding_time_us"));

        let err = FundingSettlement::builder()
            .funding_time_us(0)
            .pair(TradingPair::new("BTC", "USDC"))
            .funding_rate(d("0.0000125"))
            .exchange("hyperliquid".into())
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("funding_time_us"));
    }
}
