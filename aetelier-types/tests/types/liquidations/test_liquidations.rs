#[cfg(test)]
// -- ----------------------------------------------------------------- TESTS UTILS -- //
// -- ----------------------------------------------------------------- ----------- -- //
mod test_utils {

    use aetelier_types::liquidations::Liquidation;
    pub fn test_random_liquidation() -> Liquidation {
        Liquidation::random()
    }
}

// -- ----------------------------------------------------------------------- TESTS -- //
// -- ----------------------------------------------------------------------- ----- -- //

mod tests {

    use crate::test_utils::test_random_liquidation;
    use aetelier_types::TradeSide;
    use aetelier_types::orderbooks::decimal_to_f64;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// UTC epoch microseconds for 2020-01-01 — a floor no real timestamp
    /// predates. A value below this means the field is not microseconds.
    const US_FLOOR_2020: u64 = 1_577_836_800_000_000;

    // --------------------------------------------------------- Liquidation Values -- //

    /// `Liquidation::random()` must produce a platform-conformant event. The
    /// load-bearing check is the timestamp UNIT: `liquidation_ts_us` is UTC
    /// epoch microseconds, so it must exceed the 2020 µs floor and not lead
    /// wall-clock now. (A prior bug stored seconds here.)
    #[test]
    fn random_liquidation_is_platform_conformant() {
        let liquidation = test_random_liquidation();
        let now_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        assert!(
            liquidation.liquidation_ts_us >= US_FLOOR_2020,
            "liquidation_ts_us={} is below the 2020 µs floor — not microseconds",
            liquidation.liquidation_ts_us
        );
        assert!(
            liquidation.liquidation_ts_us <= now_us,
            "liquidation_ts_us={} leads wall-clock now_us={}",
            liquidation.liquidation_ts_us,
            now_us
        );

        assert!(matches!(liquidation.side, TradeSide::Buy | TradeSide::Sell));
        assert!(
            decimal_to_f64(liquidation.amount) > 0.0,
            "amount must be positive"
        );
        assert!(
            decimal_to_f64(liquidation.price) > 0.0,
            "price must be positive"
        );

        let valid_exchanges = ["bybit", "kraken", "coinbase", "binance"];
        assert!(valid_exchanges.contains(&liquidation.exchange.as_str()));
    }
}
