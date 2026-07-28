#[cfg(test)]
// -- ----------------------------------------------------------------- TESTS UTILS -- //
// -- ----------------------------------------------------------------- ----------- -- //
mod test_orderbook_utils {

    use aetelier_types::{
        levels::Level,
        orderbooks::{Orderbook, f64_to_decimal},
        orders::OrderSide,
        trading_pair::TradingPair,
    };

    // ------------------------------------------------------------- TEST ORDERBOOK -- //

    /// Deterministic, hand-built order book with known top-of-book values.
    ///
    /// Bids (ascending price): 99.00, 99.50, 100.00  -> best bid = 100.00
    /// Asks (ascending price): 101.00, 101.50, 102.00 -> best ask = 101.00
    /// mid = (100.00 + 101.00) / 2 = 100.50
    /// spread = 101.00 - 100.00 = 1.00
    pub fn test_orderbook() -> Orderbook {
        let bids = vec![
            Level::new(
                1,
                OrderSide::Bids,
                f64_to_decimal(99.00),
                f64_to_decimal(2.0),
                vec![],
            ),
            Level::new(
                2,
                OrderSide::Bids,
                f64_to_decimal(99.50),
                f64_to_decimal(3.0),
                vec![],
            ),
            Level::new(
                3,
                OrderSide::Bids,
                f64_to_decimal(100.00),
                f64_to_decimal(4.0),
                vec![],
            ),
        ];

        let asks = vec![
            Level::new(
                4,
                OrderSide::Asks,
                f64_to_decimal(101.00),
                f64_to_decimal(5.0),
                vec![],
            ),
            Level::new(
                5,
                OrderSide::Asks,
                f64_to_decimal(101.50),
                f64_to_decimal(6.0),
                vec![],
            ),
            Level::new(
                6,
                OrderSide::Asks,
                f64_to_decimal(102.00),
                f64_to_decimal(7.0),
                vec![],
            ),
        ];

        Orderbook::from_levels(
            1234,
            1_000_000,
            TradingPair::new("BASE", "QUOTE"),
            String::from("EXCHANGE"),
            bids,
            asks,
        )
    }
}

mod tests {

    // ----------------------------------------------------------------- FIND_LEVEL -- //
    // ----------------------------------------------------------------- ---------- -- //

    // --------------------------------------------------- FIND_LEVEL: OUTPUT VALUE -- //

    #[test]
    fn find_level_output_value() {
        use crate::test_orderbook_utils::test_orderbook;
        use aetelier_types::{orderbooks::f64_to_decimal, orders::OrderSide};

        let testable_ob = test_orderbook();

        // A known bid level exists at 99.50.
        let bid_price = f64_to_decimal(99.50);
        match testable_ob.find_level(&bid_price) {
            Ok((side, level)) => {
                assert_eq!(side, OrderSide::Bids);
                assert_eq!(level.price, bid_price);
                assert_eq!(level.level_id, 2);
            }
            Err(e) => panic!("find_level should have found the bid level: {e}"),
        }

        // A known ask level exists at 101.50.
        let ask_price = f64_to_decimal(101.50);
        match testable_ob.find_level(&ask_price) {
            Ok((side, level)) => {
                assert_eq!(side, OrderSide::Asks);
                assert_eq!(level.price, ask_price);
                assert_eq!(level.level_id, 5);
            }
            Err(e) => panic!("find_level should have found the ask level: {e}"),
        }

        // A price that does not exist must return an error.
        let missing_price = f64_to_decimal(500.00);
        assert!(testable_ob.find_level(&missing_price).is_err());
    }

    // -------------------------------------------------------------------- BEST_BID -- //
    // -------------------------------------------------------------------- -------- -- //

    #[test]
    fn best_bid_output_value() {
        use crate::test_orderbook_utils::test_orderbook;
        use aetelier_types::orderbooks::decimal_to_f64;

        let testable_ob = test_orderbook();
        let best_bid = testable_ob.best_bid().expect("best_bid should be Some");
        assert_eq!(decimal_to_f64(best_bid), 100.00);
    }

    // -------------------------------------------------------------------- BEST_ASK -- //
    // -------------------------------------------------------------------- -------- -- //

    #[test]
    fn best_ask_output_value() {
        use crate::test_orderbook_utils::test_orderbook;
        use aetelier_types::orderbooks::decimal_to_f64;

        let testable_ob = test_orderbook();
        let best_ask = testable_ob.best_ask().expect("best_ask should be Some");
        assert_eq!(decimal_to_f64(best_ask), 101.00);
    }

    // ------------------------------------------------------------------- MID_PRICE -- //
    // ------------------------------------------------------------------- --------- -- //

    #[test]
    fn mid_price_output_value() {
        use crate::test_orderbook_utils::test_orderbook;
        use aetelier_types::orderbooks::decimal_to_f64;

        let testable_ob = test_orderbook();
        let mid = testable_ob.mid_price().expect("mid_price should be Some");
        assert_eq!(decimal_to_f64(mid), 100.50);
    }

    // ---------------------------------------------------------------------- SPREAD -- //
    // ---------------------------------------------------------------------- ------ -- //

    #[test]
    fn spread_output_value() {
        use crate::test_orderbook_utils::test_orderbook;
        use aetelier_types::orderbooks::decimal_to_f64;

        let testable_ob = test_orderbook();
        let spread = testable_ob.spread().expect("spread should be Some");
        assert_eq!(decimal_to_f64(spread), 1.00);
    }
}
