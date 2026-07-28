#[cfg(test)]
// -- ----------------------------------------------------------------- TESTS UTILS -- //
// -- ----------------------------------------------------------------- ----------- -- //
mod tests {

    // ------------------------------------------------------ DETERMINISTIC OB -- //

    #[test]
    fn test_basic_orderbook() {
        use aetelier_types::levels::Level;
        use aetelier_types::orderbooks::{Orderbook, decimal_to_f64, f64_to_decimal};
        use aetelier_types::orders::{Order, OrderSide, OrderType};
        use aetelier_types::trading_pair::TradingPair;

        // Build a fixed set of orders for a given level. `n_orders` orders are
        // created with strictly increasing (deterministic) timestamps so the
        // per-level order count is exact and reproducible.
        fn make_orders(
            side: OrderSide,
            price: f64,
            amount: f64,
            n_orders: u64,
        ) -> Vec<Order> {
            (0..n_orders)
                .map(|k| {
                    Order::builder()
                        .side(side)
                        .order_type(OrderType::Limit)
                        .order_ts_us(1_000 + k)
                        .price(price)
                        .amount(amount)
                        .build()
                        .expect("deterministic order should build")
                })
                .collect()
        }

        // Deterministic bid levels (descending prices).
        // best bid = 100_000.00
        let bid_prices = [100_000.00_f64, 99_999.00, 99_998.00];
        let bid_order_counts = [3_u64, 2, 1];
        let bid_amount = 0.5_f64;

        // Deterministic ask levels (ascending prices).
        // best ask = 100_001.00
        let ask_prices = [100_001.00_f64, 100_002.00, 100_003.00];
        let ask_order_counts = [2_u64, 4, 1];
        let ask_amount = 0.25_f64;

        let bids: Vec<Level> = bid_prices
            .iter()
            .zip(bid_order_counts.iter())
            .enumerate()
            .map(|(i, (&price, &n))| {
                let orders = make_orders(OrderSide::Bids, price, bid_amount, n);
                let volume = f64_to_decimal(bid_amount * n as f64);
                Level::new(
                    i as u32,
                    OrderSide::Bids,
                    f64_to_decimal(price),
                    volume,
                    orders,
                )
            })
            .collect();

        let asks: Vec<Level> = ask_prices
            .iter()
            .zip(ask_order_counts.iter())
            .enumerate()
            .map(|(i, (&price, &n))| {
                let orders = make_orders(OrderSide::Asks, price, ask_amount, n);
                let volume = f64_to_decimal(ask_amount * n as f64);
                Level::new(
                    i as u32,
                    OrderSide::Asks,
                    f64_to_decimal(price),
                    volume,
                    orders,
                )
            })
            .collect();

        let ob = Orderbook::from_levels(
            1234,
            1_000_000,
            TradingPair::new("BASE", "QUOTE"),
            String::from("EXCHANGE"),
            bids,
            asks,
        );

        // ------------------------------------------------- Level counts -- //

        let n_bids = ob.bids.len();
        let n_asks = ob.asks.len();
        assert_eq!(n_bids, 3, "expected exactly 3 bid levels");
        assert_eq!(n_asks, 3, "expected exactly 3 ask levels");

        // -------------------------------------------- Best bid / ask -- //

        let best_bid = ob.best_bid().expect("should have bids");
        let best_ask = ob.best_ask().expect("should have asks");

        // Exact best-bid / best-ask values.
        assert_eq!(decimal_to_f64(best_bid), 100_000.00);
        assert_eq!(decimal_to_f64(best_ask), 100_001.00);

        // Crossed-book invariant: best_bid strictly below best_ask.
        assert!(
            best_bid < best_ask,
            "best_bid ({best_bid}) must be < best_ask ({best_ask})"
        );

        // Mid-price and spread are exact given the fixed inputs.
        let mid_price =
            decimal_to_f64((best_bid + best_ask) / rust_decimal::Decimal::TWO);
        assert_eq!(mid_price, 100_000.50);
        assert_eq!(decimal_to_f64(ob.spread().expect("spread")), 1.00);

        // ------------------------------------ Ordering invariants -- //

        // Bids iterated by descending price (BTreeMap is ascending, so rev()).
        let bid_prices_desc: Vec<f64> = ob
            .bids
            .values()
            .rev()
            .map(|l| decimal_to_f64(l.price))
            .collect();
        assert_eq!(bid_prices_desc, vec![100_000.00, 99_999.00, 99_998.00]);
        assert!(
            bid_prices_desc.windows(2).all(|w| w[0] > w[1]),
            "bid prices must be strictly descending: {bid_prices_desc:?}"
        );

        // Asks iterated by ascending price.
        let ask_prices_asc: Vec<f64> =
            ob.asks.values().map(|l| decimal_to_f64(l.price)).collect();
        assert_eq!(ask_prices_asc, vec![100_001.00, 100_002.00, 100_003.00]);
        assert!(
            ask_prices_asc.windows(2).all(|w| w[0] < w[1]),
            "ask prices must be strictly ascending: {ask_prices_asc:?}"
        );

        // ------------------------------------ Per-level order counts -- //

        // Levels by descending price for bids, ascending for asks.
        let bid_levels: Vec<_> = ob.bids.values().rev().collect();
        let ask_levels: Vec<_> = ob.asks.values().collect();

        assert_eq!(bid_levels[0].orders.len(), 3);
        assert_eq!(bid_levels[1].orders.len(), 2);
        assert_eq!(bid_levels[2].orders.len(), 1);
        assert_eq!(ask_levels[0].orders.len(), 2);
        assert_eq!(ask_levels[1].orders.len(), 4);
        assert_eq!(ask_levels[2].orders.len(), 1);

        // ------------------------------------------- Volume totals -- //

        // Total bid volume = 0.5 * (3 + 2 + 1) = 3.0
        let volume_bids: f64 = ob.bids.values().map(|l| decimal_to_f64(l.volume)).sum();
        assert_eq!(volume_bids, 3.0);

        // Total ask volume = 0.25 * (2 + 4 + 1) = 1.75
        let volume_asks: f64 = ob.asks.values().map(|l| decimal_to_f64(l.volume)).sum();
        assert_eq!(volume_asks, 1.75);
    }
}
