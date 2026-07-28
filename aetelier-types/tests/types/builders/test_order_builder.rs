//! Unit tests for `OrderBuilder` validation, defaults, and `OrderId` encode/decode.

#[cfg(test)]
mod tests {
    use aetelier_types::errors::BuildError;
    use aetelier_types::orders::{Order, OrderBuilder, OrderId, OrderSide, OrderType};

    // ── Happy path ──────────────────────────────────────────────────────

    #[test]
    fn build_with_all_fields() {
        let order = Order::builder()
            .side(OrderSide::Bids)
            .order_type(OrderType::Limit)
            .order_ts_us(1_700_000_000)
            .price(42_000.0)
            .amount(0.5)
            .build()
            .expect("all required fields set");

        assert_eq!(order.side, OrderSide::Bids);
        assert_eq!(order.order_type, OrderType::Limit);
        assert_eq!(order.order_ts_us, 1_700_000_000);
        assert_eq!(order.price, Some(42_000.0));
        assert_eq!(order.amount, Some(0.5));
    }

    #[test]
    fn build_minimal_required_fields_only() {
        let order = Order::builder()
            .side(OrderSide::Asks)
            .order_type(OrderType::Market)
            .build()
            .expect("side + order_type is sufficient");

        assert_eq!(order.side, OrderSide::Asks);
        assert_eq!(order.order_type, OrderType::Market);
        // order_ts_us defaults to wall clock — just check it's nonzero
        assert!(order.order_ts_us > 0);
        // price and amount default to None
        assert_eq!(order.price, None);
        assert_eq!(order.amount, None);
    }

    // ── Missing-field errors ────────────────────────────────────────────

    #[test]
    fn build_missing_side() {
        let err = Order::builder()
            .order_type(OrderType::Limit)
            .build()
            .unwrap_err();
        assert_eq!(err, BuildError::MissingField("side"));
    }

    #[test]
    fn build_missing_order_type() {
        let err = Order::builder().side(OrderSide::Bids).build().unwrap_err();
        assert_eq!(err, BuildError::MissingField("order_type"));
    }

    #[test]
    fn build_empty_builder() {
        let err = OrderBuilder::new().build().unwrap_err();
        // First missing field checked is `side`
        assert_eq!(err, BuildError::MissingField("side"));
    }

    // ── OrderId encode/decode roundtrip ─────────────────────────────────

    #[test]
    fn order_id_roundtrip_bid_market() {
        let ts: u64 = 1_700_000_000_123;
        let id = Order::encode_order_id(OrderSide::Bids, OrderType::Market, ts);
        let (side, otype, decoded_ts) = Order::decode_order_id(id);

        assert_eq!(side, OrderSide::Bids);
        assert_eq!(otype, OrderType::Market);
        assert_eq!(decoded_ts, ts);
    }

    #[test]
    fn order_id_roundtrip_ask_limit() {
        let ts: u64 = 999_999_999;
        let id = Order::encode_order_id(OrderSide::Asks, OrderType::Limit, ts);
        let (side, otype, decoded_ts) = Order::decode_order_id(id);

        assert_eq!(side, OrderSide::Asks);
        assert_eq!(otype, OrderType::Limit);
        assert_eq!(decoded_ts, ts);
    }

    #[test]
    fn order_id_struct_accessors() {
        let ts: u64 = 42;
        let oid = OrderId::new(ts, OrderSide::Asks, OrderType::Limit);

        assert_eq!(oid.timestamp(), ts);
        // Note: OrderId bit layout differs from Order::encode — check its own contract
        // bit 63 = side, bit 62 = type (OrderId), vs bit 63 = side, bit 62 = type (encode)
        assert_eq!(oid.side(), OrderSide::Asks);
        assert_eq!(oid.order(), OrderType::Limit);
    }

    #[test]
    fn order_id_zero_timestamp() {
        let id = Order::encode_order_id(OrderSide::Bids, OrderType::Market, 0);
        let (side, otype, ts) = Order::decode_order_id(id);
        assert_eq!(side, OrderSide::Bids);
        assert_eq!(otype, OrderType::Market);
        assert_eq!(ts, 0);
    }

    // ── Builder produces correct order_id ───────────────────────────────

    #[test]
    fn builder_order_id_matches_encode() {
        let order = Order::builder()
            .side(OrderSide::Asks)
            .order_type(OrderType::Limit)
            .order_ts_us(12345678)
            .build()
            .unwrap();

        let expected_id =
            Order::encode_order_id(OrderSide::Asks, OrderType::Limit, 12345678);
        assert_eq!(order.order_id, expected_id);
    }
}
