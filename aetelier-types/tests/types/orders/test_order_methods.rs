// -- ----------------------------------------------------------------- ORDER TESTS -- //
// -- ----------------------------------------------------------------- ----------- -- //

mod tests {

    // --------------------------------------------------------------------- RANDOM -- //
    // --------------------------------------------------------------------- ------ -- //

    // -------------------------------------------------------- RANDOM: OUTPUT TYPE -- //

    #[test]
    fn order_random_output_type() {
        use aetelier_types::orders::{Order, OrderSide, OrderType};

        // Feed FIXED side/type so the produced order is deterministic
        // (only the internal price/amount draw stays random).
        let i_order: Order = Order::random(
            OrderType::Limit,
            OrderSide::Bids,
            (10_000.00, 11_000.00),
            (0.0, 0.1),
        )
        .unwrap();

        // The order_type must be one of the real OrderType variants,
        // not an unchecked wildcard. This fails if a new variant is
        // ever added without updating construction.
        assert!(
            matches!(i_order.order_type, OrderType::Market | OrderType::Limit),
            "Expected a valid OrderType variant, got {:?}",
            i_order.order_type
        );

        // random() must preserve the requested type/side exactly.
        assert_eq!(
            i_order.order_type,
            OrderType::Limit,
            "random() did not preserve the requested order type"
        );
        assert_eq!(
            i_order.side,
            OrderSide::Bids,
            "random() did not preserve the requested order side"
        );
    }

    // ------------------------------------------------------------------- ORDER_ID -- //
    // --------------------------------------------------------------------- ------ -- //

    // ----------------------------------------------------- ORDER_ID: OUTPUT VALUE -- //

    #[test]
    fn order_id_output_value() {
        use aetelier_types::orders::{Order, OrderSide, OrderType};

        // FIXED, deterministic inputs so the encode/decode roundtrip is
        // fully reproducible (no wall-clock, no randomness).
        let fixed_side = OrderSide::Asks;
        let fixed_type = OrderType::Limit;
        // 1.7e15 µs fits inside the 60-bit timestamp field (< 2^60), so
        // it survives the mask untruncated.
        let fixed_ts: u64 = 1_700_000_000_000_000;

        let order_id = Order::encode_order_id(fixed_side, fixed_type, fixed_ts);

        // Exact packed value a human can verify:
        //   side bit 63 (Asks=1): 0x8000_0000_0000_0000
        //   type bit 62 (Limit=1): 0x4000_0000_0000_0000
        //   ts << 2: 1_700_000_000_000_000 * 4 = 6_800_000_000_000_000
        // Bits do not overlap, so the total is their sum.
        assert_eq!(
            order_id, 13_841_858_055_282_163_712,
            "encode_order_id produced an unexpected packed value"
        );

        let (decoded_side, decoded_type, decoded_ts) = Order::decode_order_id(order_id);

        assert_eq!(
            decoded_side, fixed_side,
            "decoded side does not match the encoded side"
        );
        assert_eq!(
            decoded_type, fixed_type,
            "decoded type does not match the encoded type"
        );
        assert_eq!(
            decoded_ts, fixed_ts,
            "decoded timestamp does not match the encoded timestamp"
        );
    }
}
