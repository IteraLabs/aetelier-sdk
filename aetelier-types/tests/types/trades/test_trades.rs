#[cfg(test)]
// -- ----------------------------------------------------------------- TESTS UTILS -- //
// -- ----------------------------------------------------------------- ----------- -- //
mod test_utils {

    use aetelier_types::trades::Trade;

    pub fn test_random_trade() -> Trade {
        Trade::random()
    }
}

// -- ----------------------------------------------------------------------- TESTS -- //
// -- ----------------------------------------------------------------------- ----- -- //

mod tests {

    use crate::test_utils::test_random_trade;
    use aetelier_types::TradeSide;
    use aetelier_types::orderbooks::decimal_to_f64;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// UTC epoch microseconds for 2020-01-01 — a floor no real timestamp
    /// predates. A value below this means the field is not microseconds
    /// (e.g. seconds or millis), which is what this asserts against.
    const US_FLOOR_2020: u64 = 1_577_836_800_000_000;

    // -------------------------------------------------------------- Trades Values -- //

    /// `Trade::random()` must produce a platform-conformant trade. The load-
    /// bearing check is the timestamp UNIT: `source_trade_ts_us` is UTC epoch
    /// microseconds, so it must exceed the 2020 µs floor and not lead wall-clock
    /// now. (A prior bug stored seconds here; a range-echo of the generator's
    /// own constants passed anyway — this asserts the invariant instead.)
    #[test]
    fn random_trade_is_platform_conformant() {
        let trade = test_random_trade();
        let now_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        assert!(
            trade.source_trade_ts_us >= US_FLOOR_2020,
            "source_trade_ts_us={} is below the 2020 µs floor — not microseconds",
            trade.source_trade_ts_us
        );
        assert!(
            trade.source_trade_ts_us <= now_us,
            "source_trade_ts_us={} leads wall-clock now_us={}",
            trade.source_trade_ts_us,
            now_us
        );

        assert!(matches!(trade.side, TradeSide::Buy | TradeSide::Sell));
        assert!(
            decimal_to_f64(trade.amount) > 0.0,
            "amount must be positive"
        );
        assert!(decimal_to_f64(trade.price) > 0.0, "price must be positive");

        let valid_exchanges = ["bybit", "kraken", "coinbase", "binance"];
        assert!(valid_exchanges.contains(&trade.exchange.as_str()));
    }

    // ------------------------------------------------------- Decimal wire format -- //

    /// `amount`/`price` are `Decimal` in memory but MUST serialize to JSON as
    /// floats (numbers), not strings — the wire format the downstream
    /// telemetry chain depends on is unchanged by the Decimal migration. Guards the
    /// `#[serde(with = "rust_decimal::serde::float")]` attribute against
    /// accidental removal (which would silently switch to string encoding).
    #[test]
    fn trade_serializes_price_amount_as_json_floats() {
        use aetelier_types::trades::Trade;
        use aetelier_types::trading_pair::TradingPair;

        let trade = Trade::builder()
            .source_trade_ts_us(1_700_000_000_000_000)
            .pair(TradingPair::new("BTC", "USDT"))
            .side(TradeSide::Buy)
            .amount(0.5)
            .price(42_000.5)
            .exchange("bybit".into())
            .id("t-001".into())
            .build()
            .expect("all fields set");

        let v: serde_json::Value = serde_json::to_value(&trade).expect("serialize trade");
        assert!(
            v["price"].is_number(),
            "price must be a JSON number, got {}",
            v["price"]
        );
        assert!(
            v["amount"].is_number(),
            "amount must be a JSON number, got {}",
            v["amount"]
        );
        assert_eq!(v["price"].as_f64().unwrap(), 42_000.5);
        assert_eq!(v["amount"].as_f64().unwrap(), 0.5);

        // Round-trips back to an equal Decimal via the same float path.
        let back: Trade = serde_json::from_value(v).expect("deserialize trade");
        assert_eq!(decimal_to_f64(back.price), 42_000.5);
        assert_eq!(decimal_to_f64(back.amount), 0.5);
    }
}

// -- ------------------------------------------------------------------- IDENTITY -- //
// -- ------------------------------------------------------------------- -------- -- //

mod identity_tests {

    use aetelier_types::TradeSide;
    use aetelier_types::trades::{Trade, TradeFingerprint, dedup_by_identity};
    use aetelier_types::trading_pair::TradingPair;

    fn fill(ts_us: u64, id: &str, price: f64, amount: f64) -> Trade {
        Trade::builder()
            .source_trade_ts_us(ts_us)
            .pair(TradingPair::new("BTC", "USD"))
            .side(TradeSide::Buy)
            .amount(amount)
            .price(price)
            .exchange("kraken".into())
            .id(id.into())
            .build()
            .expect("test trade builds")
    }

    #[test]
    fn shared_timestamp_is_not_duplication() {
        let mut trades = vec![
            fill(1_700_000_000_000_000, "a", 100.0, 1.0),
            fill(1_700_000_000_000_000, "b", 100.0, 1.0),
            fill(1_700_000_000_000_000, "c", 100.0, 1.0),
        ];
        let report = dedup_by_identity(&mut trades);
        assert_eq!(report.removed, 0);
        assert_eq!(trades.len(), 3);
    }

    #[test]
    fn repeated_identity_is_removed_first_wins() {
        let mut trades = vec![
            fill(1_700_000_000_000_000, "a", 100.0, 1.0),
            fill(1_700_000_000_000_001, "b", 101.0, 2.0),
            fill(1_700_000_000_000_000, "a", 100.0, 1.0),
        ];
        let report = dedup_by_identity(&mut trades);
        assert_eq!(report.removed, 1);
        assert_eq!(report.conflicts, 0, "a true redelivery repeats verbatim");
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].id, "a");
        assert_eq!(trades[0].source_trade_ts_us, 1_700_000_000_000_000);
    }

    /// The event time is content: a venue that restates when a trade happened
    /// is telling us something, and identity-only dedup would keep whichever
    /// copy landed first and say nothing.
    #[test]
    fn moved_event_time_under_one_identity_is_a_conflict() {
        let mut trades = vec![
            fill(1_700_000_000_000_000, "a", 100.0, 1.0),
            fill(1_700_000_000_000_002, "a", 100.0, 1.0),
        ];
        let report = dedup_by_identity(&mut trades);
        assert_eq!(report.removed, 1);
        assert_eq!(report.conflicts, 1);
    }

    #[test]
    fn identity_ignores_exchange_timestamp() {
        let early = fill(1_700_000_000_000_000, "a", 100.0, 1.0);
        let late = fill(1_700_000_000_999_999, "a", 100.0, 1.0);
        assert_eq!(early.identity(), late.identity());
    }

    #[test]
    fn identity_separates_exchange_and_pair() {
        let a = fill(1_700_000_000_000_000, "1", 100.0, 1.0);
        let mut b = a.clone();
        b.exchange = "okx".into();
        let mut c = a.clone();
        c.pair = TradingPair::new("ETH", "USD");
        assert_ne!(a.identity(), b.identity());
        assert_ne!(a.identity(), c.identity());
    }

    #[test]
    fn restated_content_under_one_identity_is_a_conflict() {
        let mut trades = vec![
            fill(1_700_000_000_000_000, "a", 100.0, 1.0),
            fill(1_700_000_000_000_000, "a", 100.0, 2.0),
        ];
        let report = dedup_by_identity(&mut trades);
        assert_eq!(report.removed, 1);
        assert_eq!(report.conflicts, 1, "amount differs under one id");
    }

    #[test]
    fn fingerprint_is_blind_to_trailing_zeros() {
        let a = fill(1_700_000_000_000_000, "a", 100.0, 1.0);
        let b = fill(1_700_000_000_000_000, "a", 100.00, 1.000);
        assert_eq!(TradeFingerprint::of(&a), TradeFingerprint::of(&b));
    }

    #[test]
    fn fingerprint_tracks_every_content_field() {
        let base = fill(1_700_000_000_000_000, "a", 100.0, 1.0);
        let base_fp = TradeFingerprint::of(&base);

        let mut moved = base.clone();
        moved.source_trade_ts_us += 1;
        let mut repriced = base.clone();
        repriced.price = aetelier_types::orderbooks::f64_to_decimal(101.0);
        let mut resized = base.clone();
        resized.amount = aetelier_types::orderbooks::f64_to_decimal(2.0);
        let mut flipped = base.clone();
        flipped.side = TradeSide::Sell;

        for (other, field) in [
            (moved, "timestamp"),
            (repriced, "price"),
            (resized, "amount"),
            (flipped, "side"),
        ] {
            assert_ne!(
                base_fp,
                TradeFingerprint::of(&other),
                "fingerprint must change with {field}"
            );
        }
    }
}

// -- ---------------------------------------------------------------- AGGREGATE -- //
// -- ---------------------------------------------------------------- --------- -- //

mod aggregate_tests {

    use aetelier_types::TradeSide;
    use aetelier_types::orderbooks::decimal_to_f64;
    use aetelier_types::trades::{
        AGGREGATION_RULE_VERSION, Trade, TradeAggregate, TradeOrderEvidence,
        TradeSplitReason,
    };
    use aetelier_types::trading_pair::TradingPair;

    const BUCKET: u64 = 1_700_000_000_000_000;
    const W: u64 = 1_000;

    fn fill(id: &str, side: TradeSide, price: f64, amount: f64) -> Trade {
        Trade::builder()
            .source_trade_ts_us(BUCKET)
            .pair(TradingPair::new("BTC", "USD"))
            .side(side)
            .amount(amount)
            .price(price)
            .exchange("kraken".into())
            .id(id.into())
            .build()
            .expect("test trade builds")
    }

    fn reduce(fills: &[Trade], reason: TradeSplitReason) -> TradeAggregate {
        let refs: Vec<&Trade> = fills.iter().collect();
        TradeAggregate::from_fills(
            &refs,
            BUCKET,
            W,
            reason,
            TradeOrderEvidence::NumericId,
        )
        .expect("aggregate reduces")
    }

    /// A sweep across three levels: the marks describe the whole footprint.
    #[test]
    fn sweep_reduces_to_one_taker() {
        let fills = vec![
            fill("1", TradeSide::Buy, 100.0, 1.0),
            fill("2", TradeSide::Buy, 101.0, 2.0),
            fill("3", TradeSide::Buy, 103.0, 1.0),
        ];
        let agg = reduce(&fills, TradeSplitReason::Swept);

        assert_eq!(agg.n_fills, 3);
        assert_eq!(decimal_to_f64(agg.qty), 4.0);
        assert_eq!(decimal_to_f64(agg.notional), 100.0 + 202.0 + 103.0);
        assert_eq!(decimal_to_f64(agg.vwap), 405.0 / 4.0);
        assert_eq!(decimal_to_f64(agg.px_first), 100.0);
        assert_eq!(decimal_to_f64(agg.px_last), 103.0);
        assert_eq!(decimal_to_f64(agg.px_min), 100.0);
        assert_eq!(decimal_to_f64(agg.px_max), 103.0);
        assert_eq!(decimal_to_f64(agg.sweep_depth), 3.0);
        assert_eq!(agg.id_first, "1");
        assert_eq!(agg.id_last, "3");
        assert_eq!(agg.rule_version, AGGREGATION_RULE_VERSION);
    }

    /// One level consumed: no depth was eaten, whatever the fill count.
    #[test]
    fn flat_price_group_has_no_sweep_depth() {
        let fills = vec![
            fill("1", TradeSide::Sell, 100.0, 1.0),
            fill("2", TradeSide::Sell, 100.0, 3.0),
        ];
        let agg = reduce(&fills, TradeSplitReason::SamePrice);

        assert_eq!(decimal_to_f64(agg.sweep_depth), 0.0);
        assert_eq!(decimal_to_f64(agg.vwap), 100.0);
        assert_eq!(decimal_to_f64(agg.qty), 4.0);
    }

    /// A sell sweep walks down, so `px_last < px_first` and depth stays
    /// positive.
    #[test]
    fn sell_sweep_depth_is_unsigned() {
        let fills = vec![
            fill("1", TradeSide::Sell, 103.0, 1.0),
            fill("2", TradeSide::Sell, 100.0, 1.0),
        ];
        let agg = reduce(&fills, TradeSplitReason::Swept);

        assert!(decimal_to_f64(agg.px_last) < decimal_to_f64(agg.px_first));
        assert_eq!(decimal_to_f64(agg.sweep_depth), 3.0);
    }

    /// A single fill still reduces, and keeps the venue's own timestamp.
    #[test]
    fn single_fill_is_the_identity_reduction() {
        let fills = vec![fill("1", TradeSide::Buy, 100.0, 2.0)];
        let agg = reduce(&fills, TradeSplitReason::Single);

        assert_eq!(agg.n_fills, 1);
        assert_eq!(agg.ts_us, BUCKET);
        assert_eq!(decimal_to_f64(agg.vwap), 100.0);
        assert_eq!(decimal_to_f64(agg.sweep_depth), 0.0);
        assert_eq!(agg.id_first, agg.id_last);
    }

    /// The vwap of a real sweep is not its midpoint: size decides.
    #[test]
    fn vwap_is_size_weighted_not_price_averaged() {
        let fills = vec![
            fill("1", TradeSide::Buy, 100.0, 9.0),
            fill("2", TradeSide::Buy, 200.0, 1.0),
        ];
        let agg = reduce(&fills, TradeSplitReason::Swept);

        assert_eq!(decimal_to_f64(agg.vwap), 110.0);
        assert!(
            decimal_to_f64(agg.vwap) < 150.0,
            "a mean of prices would be 150"
        );
    }

    #[test]
    fn placement_moves_only_the_event_time() {
        let fills = vec![fill("1", TradeSide::Buy, 100.0, 1.0)];
        let agg = reduce(&fills, TradeSplitReason::Single);
        let placed = agg.clone().place_at(BUCKET + 500);

        assert_eq!(placed.ts_us, BUCKET + 500);
        assert_eq!(placed.bucket_us, agg.bucket_us);
        assert_eq!(placed.w_us, agg.w_us);
        assert_eq!(placed.qty, agg.qty);
    }

    #[test]
    fn mixed_groups_are_rejected() {
        let mixed_side = [
            fill("1", TradeSide::Buy, 100.0, 1.0),
            fill("2", TradeSide::Sell, 100.0, 1.0),
        ];
        let refs: Vec<&Trade> = mixed_side.iter().collect();
        assert!(
            TradeAggregate::from_fills(
                &refs,
                BUCKET,
                W,
                TradeSplitReason::Swept,
                TradeOrderEvidence::NumericId
            )
            .is_err(),
            "two sides are two takers"
        );

        let mut other_venue = fill("2", TradeSide::Buy, 100.0, 1.0);
        other_venue.exchange = "okx".into();
        let cross_venue = [fill("1", TradeSide::Buy, 100.0, 1.0), other_venue];
        let refs: Vec<&Trade> = cross_venue.iter().collect();
        assert!(
            TradeAggregate::from_fills(
                &refs,
                BUCKET,
                W,
                TradeSplitReason::Swept,
                TradeOrderEvidence::NumericId
            )
            .is_err()
        );
    }

    #[test]
    fn degenerate_inputs_are_rejected() {
        assert!(
            TradeAggregate::from_fills(
                &[],
                BUCKET,
                W,
                TradeSplitReason::Single,
                TradeOrderEvidence::NumericId
            )
            .is_err(),
            "an aggregate needs a fill"
        );

        let fills = [fill("1", TradeSide::Buy, 100.0, 1.0)];
        let refs: Vec<&Trade> = fills.iter().collect();
        assert!(
            TradeAggregate::from_fills(
                &refs,
                BUCKET,
                0,
                TradeSplitReason::Single,
                TradeOrderEvidence::NumericId
            )
            .is_err(),
            "a zero-width bucket has no span to place within"
        );
    }

    #[test]
    fn weak_ordering_evidence_is_visible() {
        let fills = vec![fill("1", TradeSide::Buy, 100.0, 1.0)];
        let refs: Vec<&Trade> = fills.iter().collect();
        let asserted = TradeAggregate::from_fills(
            &refs,
            BUCKET,
            W,
            TradeSplitReason::Single,
            TradeOrderEvidence::ArrivalIndex,
        )
        .unwrap();

        assert!(!asserted.is_venue_ordered());
        assert!(reduce(&fills, TradeSplitReason::Single).is_venue_ordered());
    }
}
