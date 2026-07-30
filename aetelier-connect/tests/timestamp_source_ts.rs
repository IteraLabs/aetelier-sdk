//! Per-venue source-timestamp + datatype-independence assertions for the
//! TIMESTAMP-MODEL API.
//!
//! For each venue we drive a real `<Venue>Normalizer.normalize(event)` over a
//! synthetic wire event and prove two things:
//!
//! 1. An **orderbook-only** wire event yields exactly a `DomainEvent::Book`
//!    whose `source_orderbook_ts_us` equals the venue's wire book timestamp
//!    (non-zero) — and emits **no** `Trade`.
//! 2. A **trades-only** wire event yields exactly a `DomainEvent::Trade` whose
//!    `source_trade_ts_us` equals the venue's wire trade time (non-zero) — and
//!    emits **no** `Book`.
//!
//! Together (1) and (2) prove the two datatypes are independent: a book frame
//! never fabricates a trade and a trade frame never fabricates a book, and each
//! carries its own source timestamp drawn from the venue wire payload.
//!
//! The expected source-ts is an **independent, hand-computed** literal epoch
//! microsecond value derived directly from each fixture's raw wire timestamp —
//! never from the venue's own `ts_ms()` / `timestamp_us()` accessor. Deriving
//! the oracle from the accessor under test would be tautological: a bug in the
//! accessor would corrupt both the produced and the expected value in lockstep
//! and the assertion could never fail. The literals below are computed by hand
//! (and cross-checked against the RFC-3339 → epoch-us conversion) so an
//! accessor regression is actually caught.

use aetelier_connect::framework::{DomainEvent, Normalizer};

use aetelier_connect::framework::adapters::coinbase::CoinbaseNormalizer;
use aetelier_connect::framework::adapters::kraken::KrakenNormalizer;
use aetelier_connect::framework::adapters::okx::OkxNormalizer;

use aetelier_connect::sources::coinbase::events::CoinbaseWssEvent;
use aetelier_connect::sources::coinbase::responses::orderbooks::{
    CoinbaseL2Event, CoinbaseL2Update, CoinbaseOrderbookResponse,
};
use aetelier_connect::sources::coinbase::responses::trades::CoinbaseTradeData;
use aetelier_connect::sources::kraken::events::KrakenWssEvent;
use aetelier_connect::sources::kraken::responses::orderbooks::{
    KrakenBookData, KrakenBookResponse, KrakenPriceLevel,
};
use aetelier_connect::sources::kraken::responses::trades::KrakenTradeData;
use aetelier_connect::sources::okx::events::OkxWssEvent;
use aetelier_connect::sources::okx::responses::orderbooks::{
    OkxArg, OkxLevel, OkxOrderbookData, OkxOrderbookResponse,
};
use aetelier_connect::sources::okx::responses::trades::OkxTradeData;

/// Assert a normalizer output is a single `Book` carrying the expected source
/// orderbook ts and *no* trade.
fn assert_book_only(events: &[DomainEvent], expected_source_ts: u64, venue: &str) {
    assert!(
        expected_source_ts != 0,
        "{venue}: book fixture must carry a non-zero wire ts (test setup bug)"
    );
    assert_eq!(
        events.len(),
        1,
        "{venue}: a book-only frame must yield exactly one DomainEvent"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, DomainEvent::Trade { .. })),
        "{venue}: a book-only frame must not emit any Trade (datatype independence)"
    );
    match &events[0] {
        DomainEvent::Book(nd) => {
            assert_eq!(
                nd.source_orderbook_ts_us, expected_source_ts,
                "{venue}: Book.source_orderbook_ts_us must equal the venue wire book ts"
            );
            assert_ne!(
                nd.source_orderbook_ts_us, 0,
                "{venue}: Book.source_orderbook_ts_us must be non-zero"
            );
        }
        other => panic!("{venue}: expected Book, got {other:?}"),
    }
}

/// Assert a normalizer output is a single `Trade` carrying the expected source
/// trade ts and *no* book.
fn assert_trade_only(events: &[DomainEvent], expected_source_ts: u64, venue: &str) {
    assert!(
        expected_source_ts != 0,
        "{venue}: trade fixture must carry a non-zero wire ts (test setup bug)"
    );
    assert_eq!(
        events.len(),
        1,
        "{venue}: a trade-only frame must yield exactly one DomainEvent"
    );
    assert!(
        !events.iter().any(|e| matches!(e, DomainEvent::Book(_))),
        "{venue}: a trade-only frame must not emit any Book (datatype independence)"
    );
    match &events[0] {
        DomainEvent::Trade { trade, .. } => {
            assert_eq!(
                trade.source_trade_ts_us, expected_source_ts,
                "{venue}: Trade.source_trade_ts_us must equal the venue wire trade ts"
            );
            assert_ne!(
                trade.source_trade_ts_us, 0,
                "{venue}: Trade.source_trade_ts_us must be non-zero"
            );
        }
        other => panic!("{venue}: expected Trade, got {other:?}"),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// OKX — `ts` is Unix-ms as a string on both book and trade payloads.
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn okx_book_only_carries_source_orderbook_ts_and_no_trade() {
    let book = OkxOrderbookData {
        asks: vec![OkxLevel(
            "101.0".into(),
            "2.0".into(),
            "0".into(),
            "1".into(),
        )],
        bids: vec![OkxLevel(
            "100.0".into(),
            "1.0".into(),
            "0".into(),
            "1".into(),
        )],
        ts: "1626537446491".into(),
        seq_id: 1235,
        prev_seq_id: Some(1234),
        checksum: Some(123),
    };
    // Independent oracle: wire `ts` is Unix-ms "1626537446491" → epoch-us is
    // that value * 1_000 = 1_626_537_446_491_000 (hand-computed literal).
    let expected: u64 = 1_626_537_446_491_000;
    let resp = OkxOrderbookResponse {
        arg: OkxArg {
            channel: "books".into(),
            inst_id: "BTC-USDT".into(),
        },
        action: Some("update".into()),
        data: vec![book],
    };
    let evs = OkxNormalizer::default().normalize(OkxWssEvent::OrderbookData(resp));
    assert_book_only(&evs, expected, "okx");
}

#[test]
fn okx_trade_only_carries_source_trade_ts_and_no_book() {
    let t = OkxTradeData {
        inst_id: "BTC-USDT".into(),
        trade_id: "216970876".into(),
        px: "31684.5".into(),
        sz: "0.00001186".into(),
        side: "sell".into(),
        ts: "1626531038288".into(),
    };
    // Independent oracle: wire `ts` is Unix-ms "1626531038288" → epoch-us is
    // that value * 1_000 = 1_626_531_038_288_000 (hand-computed literal).
    let expected: u64 = 1_626_531_038_288_000;
    let evs = OkxNormalizer::default().normalize(OkxWssEvent::TradeData(vec![t]));
    assert_trade_only(&evs, expected, "okx");
}

// ─────────────────────────────────────────────────────────────────────────
// Kraken — RFC 3339 `timestamp` on the book payload, RFC 3339 trade `timestamp`.
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn kraken_book_only_carries_source_orderbook_ts_and_no_trade() {
    let resp = KrakenBookResponse {
        channel: "book".into(),
        ty: "snapshot".into(),
        data: vec![KrakenBookData {
            symbol: "BTC/USD".into(),
            bids: vec![KrakenPriceLevel {
                price: "21921.73".into(),
                qty: "0.063".into(),
            }],
            asks: vec![KrakenPriceLevel {
                price: "21922.00".into(),
                qty: "0.500".into(),
            }],
            checksum: 2439117997,
            timestamp: "2023-09-26T16:49:20.962586Z".into(),
        }],
    };
    // Independent oracle: wire `timestamp` "2023-09-26T16:49:20.962586Z".
    // 2023-09-26T16:49:20Z = 1_695_746_960 epoch-s; + 962_586 us fraction ⇒
    // 1_695_746_960 * 1_000_000 + 962_586 = 1_695_746_960_962_586 (hand-computed).
    let expected: u64 = 1_695_746_960_962_586;
    let evs = KrakenNormalizer::default().normalize(KrakenWssEvent::OrderbookData(resp));
    assert_book_only(&evs, expected, "kraken");
}

#[test]
fn kraken_trade_only_carries_source_trade_ts_and_no_book() {
    let t = KrakenTradeData {
        symbol: "BTC/USD".into(),
        side: "sell".into(),
        price: 23536.30,
        qty: 0.001,
        ord_type: "limit".into(),
        trade_id: 12345,
        timestamp: "2023-02-09T20:19:35.396Z".into(),
    };
    // Independent oracle: wire `timestamp` "2023-02-09T20:19:35.396Z".
    // 2023-02-09T20:19:35Z = 1_675_973_975 epoch-s; .396 s = 396_000 us ⇒
    // 1_675_973_975 * 1_000_000 + 396_000 = 1_675_973_975_396_000 (hand-computed).
    let expected: u64 = 1_675_973_975_396_000;
    let evs = KrakenNormalizer::default().normalize(KrakenWssEvent::TradeData(vec![t]));
    assert_trade_only(&evs, expected, "kraken");
}

// ─────────────────────────────────────────────────────────────────────────
// Coinbase — book ts is the envelope `timestamp`; trade ts is the per-trade
// `time`. Both RFC 3339.
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn coinbase_book_only_carries_source_orderbook_ts_and_no_trade() {
    let resp = CoinbaseOrderbookResponse {
        channel: "l2_data".into(),
        timestamp: "2023-02-09T20:32:50.714964855Z".into(),
        sequence_num: 7,
        events: vec![CoinbaseL2Event {
            ty: "snapshot".into(),
            product_id: "BTC-USD".into(),
            updates: vec![
                CoinbaseL2Update {
                    side: "bid".into(),
                    event_time: "2023-02-09T20:32:50.714964855Z".into(),
                    price_level: "21921.73".into(),
                    new_quantity: "0.063".into(),
                },
                CoinbaseL2Update {
                    side: "offer".into(),
                    event_time: "2023-02-09T20:32:50.714964855Z".into(),
                    price_level: "21922.00".into(),
                    new_quantity: "0.500".into(),
                },
            ],
        }],
    };
    // Book source ts is the envelope timestamp, not the per-update event_time.
    // Independent oracle: envelope "2023-02-09T20:32:50.714964855Z".
    // 2023-02-09T20:32:50Z = 1_675_974_770 epoch-s; sub-us digits truncate so
    // .714964855 s → 714_964 us ⇒ 1_675_974_770 * 1_000_000 + 714_964 =
    // 1_675_974_770_714_964 (hand-computed).
    let expected: u64 = 1_675_974_770_714_964;
    let evs =
        CoinbaseNormalizer::default().normalize(CoinbaseWssEvent::OrderbookData(resp));
    assert_book_only(&evs, expected, "coinbase");
}

#[test]
fn coinbase_trade_only_carries_source_trade_ts_and_no_book() {
    let t = CoinbaseTradeData {
        trade_id: "12345".into(),
        product_id: "BTC-USD".into(),
        price: "23536.30".into(),
        size: "0.001".into(),
        side: "SELL".into(),
        time: "2023-02-09T20:19:35.39625135Z".into(),
    };
    // Independent oracle: wire `time` "2023-02-09T20:19:35.39625135Z".
    // 2023-02-09T20:19:35Z = 1_675_973_975 epoch-s; sub-us digits truncate so
    // .39625135 s → 396_251 us ⇒ 1_675_973_975 * 1_000_000 + 396_251 =
    // 1_675_973_975_396_251 (hand-computed).
    let expected: u64 = 1_675_973_975_396_251;
    let evs =
        CoinbaseNormalizer::default().normalize(CoinbaseWssEvent::TradeData(vec![t]));
    assert_trade_only(&evs, expected, "coinbase");
}
