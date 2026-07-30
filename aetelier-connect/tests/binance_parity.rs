//! Replay parity: framework reconstruction vs the legacy reconcile, on a real
//! captured Binance BTCUSDT depth+trade frame window.
//!
//! Fixtures (`tests/fixtures/binance_btcusdt_*`) are a real ~150-frame capture:
//! the REST depth snapshot plus the WSS `@depth@100ms` + `@trade` frames around
//! it. The snapshot's `lastUpdateId` falls inside the delta range, so the
//! Binance seed/reconcile path is exercised (pre-snapshot deltas discarded, the
//! rest applied in order).
//!
//! Both paths drive the same `OrderbookDelta` engine:
//! - legacy: discard deltas with `u <= snapshot.lastUpdateId`, apply the rest.
//! - framework: seed `SourcedOrderbook` with the snapshot, then `apply` each
//!   delta (gated on `update_id > last`).
//!
//! The test asserts the two books hold identical levels after every step, and
//! that the framework normalizer reproduces every trade without dropping any.

use aetelier_connect::clients::wss::WssDecoder;
use aetelier_connect::framework::adapters::binance::BinanceNormalizer;
use aetelier_connect::framework::{
    DomainEvent, Normalizer, OrderBookState, ReconstructionModel, RecoveryAction,
    SeqPredicate, SnapshotSource, SourcedOrderbook,
};
use aetelier_connect::sources::binance::decoder::BinanceDecoder;
use aetelier_connect::sources::binance::events::BinanceWssEvent;
use aetelier_connect::sources::binance::responses::orderbooks::{
    BinanceDepthSnapshot, BinanceDepthUpdate,
};
use aetelier_connect::sources::binance::responses::trades::BinanceTradeData;
use aetelier_types::orderbooks::{NormalizedDelta, OrderbookDelta, decimal_to_f64};
use aetelier_types::trades::TradeSide;
use aetelier_types::trading_pair::TradingPair;

const WS_FRAMES: &str = include_str!("../datasets/binance/btcusdt_depth_trade.jsonl");
const REST_SNAPSHOT: &str =
    include_str!("../datasets/binance/btcusdt_rest_snapshot.json");
const SYMBOL: &str = "BTCUSDT";

/// Decode the captured WSS frames into ordered depth updates and trades.
fn decode_frames() -> (Vec<BinanceDepthUpdate>, Vec<BinanceTradeData>) {
    let mut depth = Vec::new();
    let mut trades = Vec::new();
    for line in WS_FRAMES.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match BinanceDecoder::decode(line).expect("decode") {
            Some(BinanceWssEvent::DepthUpdate(u)) => depth.push(u),
            Some(BinanceWssEvent::TradeData(t)) => trades.push(t),
            Some(BinanceWssEvent::DepthSnapshot(_)) | None => {}
        }
    }
    (depth, trades)
}

/// `true` if both books hold identical bid and ask levels (price → size).
fn books_equal(a: &OrderbookDelta, b: &OrderbookDelta) -> bool {
    a.top_bids(usize::MAX) == b.top_bids(usize::MAX)
        && a.top_asks(usize::MAX) == b.top_asks(usize::MAX)
}

#[test]
fn framework_reconstruction_matches_legacy_reconcile_on_real_frames() {
    let snapshot: BinanceDepthSnapshot =
        serde_json::from_str(REST_SNAPSHOT).expect("parse rest snapshot");
    let snap_id = snapshot.last_update_id;
    let snap_delta = snapshot.to_normalized(SYMBOL);

    let (depth, _trades) = decode_frames();

    // Binance reconcile: drop deltas fully older than the snapshot.
    let applied: Vec<NormalizedDelta> = depth
        .iter()
        .filter(|u| u.last_update_id > snap_id)
        .map(|u| u.to_normalized())
        .collect();

    assert!(
        applied.len() >= 50,
        "fixture should contain a real run of applied deltas, got {}",
        applied.len()
    );
    assert!(
        depth.len() > applied.len(),
        "fixture should contain pre-snapshot deltas that get discarded ({} total, {} applied)",
        depth.len(),
        applied.len()
    );

    let pair = TradingPair::new("BTC", "USDT");

    // Legacy path: raw OrderbookDelta seeded by the snapshot.
    let mut legacy = OrderbookDelta::new(pair.clone());
    legacy.process(&snap_delta).expect("legacy seed");

    // Framework path: SourcedOrderbook with the Binance reconstruction model.
    let mut framework = SourcedOrderbook::new(
        pair,
        ReconstructionModel::SeqDelta {
            predicate: SeqPredicate::RangeInclusive,
            source: SnapshotSource::RestSnapshot,
        },
        RecoveryAction::RestSnapshot,
    );
    framework.apply(snap_delta).expect("framework seed");
    assert_eq!(framework.state(), OrderBookState::Synced);

    // Seeds must agree.
    assert!(
        books_equal(&legacy, framework.book()),
        "book diverged at the seed snapshot"
    );

    // Apply every delta to both, comparing after each step.
    for (i, delta) in applied.iter().enumerate() {
        legacy
            .process(delta)
            .unwrap_or_else(|e| panic!("legacy delta {i}: {e:?}"));
        framework
            .apply(delta.clone())
            .unwrap_or_else(|e| panic!("framework delta {i} unexpectedly gapped: {e:?}"));

        assert_eq!(
            framework.state(),
            OrderBookState::Synced,
            "framework not Synced at delta {i}"
        );
        assert!(
            books_equal(&legacy, framework.book()),
            "book diverged after applying delta {i} (update_id={})",
            delta.update_id
        );
        assert_eq!(
            legacy.best_bid(),
            framework.book().best_bid(),
            "best_bid diverged at delta {i}"
        );
        assert_eq!(
            legacy.best_ask(),
            framework.book().best_ask(),
            "best_ask diverged at delta {i}"
        );
    }

    // Both books must hold real, non-trivial depth at the end.
    assert!(legacy.bid_depth() > 0 && legacy.ask_depth() > 0);
}

#[test]
fn framework_normalizer_reproduces_every_trade() {
    let (_depth, trades) = decode_frames();
    assert!(
        trades.len() >= 100,
        "fixture should be trade-rich, got {}",
        trades.len()
    );

    let normalizer = BinanceNormalizer::default();
    let mut normalized = 0usize;

    for raw in &trades {
        let events = normalizer.normalize(BinanceWssEvent::TradeData(raw.clone()));
        // Exactly one DomainEvent::Trade per print — no head-drop, no fan-out.
        assert_eq!(events.len(), 1, "trade {} did not map 1:1", raw.trade_id);
        let DomainEvent::Trade { trade, sequence } = &events[0] else {
            panic!("expected DomainEvent::Trade");
        };
        assert_eq!(trade.id, raw.trade_id.to_string());
        assert_eq!(trade.source_trade_ts_us, raw.trade_time * 1_000);
        assert_eq!(
            decimal_to_f64(trade.price),
            raw.price.parse::<f64>().unwrap()
        );
        assert_eq!(
            decimal_to_f64(trade.amount),
            raw.quantity.parse::<f64>().unwrap()
        );
        let expected_side = if raw.is_buyer_maker {
            TradeSide::Sell
        } else {
            TradeSide::Buy
        };
        assert_eq!(
            trade.side, expected_side,
            "taker side wrong for {}",
            raw.trade_id
        );
        assert_eq!(trade.exchange, "binance");
        assert_eq!(
            *sequence,
            Some(raw.trade_id),
            "trade sequence must carry the venue's monotonic trade_id"
        );
        normalized += 1;
    }

    // Every captured trade is represented (the legacy head-drop bug would lose
    // all but the first print of any batched frame).
    assert_eq!(normalized, trades.len());
}
