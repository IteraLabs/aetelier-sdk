//! Wire-contract tests: validate that serde serialisation of core SDK types
//! produces the exact JSON forms expected by the webapp and backend, and that
//! round-tripping through JSON is lossless.
//!
//! These tests act as a regression gate — if you change a `#[serde(rename)]`,
//! add a variant, or alter a default, these tests should fail loudly so the
//! mismatch is caught before it reaches the WebSocket wire.

use aetelier_types::exchanges::{Exchange, MarketType};
use aetelier_types::synchronizers::{ClockMode, WorkerMode};

// ─────────────────────────────────────────────────────────────────────────────
// Test 1: Core types produce the exact expected JSON wire forms
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn core_types_json_snapshot() {
    // Exchange variants serialize to lowercase strings.
    assert_eq!(
        serde_json::to_string(&Exchange::Bybit).unwrap(),
        r#""bybit""#
    );
    assert_eq!(
        serde_json::to_string(&Exchange::Coinbase).unwrap(),
        r#""coinbase""#
    );
    assert_eq!(
        serde_json::to_string(&Exchange::Kraken).unwrap(),
        r#""kraken""#
    );
    assert_eq!(
        serde_json::to_string(&Exchange::Binance).unwrap(),
        r#""binance""#
    );
    assert_eq!(serde_json::to_string(&Exchange::Okx).unwrap(), r#""okx""#);
    assert_eq!(
        serde_json::to_string(&Exchange::Gateio).unwrap(),
        r#""gateio""#
    );

    // MarketType variants serialize to lowercase strings.
    assert_eq!(
        serde_json::to_string(&MarketType::Spot).unwrap(),
        r#""spot""#
    );
    assert_eq!(
        serde_json::to_string(&MarketType::Perpetual).unwrap(),
        r#""perpetual""#
    );
    assert_eq!(
        serde_json::to_string(&MarketType::Inverse).unwrap(),
        r#""inverse""#
    );

    // WorkerMode::Raw serializes as a simple string tag.
    assert_eq!(serde_json::to_string(&WorkerMode::Raw).unwrap(), r#""Raw""#);

    // WorkerMode::Clock serializes as a tagged object with clock + period_us.
    let clock_mode = WorkerMode::Clock {
        clock: ClockMode::TradeDriven,
        period_us: 100_000_000,
    };
    let json = serde_json::to_string(&clock_mode).unwrap();
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(v["Clock"]["clock"], "TradeDriven");
    assert_eq!(v["Clock"]["period_us"], 100_000_000u64);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2: MarketType::Inverse has a distinct wire form (regression for the
// webapp bug where Inverse silently mapped to Spot)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn market_type_inverse_has_distinct_wire_form() {
    let spot = serde_json::to_string(&MarketType::Spot).unwrap();
    let perp = serde_json::to_string(&MarketType::Perpetual).unwrap();
    let inv = serde_json::to_string(&MarketType::Inverse).unwrap();

    // All three must be distinct on the wire.
    assert_ne!(spot, perp);
    assert_ne!(spot, inv);
    assert_ne!(perp, inv);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3: All WorkerMode and ClockMode variants round-trip through JSON
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn worker_mode_all_variants_roundtrip() {
    let clock_modes = [
        ClockMode::OrderbookDriven,
        ClockMode::TradeDriven,
        ClockMode::LiquidationDriven,
        ClockMode::ExternalClock,
    ];

    // ClockMode round-trips.
    for mode in &clock_modes {
        let json = serde_json::to_string(mode).unwrap();
        let back: ClockMode = serde_json::from_str(&json).unwrap();
        assert_eq!(*mode, back, "ClockMode round-trip failed for {:?}", mode);
    }

    // WorkerMode::Raw round-trips.
    let raw_json = serde_json::to_string(&WorkerMode::Raw).unwrap();
    let raw_back: WorkerMode = serde_json::from_str(&raw_json).unwrap();
    assert_eq!(WorkerMode::Raw, raw_back);

    // WorkerMode::Clock round-trips for every clock variant.
    for clock in &clock_modes {
        let wm = WorkerMode::Clock {
            clock: *clock,
            period_us: 500_000_000,
        };
        let json = serde_json::to_string(&wm).unwrap();
        let back: WorkerMode = serde_json::from_str(&json).unwrap();
        assert_eq!(
            wm, back,
            "WorkerMode::Clock round-trip failed for {:?}",
            clock
        );
    }

    // Exchange round-trips.
    for ex in [
        Exchange::Bybit,
        Exchange::Coinbase,
        Exchange::Kraken,
        Exchange::Binance,
        Exchange::Okx,
        Exchange::Gateio,
    ] {
        let json = serde_json::to_string(&ex).unwrap();
        let back: Exchange = serde_json::from_str(&json).unwrap();
        assert_eq!(ex, back, "Exchange round-trip failed for {:?}", ex);
    }

    // MarketType round-trips.
    for mt in [MarketType::Spot, MarketType::Perpetual, MarketType::Inverse] {
        let json = serde_json::to_string(&mt).unwrap();
        let back: MarketType = serde_json::from_str(&json).unwrap();
        assert_eq!(mt, back, "MarketType round-trip failed for {:?}", mt);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4: Exchange::from_str_loose returns None for unknown exchanges
// (regression for the webapp bug that silently fell back to Binance)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn exchange_from_str_loose_known_and_unknown() {
    assert_eq!(Exchange::from_str_loose("bybit"), Some(Exchange::Bybit));
    assert_eq!(
        Exchange::from_str_loose("COINBASE"),
        Some(Exchange::Coinbase)
    );
    assert_eq!(Exchange::from_str_loose("Kraken"), Some(Exchange::Kraken));
    assert_eq!(Exchange::from_str_loose("binance"), Some(Exchange::Binance));
    assert_eq!(Exchange::from_str_loose("okx"), Some(Exchange::Okx));
    assert_eq!(Exchange::from_str_loose("OKEX"), Some(Exchange::Okx));
    assert_eq!(Exchange::from_str_loose("gateio"), Some(Exchange::Gateio));
    assert_eq!(Exchange::from_str_loose("gate"), Some(Exchange::Gateio));

    // Unknown exchanges must return None, not a silent fallback.
    assert_eq!(Exchange::from_str_loose(""), None);
    assert_eq!(Exchange::from_str_loose("deribit"), None);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 5: MarketType::from_str_loose returns None for unknown types
// (regression for the webapp bug where .contains("perpetual") mapped
// Inverse to Spot)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn market_type_from_str_loose_known_and_unknown() {
    assert_eq!(MarketType::from_str_loose("spot"), Some(MarketType::Spot));
    assert_eq!(
        MarketType::from_str_loose("perpetual"),
        Some(MarketType::Perpetual)
    );
    assert_eq!(
        MarketType::from_str_loose("perp"),
        Some(MarketType::Perpetual)
    );
    assert_eq!(
        MarketType::from_str_loose("linear"),
        Some(MarketType::Perpetual)
    );
    assert_eq!(
        MarketType::from_str_loose("inverse"),
        Some(MarketType::Inverse)
    );
    assert_eq!(
        MarketType::from_str_loose("coin"),
        Some(MarketType::Inverse)
    );

    // Unknown types must return None.
    assert_eq!(MarketType::from_str_loose("futures"), None);
    assert_eq!(MarketType::from_str_loose(""), None);
    assert_eq!(MarketType::from_str_loose("options"), None);
}
