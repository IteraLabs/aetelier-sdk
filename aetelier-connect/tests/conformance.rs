//! Data-plane venue conformance harness (consolidated `[[test]]` binary).
//!
//! Each venue has a `VenueConformance` descriptor; `conformance_suite!`
//! instantiates every kind in `kinds::KINDS` as a named `#[test]` for it, so
//! the kinds matrix is the test surface. A new kind ratchets to every venue at
//! once (add it to `KINDS` and to the macro body); a venue missing a required
//! fixture fails its kind.
//!
//! Venue cycle status: all 12 venues wired and fully DONE — every venue's row is
//! green across all applicable kinds, which the `conformance_matrix` meta-test
//! enforces for every `done: true` venue (no Skip, no Fail).

#[path = "conformance/coverage.rs"]
mod coverage;
#[path = "conformance/harness.rs"]
mod harness;
#[path = "conformance/kinds.rs"]
mod kinds;

use harness::{Class, ExpectSource, VenueConformance};

// ── Venue descriptors (fixtures + doc-derived expectations only) ────────────

const BINANCE: VenueConformance = VenueConformance {
    venue: "binance",
    wire_symbol: "BTCUSDT",
    canonical_base: "BTC",
    canonical_quote: "USDT",
    // Binance depth is a REST-seeded incremental stream (docs: fetch the depth
    // snapshot, then apply @depth diffs whose U/u straddle lastUpdateId).
    expect_source: ExpectSource::Rest,
    expect_needs_rest: true,
    ws_fixture: "binance/btcusdt_depth_trade.jsonl",
    rest_fixture: Some("binance/btcusdt_rest_snapshot.json"),
    expect_classes: &[Class::Book, Class::Trade],
    done: true,
};

// OKX and Kraken: ChecksumDelta venues (CRC32 continuity, no REST seed,
// self-seeded from the first snapshot frame). Cycle #2 certified the book +
// checksum surface; the trade channel was later captured live (combined
// book+trade fixtures) and their monotonic trade ids armed, flipping both to
// fully DONE.
const OKX: VenueConformance = VenueConformance {
    venue: "okx",
    wire_symbol: "BTC-USDT",
    canonical_base: "BTC",
    canonical_quote: "USDT",
    expect_source: ExpectSource::WssSelfSeed,
    expect_needs_rest: false,
    ws_fixture: "okx/btcusdt_book_trade.jsonl",
    rest_fixture: None,
    expect_classes: &[Class::Book, Class::Trade],
    done: true,
};

const KRAKEN: VenueConformance = VenueConformance {
    venue: "kraken",
    wire_symbol: "BTC/USD",
    canonical_base: "BTC",
    canonical_quote: "USD",
    expect_source: ExpectSource::None,
    expect_needs_rest: false,
    ws_fixture: "kraken/btcusd_book_trade.jsonl",
    rest_fixture: None,
    expect_classes: &[Class::Book, Class::Trade],
    done: true,
};

// Cycle #3: Coinbase, Bybit, Gate.io — self-seed venues with book + trade
// channels captured live (examples/capture_fixture). First fully-DONE venues
// with real trade coverage since Binance.
const COINBASE: VenueConformance = VenueConformance {
    venue: "coinbase",
    wire_symbol: "BTC-USD",
    canonical_base: "BTC",
    canonical_quote: "USD",
    expect_source: ExpectSource::WssSelfSeed,
    expect_needs_rest: false,
    ws_fixture: "coinbase/btcusd_book_trade.jsonl",
    rest_fixture: None,
    expect_classes: &[Class::Book, Class::Trade],
    done: true,
};

const BYBIT: VenueConformance = VenueConformance {
    venue: "bybit",
    wire_symbol: "BTCUSDT",
    canonical_base: "BTC",
    canonical_quote: "USDT",
    expect_source: ExpectSource::WssSelfSeed,
    expect_needs_rest: false,
    ws_fixture: "bybit/btcusdt_book_trade.jsonl",
    rest_fixture: None,
    expect_classes: &[Class::Book, Class::Trade],
    done: true,
};

const GATEIO: VenueConformance = VenueConformance {
    venue: "gateio",
    wire_symbol: "BTC_USDT",
    canonical_base: "BTC",
    canonical_quote: "USDT",
    expect_source: ExpectSource::None,
    expect_needs_rest: false,
    ws_fixture: "gateio/btcusdt_book_trade.jsonl",
    rest_fixture: None,
    expect_classes: &[Class::Book, Class::Trade],
    done: true,
};

// Cycle #4 (self-seed subset): bitget, poloniex (SeqDelta ExactPrev), upbit
// (FullRefresh), htx (SeqDelta ExactPrev, in-band ReqOnSocket seed — the REQ
// reply is captured in the fixture). All captured live via capture_fixture.
const BITGET: VenueConformance = VenueConformance {
    venue: "bitget",
    wire_symbol: "BTCUSDT",
    canonical_base: "BTC",
    canonical_quote: "USDT",
    expect_source: ExpectSource::WssSelfSeed,
    expect_needs_rest: false,
    ws_fixture: "bitget/btcusdt_book_trade.jsonl",
    rest_fixture: None,
    expect_classes: &[Class::Book, Class::Trade],
    done: true,
};
const POLONIEX: VenueConformance = VenueConformance {
    venue: "poloniex",
    wire_symbol: "BTC_USDT",
    canonical_base: "BTC",
    canonical_quote: "USDT",
    expect_source: ExpectSource::WssSelfSeed,
    expect_needs_rest: false,
    ws_fixture: "poloniex/btcusdt_book_trade.jsonl",
    rest_fixture: None,
    expect_classes: &[Class::Book, Class::Trade],
    done: true,
};
const UPBIT: VenueConformance = VenueConformance {
    venue: "upbit",
    wire_symbol: "KRW-BTC",
    canonical_base: "BTC",
    canonical_quote: "KRW",
    expect_source: ExpectSource::None,
    expect_needs_rest: false,
    ws_fixture: "upbit/krwbtc_book_trade.jsonl",
    rest_fixture: None,
    expect_classes: &[Class::Book, Class::Trade],
    done: true,
};
const HTX: VenueConformance = VenueConformance {
    venue: "htx",
    wire_symbol: "btcusdt",
    canonical_base: "BTC",
    canonical_quote: "USDT",
    expect_source: ExpectSource::ReqOnSocket,
    expect_needs_rest: false,
    ws_fixture: "htx/btcusdt_book_trade.jsonl",
    rest_fixture: None,
    // The in-band REQ reply is captured mid-stream (fired at t=6s), so it aligns
    // with the delta stream and the book reconstructs via the kind's
    // buffer-and-reconcile path.
    expect_classes: &[Class::Book, Class::Trade],
    done: true,
};

// Cycle #4 (REST/L3-seeded): bitso (L3 diff-orders, seeded 0-based from a REST
// L3 snapshot) and kucoin (SeqDelta RangeInclusive, REST level2 seed). Both
// use replay_seed against a committed REST snapshot fixture.
const BITSO: VenueConformance = VenueConformance {
    venue: "bitso",
    // btc_mxn is Bitso's flagship (Mexican) market — it actually trades, where
    // btc_usdt was too quiet to certify the trade channel.
    wire_symbol: "btc_mxn",
    canonical_base: "BTC",
    canonical_quote: "MXN",
    expect_source: ExpectSource::Rest,
    expect_needs_rest: true,
    ws_fixture: "bitso/btcmxn_book_trade.jsonl",
    rest_fixture: Some("bitso/btcmxn_rest_l3.json"),
    expect_classes: &[Class::Book, Class::Trade],
    done: true,
};
// KuCoin: RangeInclusive REST-seed. The seed (level2 snapshot) is captured
// mid-stream so its sequence lands inside the buffered delta window — the
// straddling delta chains from it and reconstruction proceeds.
const KUCOIN: VenueConformance = VenueConformance {
    venue: "kucoin",
    wire_symbol: "BTC-USDT",
    canonical_base: "BTC",
    canonical_quote: "USDT",
    expect_source: ExpectSource::Rest,
    expect_needs_rest: true,
    ws_fixture: "kucoin/btcusdt_book_trade.jsonl",
    rest_fixture: Some("kucoin/btcusdt_rest_l2.json"),
    expect_classes: &[Class::Book, Class::Trade],
    done: true,
};

/// Instantiate every kind in `kinds::KINDS` as a named `#[test]` for one
/// venue. The body lists each kind once; adding a kind means adding a line
/// here and its entry in `KINDS` — it then applies to every venue.
macro_rules! conformance_suite {
    ($mod:ident, $desc:expr) => {
        mod $mod {
            use super::*;

            fn kind(name: &str) -> &'static harness::Kind {
                kinds::KINDS
                    .iter()
                    .find(|k| k.name == name)
                    .expect("kind registered")
            }

            #[test]
            fn decode_surface() {
                harness::assert_kind(&$desc, kind("decode_surface"));
            }
            #[test]
            fn seeding_taxonomy() {
                harness::assert_kind(&$desc, kind("seeding_taxonomy"));
            }
            #[test]
            fn symbol_canonicalization() {
                harness::assert_kind(&$desc, kind("symbol_canonicalization"));
            }
            #[test]
            fn trade_continuity() {
                harness::assert_kind(&$desc, kind("trade_continuity"));
            }
            #[test]
            fn trade_invariants() {
                harness::assert_kind(&$desc, kind("trade_invariants"));
            }
            #[test]
            fn book_delta_application() {
                harness::assert_kind(&$desc, kind("book_delta_application"));
            }
            #[test]
            fn book_runtime_replay() {
                harness::assert_kind(&$desc, kind("book_runtime_replay"));
            }
            #[test]
            fn book_invariants() {
                harness::assert_kind(&$desc, kind("book_invariants"));
            }
            #[test]
            fn gap_detection() {
                harness::assert_kind(&$desc, kind("gap_detection"));
            }
            #[test]
            fn checksum_validation() {
                harness::assert_kind(&$desc, kind("checksum_validation"));
            }
        }
    };
}

conformance_suite!(binance, BINANCE);
conformance_suite!(okx, OKX);
conformance_suite!(kraken, KRAKEN);
conformance_suite!(coinbase, COINBASE);
conformance_suite!(bybit, BYBIT);
conformance_suite!(gateio, GATEIO);
conformance_suite!(bitget, BITGET);
conformance_suite!(poloniex, POLONIEX);
conformance_suite!(upbit, UPBIT);
conformance_suite!(htx, HTX);
conformance_suite!(bitso, BITSO);
conformance_suite!(kucoin, KUCOIN);

// ── Registry of wired venues, for the matrix meta-test ──────────────────────

const WIRED: &[&VenueConformance] = &[
    &BINANCE, &OKX, &KRAKEN, &COINBASE, &BYBIT, &GATEIO, &BITGET, &POLONIEX, &UPBIT,
    &HTX, &BITSO, &KUCOIN,
];

/// Meta-test: emit the kinds x venues coverage matrix and enforce the ratchet
/// invariants — the macro body must instantiate exactly `KINDS`, and every
/// DONE venue's full row must be green (no Skip, no Fail).
#[test]
fn conformance_matrix() {
    let kind_names: Vec<&str> = kinds::KINDS.iter().map(|k| k.name).collect();

    println!("\n=== data-plane conformance matrix ===");
    println!("kinds ({}):", kind_names.len());
    for k in kinds::KINDS {
        println!("  {:<24} -> {}", k.name, k.atlas);
    }
    for desc in WIRED {
        let mut row = Vec::new();
        for k in kinds::KINDS {
            let cell = match (k.run)(desc) {
                harness::Outcome::Pass => "PASS",
                harness::Outcome::NotApplicable(_) => "n/a",
                harness::Outcome::Skip(_) => "skip",
                harness::Outcome::Fail(_) => "FAIL",
            };
            row.push(format!("{}={}", k.name, cell));
        }
        println!("{:<10} done={} | {}", desc.venue, desc.done, row.join("  "));

        if desc.done {
            for k in kinds::KINDS {
                match (k.run)(desc) {
                    // Pass and NotApplicable are both green for a DONE venue;
                    // NotApplicable is a by-design non-gap, not a coverage hole.
                    harness::Outcome::Pass | harness::Outcome::NotApplicable(_) => {}
                    other => panic!(
                        "DONE venue {} has non-green kind {}: {:?}",
                        desc.venue, k.name, other
                    ),
                }
            }
        }
    }
    println!("=====================================\n");
}
