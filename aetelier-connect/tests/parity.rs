#[path = "parity/harness.rs"]
mod harness;
#[path = "parity/kinds.rs"]
mod kinds;
#[path = "parity/legacy.rs"]
mod legacy;

use harness::{
    DeltaField, LegacyDeltaSource, LevelCompare, Seeding, TradeFanout, VenueParity,
};

const BINANCE: VenueParity = VenueParity {
    venue: "binance",
    canonical_base: "BTC",
    canonical_quote: "USDT",
    wire_symbol: "BTCUSDT",
    ws_fixture: "binance/btcusdt_depth_trade.jsonl",
    rest_fixture: Some("binance/btcusdt_rest_snapshot.json"),
    shim: legacy::binance,
    legacy_delta_source: LegacyDeltaSource::ProductionToNormalized,
    seeding: Seeding::RestFixture,
    legacy_engine_max_depth: None,
    level_compare: LevelCompare::ExactWireToken,
    declared_divergences: &[],
    trade_fanout: TradeFanout::OnePrintPerFrame,
    min_book_frames: 130,
    min_trades: 1369,
    done: true,
};

const BYBIT: VenueParity = VenueParity {
    venue: "bybit",
    canonical_base: "BTC",
    canonical_quote: "USDT",
    wire_symbol: "BTCUSDT",
    ws_fixture: "bybit/btcusdt_book_trade.jsonl",
    rest_fixture: None,
    shim: legacy::bybit,
    legacy_delta_source: LegacyDeltaSource::ProductionToNormalized,
    seeding: Seeding::SelfSeedFirstSnapshot,
    legacy_engine_max_depth: None,
    level_compare: LevelCompare::ExactWireToken,
    declared_divergences: &[DeltaField::Sequence],
    trade_fanout: TradeFanout::BatchedPrints,
    min_book_frames: 734,
    min_trades: 64,
    done: true,
};

const KRAKEN: VenueParity = VenueParity {
    venue: "kraken",
    canonical_base: "BTC",
    canonical_quote: "USD",
    wire_symbol: "BTC/USD",
    ws_fixture: "kraken/btcusd_book_trade.jsonl",
    rest_fixture: None,
    shim: legacy::kraken,
    legacy_delta_source: LegacyDeltaSource::ProductionToNormalized,
    seeding: Seeding::SelfSeedFirstSnapshot,
    legacy_engine_max_depth: Some(10),
    level_compare: LevelCompare::ExactWireToken,
    declared_divergences: &[],
    trade_fanout: TradeFanout::BatchedPrints,
    min_book_frames: 1650,
    min_trades: 46,
    done: true,
};

const COINBASE: VenueParity = VenueParity {
    venue: "coinbase",
    canonical_base: "BTC",
    canonical_quote: "USD",
    wire_symbol: "BTC-USD",
    ws_fixture: "coinbase/btcusd_book_trade.jsonl",
    rest_fixture: None,
    shim: legacy::coinbase,
    legacy_delta_source: LegacyDeltaSource::ProductionToNormalized,
    seeding: Seeding::SelfSeedFirstSnapshot,
    legacy_engine_max_depth: None,
    level_compare: LevelCompare::ExactWireToken,
    declared_divergences: &[],
    trade_fanout: TradeFanout::BatchedPrints,
    min_book_frames: 156,
    min_trades: 43,
    done: true,
};

const OKX: VenueParity = VenueParity {
    venue: "okx",
    canonical_base: "BTC",
    canonical_quote: "USDT",
    wire_symbol: "BTC-USDT",
    ws_fixture: "okx/btcusdt_book_trade.jsonl",
    rest_fixture: None,
    shim: legacy::okx,
    legacy_delta_source: LegacyDeltaSource::NoProductionMapping,
    seeding: Seeding::SelfSeedFirstSnapshot,
    legacy_engine_max_depth: None,
    level_compare: LevelCompare::ExactWireToken,
    declared_divergences: &[],
    trade_fanout: TradeFanout::OnePrintPerFrame,
    min_book_frames: 548,
    min_trades: 48,
    done: true,
};

const GATEIO: VenueParity = VenueParity {
    venue: "gateio",
    canonical_base: "BTC",
    canonical_quote: "USDT",
    wire_symbol: "BTC_USDT",
    ws_fixture: "gateio/btcusdt_book_trade.jsonl",
    rest_fixture: None,
    shim: legacy::gateio,
    legacy_delta_source: LegacyDeltaSource::NoProductionMapping,
    seeding: Seeding::SelfSeedFirstSnapshot,
    legacy_engine_max_depth: None,
    level_compare: LevelCompare::ExactWireToken,
    declared_divergences: &[],
    trade_fanout: TradeFanout::OnePrintPerFrame,
    min_book_frames: 362,
    min_trades: 34,
    done: true,
};

const BITGET: VenueParity = VenueParity {
    venue: "bitget",
    canonical_base: "BTC",
    canonical_quote: "USDT",
    wire_symbol: "BTCUSDT",
    ws_fixture: "bitget/btcusdt_book_trade.jsonl",
    rest_fixture: None,
    shim: legacy::bitget,
    legacy_delta_source: LegacyDeltaSource::NoProductionMapping,
    seeding: Seeding::SelfSeedFirstSnapshot,
    legacy_engine_max_depth: None,
    level_compare: LevelCompare::ExactWireToken,
    declared_divergences: &[],
    trade_fanout: TradeFanout::BatchedPrints,
    min_book_frames: 654,
    min_trades: 42,
    done: true,
};

const POLONIEX: VenueParity = VenueParity {
    venue: "poloniex",
    canonical_base: "BTC",
    canonical_quote: "USDT",
    wire_symbol: "BTC_USDT",
    ws_fixture: "poloniex/btcusdt_book_trade.jsonl",
    rest_fixture: None,
    shim: legacy::poloniex,
    legacy_delta_source: LegacyDeltaSource::NoProductionMapping,
    seeding: Seeding::SelfSeedFirstSnapshot,
    legacy_engine_max_depth: None,
    level_compare: LevelCompare::ExactWireToken,
    declared_divergences: &[],
    trade_fanout: TradeFanout::OnePrintPerFrame,
    min_book_frames: 688,
    min_trades: 10,
    done: true,
};

const UPBIT: VenueParity = VenueParity {
    venue: "upbit",
    canonical_base: "BTC",
    canonical_quote: "KRW",
    wire_symbol: "KRW-BTC",
    ws_fixture: "upbit/krwbtc_book_trade.jsonl",
    rest_fixture: None,
    shim: legacy::upbit,
    legacy_delta_source: LegacyDeltaSource::NoProductionMapping,
    seeding: Seeding::SelfSeedFirstSnapshot,
    legacy_engine_max_depth: None,
    level_compare: LevelCompare::NumericValue,
    declared_divergences: &[],
    trade_fanout: TradeFanout::OnePrintPerFrame,
    min_book_frames: 199,
    min_trades: 46,
    done: true,
};

const HTX: VenueParity = VenueParity {
    venue: "htx",
    canonical_base: "BTC",
    canonical_quote: "USDT",
    wire_symbol: "btcusdt",
    ws_fixture: "htx/btcusdt_book_trade.jsonl",
    rest_fixture: None,
    shim: legacy::htx,
    legacy_delta_source: LegacyDeltaSource::NoProductionMapping,
    seeding: Seeding::SelfSeedFirstSnapshot,
    legacy_engine_max_depth: None,
    level_compare: LevelCompare::NumericValue,
    declared_divergences: &[],
    trade_fanout: TradeFanout::BatchedPrints,
    min_book_frames: 213,
    min_trades: 3,
    done: true,
};

const BITSO: VenueParity = VenueParity {
    venue: "bitso",
    canonical_base: "BTC",
    canonical_quote: "MXN",
    wire_symbol: "btc_mxn",
    ws_fixture: "bitso/btcmxn_book_trade.jsonl",
    rest_fixture: Some("bitso/btcmxn_rest_l3.json"),
    shim: legacy::bitso,
    legacy_delta_source: LegacyDeltaSource::NoProductionMapping,
    seeding: Seeding::SelfSeedFirstSnapshot,
    legacy_engine_max_depth: None,
    level_compare: LevelCompare::ExactWireToken,
    declared_divergences: &[],
    trade_fanout: TradeFanout::OnePrintPerFrame,
    min_book_frames: 899,
    min_trades: 11,
    done: true,
};

const KUCOIN: VenueParity = VenueParity {
    venue: "kucoin",
    canonical_base: "BTC",
    canonical_quote: "USDT",
    wire_symbol: "BTC-USDT",
    ws_fixture: "kucoin/btcusdt_book_trade.jsonl",
    rest_fixture: Some("kucoin/btcusdt_rest_l2.json"),
    shim: legacy::kucoin,
    legacy_delta_source: LegacyDeltaSource::NoProductionMapping,
    seeding: Seeding::SelfSeedFirstSnapshot,
    legacy_engine_max_depth: None,
    level_compare: LevelCompare::ExactWireToken,
    declared_divergences: &[],
    trade_fanout: TradeFanout::OnePrintPerFrame,
    min_book_frames: 690,
    min_trades: 15,
    done: true,
};

macro_rules! parity_suite {
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
            fn trade_wire_parity() {
                harness::assert_kind(&$desc, kind("trade_wire_parity"));
            }
            #[test]
            fn book_wire_parity() {
                harness::assert_kind(&$desc, kind("book_wire_parity"));
            }
            #[test]
            fn legacy_delta_equivalence() {
                harness::assert_kind(&$desc, kind("legacy_delta_equivalence"));
            }
            #[test]
            fn reconstruction_lockstep() {
                harness::assert_kind(&$desc, kind("reconstruction_lockstep"));
            }
        }
    };
}

parity_suite!(binance, BINANCE);
parity_suite!(bybit, BYBIT);
parity_suite!(kraken, KRAKEN);
parity_suite!(coinbase, COINBASE);
parity_suite!(okx, OKX);
parity_suite!(gateio, GATEIO);
parity_suite!(bitget, BITGET);
parity_suite!(poloniex, POLONIEX);
parity_suite!(upbit, UPBIT);
parity_suite!(htx, HTX);
parity_suite!(bitso, BITSO);
parity_suite!(kucoin, KUCOIN);

const WIRED: &[&VenueParity] = &[
    &BINANCE, &BYBIT, &KRAKEN, &COINBASE, &OKX, &GATEIO, &BITGET, &POLONIEX, &UPBIT,
    &HTX, &BITSO, &KUCOIN,
];

#[test]
fn parity_matrix() {
    println!("\n=== legacy-vs-framework parity matrix ===");
    for k in kinds::KINDS {
        println!("  {:<26} -> {}", k.name, k.proves);
    }
    for desc in WIRED {
        let mut row = Vec::new();
        for k in kinds::KINDS {
            let cell = match (k.run)(desc) {
                harness::Outcome::Pass => "PASS",
                harness::Outcome::NotApplicable(_) => "n/a",
                harness::Outcome::Fail(_) => "FAIL",
            };
            row.push(format!("{}={}", k.name, cell));
        }
        println!("{:<10} done={} | {}", desc.venue, desc.done, row.join("  "));

        if desc.done {
            for k in kinds::KINDS {
                match (k.run)(desc) {
                    harness::Outcome::Pass | harness::Outcome::NotApplicable(_) => {}
                    other => panic!(
                        "DONE venue {} has non-green parity kind {}: {:?}",
                        desc.venue, k.name, other
                    ),
                }
            }
        }
    }
    println!("=========================================\n");
}
