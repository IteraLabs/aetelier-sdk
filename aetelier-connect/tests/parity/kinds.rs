use aetelier_connect::framework::model::{OrderBookState, SourcedOrderbook};
use aetelier_connect::framework::registry::registry;
use aetelier_types::orderbooks::{NormalizedDelta, OrderbookDelta, decimal_to_f64};
use aetelier_types::trading_pair::TradingPair;

use super::harness::{
    DeltaField, Kind, LegacyDeltaSource, LegacyInput, Outcome, PairedFrame, Seeding,
    TradeFanout, VenueParity, book_fields_differing, books_equal, delta_fields_differing,
    fixture_raw, framework_books, framework_trades, replay_paired,
};

fn declared(desc: &VenueParity, field: DeltaField) -> bool {
    desc.declared_divergences.contains(&field)
}

fn same_number(wire: f64, framework: f64) -> bool {
    wire == framework
        || (wire - framework).abs() <= 1e-12 * wire.abs().max(framework.abs())
}

fn trade_wire_parity(desc: &VenueParity) -> Outcome {
    let frames = match replay_paired(desc) {
        Ok(f) => f,
        Err(e) => return Outcome::Fail(e),
    };
    let canonical = TradingPair::new(desc.canonical_base, desc.canonical_quote);
    let mut compared = 0usize;
    let mut widest_frame = 0usize;

    for frame in &frames {
        let legacy = &frame.legacy.trades;
        let framework = framework_trades(frame);
        if legacy.len() != framework.len() {
            return Outcome::Fail(format!(
                "frame {}: legacy decoded {} trade prints, framework emitted {} — a print was dropped or duplicated",
                frame.index,
                legacy.len(),
                framework.len()
            ));
        }
        widest_frame = widest_frame.max(legacy.len());
        for (position, (want, (got, sequence))) in
            legacy.iter().zip(framework).enumerate()
        {
            let at = format!("frame {} print {position}", frame.index);
            if got.id != want.id {
                return Outcome::Fail(format!(
                    "{at}: id {} on the wire, {} from the framework",
                    want.id, got.id
                ));
            }
            if !same_number(want.price, decimal_to_f64(got.price)) {
                return Outcome::Fail(format!(
                    "{at}: price {} on the wire, {} from the framework",
                    want.price,
                    decimal_to_f64(got.price)
                ));
            }
            if !same_number(want.amount, decimal_to_f64(got.amount)) {
                return Outcome::Fail(format!(
                    "{at}: amount {} on the wire, {} from the framework",
                    want.amount,
                    decimal_to_f64(got.amount)
                ));
            }
            if got.side != want.side {
                return Outcome::Fail(format!(
                    "{at}: taker side {:?} on the wire, {:?} from the framework",
                    want.side, got.side
                ));
            }
            if got.source_trade_ts_us != want.source_trade_ts_us {
                return Outcome::Fail(format!(
                    "{at}: venue timestamp {}us on the wire, {}us from the framework",
                    want.source_trade_ts_us, got.source_trade_ts_us
                ));
            }
            if sequence != want.sequence {
                return Outcome::Fail(format!(
                    "{at}: trade sequence {:?} on the wire, {:?} from the framework",
                    want.sequence, sequence
                ));
            }
            if got.exchange != desc.venue {
                return Outcome::Fail(format!(
                    "{at}: exchange tagged {}, expected {}",
                    got.exchange, desc.venue
                ));
            }
            if got.pair.to_canonical() != canonical.to_canonical() {
                return Outcome::Fail(format!(
                    "{at}: pair decoded to {}, expected {}",
                    got.pair.to_canonical(),
                    canonical.to_canonical()
                ));
            }
            compared += 1;
        }
    }

    if compared < desc.min_trades {
        return Outcome::Fail(format!(
            "fixture yielded {compared} trade prints, below the declared floor of {} — a truncated fixture would pass vacuously",
            desc.min_trades
        ));
    }
    if desc.trade_fanout == TradeFanout::BatchedPrints && widest_frame < 2 {
        return Outcome::Fail(
            "venue declares batched prints but no fixture frame carries more than one — the fan-out path is unexercised".into(),
        );
    }
    Outcome::Pass
}

fn book_wire_parity(desc: &VenueParity) -> Outcome {
    let frames = match replay_paired(desc) {
        Ok(f) => f,
        Err(e) => return Outcome::Fail(e),
    };
    let mut compared = 0usize;
    let mut levels_compared = 0usize;
    let mut orders_compared = 0usize;
    let mut divergences_seen: Vec<DeltaField> = Vec::new();

    for frame in &frames {
        let legacy = &frame.legacy.books;
        let framework = framework_books(frame);
        if legacy.len() != framework.len() {
            return Outcome::Fail(format!(
                "frame {}: legacy decoded {} book elements, framework emitted {} — an element was dropped or duplicated",
                frame.index,
                legacy.len(),
                framework.len()
            ));
        }
        for (position, (want, got)) in legacy.iter().zip(framework).enumerate() {
            let differing = book_fields_differing(want, got, desc.level_compare);
            for field in &differing {
                if !declared(desc, *field) {
                    return Outcome::Fail(format!(
                        "frame {} book {position}: {field:?} differs — wire {want:?}, framework {got:?}",
                        frame.index
                    ));
                }
                if !divergences_seen.contains(field) {
                    divergences_seen.push(*field);
                }
            }
            levels_compared += want.bids.len() + want.asks.len();
            orders_compared += want.orders.len();
            compared += 1;
        }
    }

    if compared < desc.min_book_frames {
        return Outcome::Fail(format!(
            "fixture yielded {compared} book elements, below the declared floor of {}",
            desc.min_book_frames
        ));
    }
    if levels_compared == 0 && orders_compared == 0 {
        return Outcome::Fail(
            "no price levels and no L3 orders were compared — the projection is empty"
                .into(),
        );
    }
    for field in desc.declared_divergences {
        if !divergences_seen.contains(field) {
            return Outcome::Fail(format!(
                "{field:?} is declared as a known divergence but the two paths now agree on it — drop the declaration"
            ));
        }
    }
    Outcome::Pass
}

fn legacy_delta_equivalence(desc: &VenueParity) -> Outcome {
    if desc.legacy_delta_source == LegacyDeltaSource::NoProductionMapping {
        return Outcome::NotApplicable(
            "no to_normalized under src/sources — the framework adapter is the only wire-to-delta mapping".into(),
        );
    }
    let frames = match replay_paired(desc) {
        Ok(f) => f,
        Err(e) => return Outcome::Fail(e),
    };
    let mut compared = 0usize;
    let mut divergences_seen: Vec<DeltaField> = Vec::new();

    for frame in &frames {
        let Some(production) = frame.legacy.production_deltas.as_ref() else {
            return Outcome::Fail(format!(
                "frame {}: venue declares a production mapping but the shim returned none",
                frame.index
            ));
        };
        let framework = framework_books(frame);
        if production.len() != framework.len() {
            return Outcome::Fail(format!(
                "frame {}: to_normalized produced {} deltas, framework emitted {}",
                frame.index,
                production.len(),
                framework.len()
            ));
        }
        for (position, (want, got)) in production.iter().zip(framework).enumerate() {
            for field in delta_fields_differing(want, got, desc.level_compare) {
                if !declared(desc, field) {
                    return Outcome::Fail(format!(
                        "frame {} delta {position}: {field:?} differs between to_normalized and the adapter",
                        frame.index
                    ));
                }
                if !divergences_seen.contains(&field) {
                    divergences_seen.push(field);
                }
            }
            compared += 1;
        }
    }

    if compared < desc.min_book_frames {
        return Outcome::Fail(format!(
            "compared {compared} deltas, below the declared floor of {}",
            desc.min_book_frames
        ));
    }
    for field in desc.declared_divergences {
        if !divergences_seen.contains(field) {
            return Outcome::Fail(format!(
                "{field:?} is declared as a known divergence but to_normalized and the adapter now agree — drop the declaration"
            ));
        }
    }
    Outcome::Pass
}

fn paired_delta_streams(
    frames: &[PairedFrame],
) -> Result<Vec<(NormalizedDelta, NormalizedDelta)>, Outcome> {
    let mut paired = Vec::new();
    for frame in frames {
        let Some(production) = frame.legacy.production_deltas.as_ref() else {
            return Err(Outcome::Fail(format!(
                "frame {}: no production deltas",
                frame.index
            )));
        };
        let framework = framework_books(frame);
        if production.len() != framework.len() {
            return Err(Outcome::Fail(format!(
                "frame {}: delta stream lengths diverge",
                frame.index
            )));
        }
        for (legacy, framework) in production.iter().zip(framework) {
            paired.push((legacy.clone(), framework.clone()));
        }
    }
    Ok(paired)
}

fn reconstruction_lockstep(desc: &VenueParity) -> Outcome {
    if desc.legacy_delta_source == LegacyDeltaSource::NoProductionMapping {
        return Outcome::NotApplicable(
            "no independently-authored legacy delta stream — a lockstep would compare the test against itself".into(),
        );
    }
    let adapter = *registry().get(desc.venue).unwrap();
    let model = adapter.book_model("orders");
    let frames = match replay_paired(desc) {
        Ok(f) => f,
        Err(e) => return Outcome::Fail(e),
    };
    let paired = match paired_delta_streams(&frames) {
        Ok(p) => p,
        Err(outcome) => return outcome,
    };

    let canonical = TradingPair::new(desc.canonical_base, desc.canonical_quote);
    let mut legacy_book = OrderbookDelta::new(canonical.clone())
        .with_max_depth(desc.legacy_engine_max_depth);
    let mut framework_book =
        SourcedOrderbook::new(canonical.clone(), model.clone(), model.recovery_action());

    let (seed_update_id, stream): (u64, Vec<(NormalizedDelta, NormalizedDelta)>) =
        match desc.seeding {
            Seeding::RestFixture => {
                let Some(rest_rel) = desc.rest_fixture else {
                    return Outcome::Fail("REST-seeded venue has no rest_fixture".into());
                };
                let raw = match fixture_raw(rest_rel) {
                    Ok(r) => r,
                    Err(e) => return Outcome::Fail(e),
                };
                let legacy_seed = match (desc.shim)(LegacyInput::RestSeed {
                    body: &raw,
                    wire_symbol: desc.wire_symbol,
                }) {
                    Ok(decoded) => match decoded
                        .production_deltas
                        .and_then(|deltas| deltas.into_iter().next())
                    {
                        Some(seed) => seed,
                        None => {
                            return Outcome::Fail(
                                "legacy shim produced no seed from the REST fixture"
                                    .into(),
                            );
                        }
                    },
                    Err(e) => return Outcome::Fail(format!("legacy seed: {e}")),
                };
                let framework_seed = match adapter.replay_seed(&raw, desc.wire_symbol) {
                    Ok(Some(seed)) => seed,
                    Ok(None) => {
                        return Outcome::Fail(
                            "adapter returned no seed from the REST fixture".into(),
                        );
                    }
                    Err(e) => {
                        return Outcome::Fail(format!("adapter seed parse failed: {e}"));
                    }
                };
                if legacy_seed.update_id != framework_seed.update_id {
                    return Outcome::Fail(format!(
                        "seed update_id diverges: legacy {}, framework {}",
                        legacy_seed.update_id, framework_seed.update_id
                    ));
                }
                let seed_id = legacy_seed.update_id;
                if let Err(e) = apply_legacy(&mut legacy_book, &legacy_seed, &canonical) {
                    return Outcome::Fail(format!("legacy seed apply failed: {e}"));
                }
                if let Err(e) =
                    apply_framework(&mut framework_book, &framework_seed, &canonical)
                {
                    return Outcome::Fail(format!("framework seed apply failed: {e}"));
                }
                (seed_id, paired)
            }
            Seeding::SelfSeedFirstSnapshot => {
                let Some(seed_position) = paired.iter().position(|(l, _)| l.is_snapshot)
                else {
                    return Outcome::Fail(
                        "self-seed venue: no snapshot delta in the fixture".into(),
                    );
                };
                let (legacy_seed, framework_seed) = paired[seed_position].clone();
                if let Err(e) = apply_legacy(&mut legacy_book, &legacy_seed, &canonical) {
                    return Outcome::Fail(format!("legacy seed apply failed: {e}"));
                }
                if let Err(e) =
                    apply_framework(&mut framework_book, &framework_seed, &canonical)
                {
                    return Outcome::Fail(format!("framework seed apply failed: {e}"));
                }
                (legacy_seed.update_id, paired[seed_position + 1..].to_vec())
            }
        };

    if !books_equal(&legacy_book, framework_book.book()) {
        return Outcome::Fail("books diverged at the seed".into());
    }

    let mut applied = 0usize;
    let mut discarded = 0usize;
    let mut best_bid_moved = false;
    let seed_best_bid = legacy_book.best_bid();

    for (legacy_delta, framework_delta) in &stream {
        if desc.seeding == Seeding::RestFixture
            && legacy_delta.update_id <= seed_update_id
        {
            discarded += 1;
            continue;
        }
        if legacy_delta.is_snapshot {
            discarded += 1;
            continue;
        }
        if let Err(e) = apply_legacy(&mut legacy_book, legacy_delta, &canonical) {
            return Outcome::Fail(format!("legacy engine rejected delta {applied}: {e}"));
        }
        if let Err(e) = apply_framework(&mut framework_book, framework_delta, &canonical)
        {
            return Outcome::Fail(format!(
                "framework engine gapped at delta {applied}: {e} (the legacy engine accepted it)"
            ));
        }
        if framework_book.state() != OrderBookState::Synced {
            return Outcome::Fail(format!(
                "framework book left Synced at delta {applied}: {:?}",
                framework_book.state()
            ));
        }
        if !books_equal(&legacy_book, framework_book.book()) {
            return Outcome::Fail(format!(
                "books diverged after delta {applied} (update_id {})",
                legacy_delta.update_id
            ));
        }
        if legacy_book.best_bid() != framework_book.book().best_bid() {
            return Outcome::Fail(format!("best_bid diverged at delta {applied}"));
        }
        if legacy_book.best_ask() != framework_book.book().best_ask() {
            return Outcome::Fail(format!("best_ask diverged at delta {applied}"));
        }
        if legacy_book.best_bid() != seed_best_bid {
            best_bid_moved = true;
        }
        applied += 1;
    }

    let floor = desc.min_book_frames.saturating_sub(1 + discarded);
    if applied < floor {
        return Outcome::Fail(format!(
            "only {applied} deltas reached both engines, below the floor of {floor}"
        ));
    }
    if desc.seeding == Seeding::RestFixture && discarded == 0 {
        return Outcome::Fail(
            "no pre-seed delta was discarded — the REST reconcile branch is unexercised"
                .into(),
        );
    }
    if legacy_book.bid_depth() == 0 || legacy_book.ask_depth() == 0 {
        return Outcome::Fail("legacy book ended without both sides populated".into());
    }
    if !best_bid_moved {
        return Outcome::Fail(
            "best_bid never moved across the whole replay — the books may be frozen"
                .into(),
        );
    }
    Outcome::Pass
}

fn apply_legacy(
    book: &mut OrderbookDelta,
    delta: &NormalizedDelta,
    canonical: &TradingPair,
) -> Result<(), String> {
    let mut delta = delta.clone();
    delta.symbol = canonical.to_canonical();
    book.process(&delta)
        .map_err(|e| format!("{e:?}"))
        .map(|_| ())
}

fn apply_framework(
    book: &mut SourcedOrderbook,
    delta: &NormalizedDelta,
    canonical: &TradingPair,
) -> Result<(), String> {
    let mut delta = delta.clone();
    delta.symbol = canonical.to_canonical();
    book.apply(delta)
        .map_err(|e| format!("{:?}", e.reason))
        .map(|_| ())
}

pub const KINDS: &[Kind] = &[
    Kind {
        name: "trade_wire_parity",
        proves: "one framework Trade per legacy wire print, in order, with identical id, price, amount, taker side, venue timestamp and sequence",
        run: trade_wire_parity,
    },
    Kind {
        name: "book_wire_parity",
        proves: "one framework Book per legacy wire book element, with identical symbol, ids, levels, checksum, snapshot flag and venue timestamp",
        run: book_wire_parity,
    },
    Kind {
        name: "legacy_delta_equivalence",
        proves: "where src/sources owns a to_normalized, its delta stream equals the adapter's field for field",
        run: legacy_delta_equivalence,
    },
    Kind {
        name: "reconstruction_lockstep",
        proves: "OrderbookDelta fed the legacy stream and SourcedOrderbook fed the framework stream hold identical levels after every delta",
        run: reconstruction_lockstep,
    },
];
