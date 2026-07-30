//! The conformance kinds — generic checks over a `VenueConformance`, each
//! reading the reconstruction model / codec / decode path from the adapter
//! registry so it cannot drift from code.
//!
//! Registered in `KINDS`; the `conformance_suite!` macro instantiates every
//! entry for every venue, so adding a kind here ratchets it to all venues.

use aetelier_connect::framework::model::{
    DomainEvent, OrderBookState, ReconstructionModel, SourcedOrderbook,
};
use aetelier_connect::framework::registry::registry;
use aetelier_types::orderbooks::NormalizedDelta;
use aetelier_types::trading_pair::TradingPair;
use rust_decimal::Decimal;

use super::harness::{
    Class, Kind, Outcome, VenueConformance, fixture_lines, fixture_raw,
};

/// Replay every fixture frame through the adapter's real decode+normalize
/// path; collect the DomainEvents. Errors surface the venue id + the frame.
fn replay_all(desc: &VenueConformance) -> Result<Vec<DomainEvent>, String> {
    let adapter = *registry().get(desc.venue).unwrap();
    let lines = fixture_lines(desc.ws_fixture)?;
    if lines.is_empty() {
        return Err(format!("fixture {} has no frames", desc.ws_fixture));
    }
    let mut events = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        match adapter.replay_frame(line) {
            Ok(mut evs) => events.append(&mut evs),
            Err(e) => return Err(format!("frame {i} failed to decode: {e}")),
        }
    }
    Ok(events)
}

fn class_of(ev: &DomainEvent) -> Option<Class> {
    match ev {
        DomainEvent::Book(_) => Some(Class::Book),
        DomainEvent::Trade { .. } => Some(Class::Trade),
        // Control signals (connection-gap etc.) are not a documented message
        // class — they never satisfy or violate an expect_classes entry.
        DomainEvent::ConnectionGap { .. } => None,
    }
}

/// KIND: decode surface. Every fixture frame decodes without error, and every
/// message class the venue docs declare for our channels actually appears in
/// the fixture (no silently-missing class). Anchors DAT-CN decode + the
/// normalize boundary.
fn decode_surface(desc: &VenueConformance) -> Outcome {
    let events = match replay_all(desc) {
        Ok(e) => e,
        Err(e) => return Outcome::Fail(e),
    };
    if events.is_empty() {
        return Outcome::Fail("no DomainEvents produced from the fixture".into());
    }
    let seen: Vec<Class> = {
        let mut v = Vec::new();
        for ev in &events {
            let Some(c) = class_of(ev) else { continue };
            if !v.contains(&c) {
                v.push(c);
            }
        }
        v
    };
    for want in desc.expect_classes {
        if !seen.contains(want) {
            return Outcome::Fail(format!(
                "documented class {want:?} never appears in the fixture (saw {seen:?})"
            ));
        }
    }
    Outcome::Pass
}

/// KIND: seeding taxonomy. The adapter's declared model derives the seeding
/// need + source the venue docs specify. The single doc-vs-code check;
/// everything is read from the registry. Anchors DAT-RT seeding + the Book
/// family / SnapshotSource SSoT.
fn seeding_taxonomy(desc: &VenueConformance) -> Outcome {
    let adapter = *registry().get(desc.venue).unwrap();
    let model = adapter.book_model("orders");
    if !desc.expect_source.matches(model.snapshot_source()) {
        return Outcome::Fail(format!(
            "snapshot source: doc expects {:?}, model has {:?}",
            desc.expect_source,
            model.snapshot_source()
        ));
    }
    if model.needs_rest() != desc.expect_needs_rest {
        return Outcome::Fail(format!(
            "needs_rest: doc expects {}, model derives {}",
            desc.expect_needs_rest,
            model.needs_rest()
        ));
    }
    // Coherence: a REST-seeded model must ship a seeder (the worker bails
    // otherwise). Read from the registry, not the descriptor.
    if model.needs_rest() && adapter.rest_seeder().is_none() {
        return Outcome::Fail(
            "REST-seeded model but adapter provides no rest_seeder".into(),
        );
    }
    Outcome::Pass
}

/// KIND: symbol canonicalization. The venue's wire symbol round-trips through
/// the profile codec to the documented canonical pair. Anchors venue
/// variation (symbol codec) + DAT-RT symbol decode.
fn symbol_canonicalization(desc: &VenueConformance) -> Outcome {
    let adapter = *registry().get(desc.venue).unwrap();
    let codec = &adapter.profile().symbol_codec;
    let want = TradingPair::new(desc.canonical_base, desc.canonical_quote);
    match codec.decode(desc.wire_symbol) {
        Some(pair) if pair.to_canonical() == want.to_canonical() => Outcome::Pass,
        Some(pair) => Outcome::Fail(format!(
            "codec decoded {} to {}, doc expects {}",
            desc.wire_symbol,
            pair.to_canonical(),
            want.to_canonical()
        )),
        None => Outcome::Fail(format!(
            "codec could not decode wire symbol {}",
            desc.wire_symbol
        )),
    }
}

/// KIND: trade continuity. Trades replay from the fixture, and where the venue
/// supplies a monotonic trade sequence, it is non-decreasing (the input to the
/// SourcedTradebook loss accounting). Skips if the fixture carries no trades
/// and the venue does not declare the Trade class. Anchors DAT-TB.
fn trade_continuity(desc: &VenueConformance) -> Outcome {
    let events = match replay_all(desc) {
        Ok(e) => e,
        Err(e) => return Outcome::Fail(e),
    };
    let seqs: Vec<u64> = events
        .iter()
        .filter_map(|ev| match ev {
            DomainEvent::Trade { sequence, .. } => *sequence,
            _ => None,
        })
        .collect();
    let trade_count = events
        .iter()
        .filter(|ev| matches!(ev, DomainEvent::Trade { .. }))
        .count();

    if !desc.expect_classes.contains(&Class::Trade) {
        return Outcome::Skip("Trade class not in this venue's fixture scope yet".into());
    }
    if trade_count == 0 {
        return Outcome::Fail("Trade class declared but no trades in the fixture".into());
    }
    if seqs.is_empty() {
        return Outcome::NotApplicable(
            "venue supplies no trade sequence (best-effort)".into(),
        );
    }
    for w in seqs.windows(2) {
        if w[1] < w[0] {
            return Outcome::Fail(format!(
                "trade sequence went backwards: {} then {}",
                w[0], w[1]
            ));
        }
    }
    Outcome::Pass
}

/// Reconstruct a venue's order book from its fixture — replay frames, seed per
/// the reconstruction model, apply deltas — returning the finished book plus the
/// count of applied deltas. On the way it errors as an `Outcome`:
/// `NotApplicable` when the venue declares no Book class, `Fail` on a
/// reconstruction gap. Shared by every book-level kind so they all certify the
/// SAME reconstruction rather than each rolling its own.
fn reconstruct_book(desc: &VenueConformance) -> Result<(SourcedOrderbook, u64), Outcome> {
    let adapter = *registry().get(desc.venue).unwrap();
    let model = adapter.book_model("orders");
    let events = replay_all(desc).map_err(Outcome::Fail)?;
    let deltas: Vec<_> = events
        .into_iter()
        .filter_map(|ev| match ev {
            DomainEvent::Book(d) => Some(d),
            _ => None,
        })
        .collect();
    if deltas.is_empty() {
        if desc.expect_classes.contains(&Class::Book) {
            return Err(Outcome::Fail(
                "Book class declared but no book deltas in the fixture".into(),
            ));
        }
        return Err(Outcome::NotApplicable(
            "venue declares no Book class".into(),
        ));
    }

    let canonical = TradingPair::new(desc.canonical_base, desc.canonical_quote);
    let mut book =
        SourcedOrderbook::new(canonical.clone(), model.clone(), model.recovery_action());
    let mut applied = 0u64;

    // FullRefresh: every frame is a complete book — apply each in wire order,
    // no seed/delta distinction.
    if matches!(model, ReconstructionModel::FullRefresh) {
        for mut d in deltas {
            d.symbol = canonical.to_canonical();
            match book.apply(d) {
                Ok(_) => applied += 1,
                Err(e) => {
                    return Err(Outcome::Fail(format!(
                        "full-refresh frame rejected after {applied}: {}",
                        e.reason
                    )));
                }
            }
        }
    } else if matches!(
        model.snapshot_source(),
        Some(aetelier_connect::framework::model::SnapshotSource::ReqOnSocket)
    ) {
        // In-band REQ seed (HTX): the REQ reply is a snapshot that arrives
        // mid-stream — deltas keep flowing while the REQ round-trips, so the
        // reply's WIRE position does not match its seqNum position. Mirror the
        // runtime's buffer-and-reconcile: seed at the reply, then apply every
        // buffered delta past the seed's seqNum in seqNum order (the chaining
        // delta may have arrived before the reply on the wire).
        let Some(seed_pos) = deltas.iter().rposition(|d| d.is_snapshot) else {
            return Err(Outcome::Fail(
                "ReqOnSocket venue: no REQ-reply snapshot in the fixture".into(),
            ));
        };
        let mut seed = deltas[seed_pos].clone();
        let seed_id = seed.update_id;
        seed.symbol = canonical.to_canonical();
        if let Err(e) = book.apply(seed) {
            return Err(Outcome::Fail(format!(
                "REQ-seed apply failed: {}",
                e.reason
            )));
        }
        let mut chain: Vec<_> = deltas
            .into_iter()
            .filter(|d| !d.is_snapshot && d.update_id > seed_id)
            .collect();
        chain.sort_by_key(|d| d.update_id);
        for mut d in chain {
            d.symbol = canonical.to_canonical();
            match book.apply(d) {
                Ok(_) => applied += 1,
                Err(e) => {
                    return Err(Outcome::Fail(format!(
                        "book gapped after {applied} clean applies past the REQ seed: {}",
                        e.reason
                    )));
                }
            }
        }
    } else {
        // Incremental (SeqDelta / L3), snapshot-first or REST-seeded: apply in
        // wire order, discarding deltas the seed covers.
        let seed_update_id = if model.needs_rest() {
            let Some(rest_rel) = desc.rest_fixture else {
                return Err(Outcome::Fail(
                    "REST-seeded venue has no rest_fixture in the descriptor".into(),
                ));
            };
            let raw = fixture_raw(rest_rel).map_err(Outcome::Fail)?;
            let mut seed = match adapter.replay_seed(&raw, desc.wire_symbol) {
                Ok(Some(s)) => s,
                Ok(None) => {
                    return Err(Outcome::Fail(
                        "REST-model adapter returned no seed from the fixture".into(),
                    ));
                }
                Err(e) => return Err(Outcome::Fail(format!("seed parse failed: {e}"))),
            };
            seed.symbol = canonical.to_canonical();
            let id = seed.update_id;
            if let Err(e) = book.apply(seed) {
                return Err(Outcome::Fail(format!("seed apply failed: {}", e.reason)));
            }
            Some(id)
        } else {
            None
        };

        let mut started = seed_update_id.is_some();
        for mut d in deltas {
            if let Some(seed_id) = seed_update_id
                && d.update_id <= seed_id
            {
                continue;
            }
            if !started {
                if d.is_snapshot {
                    started = true;
                } else {
                    continue;
                }
            }
            d.symbol = canonical.to_canonical();
            match book.apply(d) {
                Ok(_) => applied += 1,
                Err(e) => {
                    return Err(Outcome::Fail(format!(
                        "book gapped after {applied} clean applies: {}",
                        e.reason
                    )));
                }
            }
        }
    }
    if applied == 0 {
        return Err(Outcome::Fail("no book deltas applied".into()));
    }
    Ok((book, applied))
}

/// KIND: book delta application. The fixture's book deltas apply in order
/// (self-seeded, REST-seeded, or in-band-REQ-seeded per the model) without a
/// spurious gap, and the reconstructed book is non-degenerate — it reaches
/// `Synced` and is non-empty. Anchors DAT-OB apply + OrderBookState.
fn book_delta_application(desc: &VenueConformance) -> Outcome {
    let (book, _applied) = match reconstruct_book(desc) {
        Ok(x) => x,
        Err(outcome) => return outcome,
    };
    if book.state() != OrderBookState::Synced {
        return Outcome::Fail(format!(
            "book not Synced after replay: {:?}",
            book.state()
        ));
    }
    if book.book().bid_depth() == 0 && book.book().ask_depth() == 0 {
        return Outcome::Fail("reconstructed book is empty".into());
    }
    Outcome::Pass
}

/// KIND: book invariants. An INDEPENDENT oracle — it walks the reconstructed L2
/// book and asserts physical/logical truths that no `SeqPredicate` checks, so a
/// systematic apply error (wrong side, wrong decimal scale, a zero level) that
/// still reaches `Synced` is caught here where `book_delta_application` — which
/// only asks the code's own state machine — cannot see it. Asserts: the book is
/// not crossed (best_bid < best_ask), every level carries a strictly-positive
/// price and size, and levels are ordered (bids descending, asks ascending).
/// Anchors DAT-OB-INV-16.
fn book_invariants(desc: &VenueConformance) -> Outcome {
    let (book, _) = match reconstruct_book(desc) {
        Ok(x) => x,
        Err(outcome) => return outcome,
    };
    let ob = book.book();

    // Not crossed: the best bid must not exceed the best ask.
    if let (Some((bid, _)), Some((ask, _))) = (ob.best_bid(), ob.best_ask())
        && bid > ask
    {
        return Outcome::Fail(format!("crossed book: best_bid {bid} > best_ask {ask}"));
    }

    // Bids: strictly-positive levels, strictly descending in price.
    let bids = ob.top_bids(ob.bid_depth());
    for (p, s) in &bids {
        if *p <= Decimal::ZERO || *s <= Decimal::ZERO {
            return Outcome::Fail(format!("non-positive bid level: price {p}, size {s}"));
        }
    }
    for w in bids.windows(2) {
        if w[0].0 <= w[1].0 {
            return Outcome::Fail(format!(
                "bids not strictly descending: {} then {}",
                w[0].0, w[1].0
            ));
        }
    }

    // Asks: strictly-positive levels, strictly ascending in price.
    let asks = ob.top_asks(ob.ask_depth());
    for (p, s) in &asks {
        if *p <= Decimal::ZERO || *s <= Decimal::ZERO {
            return Outcome::Fail(format!("non-positive ask level: price {p}, size {s}"));
        }
    }
    for w in asks.windows(2) {
        if w[0].0 >= w[1].0 {
            return Outcome::Fail(format!(
                "asks not strictly ascending: {} then {}",
                w[0].0, w[1].0
            ));
        }
    }

    Outcome::Pass
}

/// KIND: trade invariants. An INDEPENDENT oracle on the replayed trades: every
/// trade carries a strictly-positive price and amount, and a microsecond-scale
/// venue timestamp (guards the µs unit contract at trade scope — a seconds/ms
/// value falls below the 2020 floor, a nanosecond value above the 2100 ceiling).
/// The taker side is a real enum by construction (unknown sides are dropped at
/// decode), so nothing to assert there. Anchors DAT-TB-INV-12.
fn trade_invariants(desc: &VenueConformance) -> Outcome {
    // UTC epoch microseconds for 2020-01-01 and 2100-01-01 — any real market
    // timestamp expressed in µs lies between them; other units do not.
    const US_FLOOR_2020: u64 = 1_577_836_800_000_000;
    const US_CEIL_2100: u64 = 4_102_444_800_000_000;

    let events = match replay_all(desc) {
        Ok(e) => e,
        Err(e) => return Outcome::Fail(e),
    };
    let mut seen = 0u64;
    for ev in &events {
        let DomainEvent::Trade { trade, .. } = ev else {
            continue;
        };
        seen += 1;
        if trade.price <= Decimal::ZERO || trade.amount <= Decimal::ZERO {
            return Outcome::Fail(format!(
                "non-positive trade: price {}, amount {}",
                trade.price, trade.amount
            ));
        }
        let ts = trade.source_trade_ts_us;
        if !(US_FLOOR_2020..US_CEIL_2100).contains(&ts) {
            return Outcome::Fail(format!(
                "trade timestamp {ts} is not microsecond-scale (outside [2020, 2100) µs)"
            ));
        }
    }
    if seen == 0 {
        return Outcome::NotApplicable("no trades in the fixture".into());
    }
    Outcome::Pass
}

/// KIND: checksum validation. For a ChecksumDelta venue, the CRC32 checksum is
/// the ONLY continuity guarantee. This kind certifies it end-to-end: every
/// book DELTA in the fixture carries a checksum (absence would fail closed per
/// the fail-closed rule), and replaying the fixture through the real ChecksumDelta apply
/// path validates every one (the book stays Synced, so at least one live CRC32
/// matched the reconstructed book). Non-checksum venues skip. Anchors DAT-OB
/// ChecksumDelta + the fail-closed rule.
fn checksum_validation(desc: &VenueConformance) -> Outcome {
    use aetelier_connect::framework::model::ReconstructionModel;
    let adapter = *registry().get(desc.venue).unwrap();
    let model = adapter.book_model("orders");
    if !matches!(model, ReconstructionModel::ChecksumDelta { .. }) {
        return Outcome::NotApplicable("not a checksum-delta venue".into());
    }
    let events = match replay_all(desc) {
        Ok(e) => e,
        Err(e) => return Outcome::Fail(e),
    };
    let deltas: Vec<_> = events
        .into_iter()
        .filter_map(|ev| match ev {
            DomainEvent::Book(d) => Some(d),
            _ => None,
        })
        .collect();
    if deltas.is_empty() {
        return Outcome::Fail("checksum venue but no book deltas in the fixture".into());
    }

    // Every non-snapshot delta must carry a checksum — a checksum venue that
    // omits one would fail closed, so the fixture must not.
    let mut checksummed = 0u64;
    for d in &deltas {
        if !d.is_snapshot {
            match d.checksum {
                Some(_) => checksummed += 1,
                None => {
                    return Outcome::Fail(
                        "a book delta carries no checksum (would fail closed)".into(),
                    );
                }
            }
        }
    }
    if checksummed == 0 {
        return Outcome::Fail("fixture exercises no delta checksum".into());
    }

    // Replay through the real ChecksumDelta apply: each validated frame either
    // matches (book stays Synced) or gaps on a CRC32 mismatch (Err). A clean
    // Synced end means every live checksum validated the reconstruction.
    let canonical = TradingPair::new(desc.canonical_base, desc.canonical_quote);
    let mut book =
        SourcedOrderbook::new(canonical.clone(), model.clone(), model.recovery_action());
    let mut started = false;
    for mut d in deltas {
        if !started {
            if d.is_snapshot {
                started = true;
            } else {
                continue;
            }
        }
        d.symbol = canonical.to_canonical();
        if let Err(e) = book.apply(d) {
            return Outcome::Fail(format!("checksum validation gapped: {}", e.reason));
        }
    }
    if book.state() != OrderBookState::Synced {
        return Outcome::Fail(format!(
            "book not Synced after checksum replay: {:?}",
            book.state()
        ));
    }
    Outcome::Pass
}

/// Seed a venue's book (per its model) and return the seeded book plus the
/// ORDERED post-seed non-snapshot deltas that would apply next. The building
/// block for `gap_detection`: it needs a clean Synced baseline plus the
/// contiguous delta run to inject a drop into. Only the sequence-seeded models
/// reach here (`gap_detection` screens out FullRefresh / L3 / Monotonic /
/// Checksum first), so it handles ReqOnSocket, REST-seeded, and self-seeded
/// snapshots. Deltas are returned in seqNum order (ReqOnSocket wire order can
/// differ) and filtered to those strictly past the seed.
fn seeded_book_and_deltas(
    desc: &VenueConformance,
) -> Result<(SourcedOrderbook, Vec<NormalizedDelta>), Outcome> {
    let adapter = *registry().get(desc.venue).unwrap();
    let model = adapter.book_model("orders");
    let events = replay_all(desc).map_err(Outcome::Fail)?;
    let deltas: Vec<NormalizedDelta> = events
        .into_iter()
        .filter_map(|ev| match ev {
            DomainEvent::Book(d) => Some(d),
            _ => None,
        })
        .collect();
    if deltas.is_empty() {
        return Err(Outcome::NotApplicable(
            "no book deltas in the fixture".into(),
        ));
    }
    let canonical = TradingPair::new(desc.canonical_base, desc.canonical_quote);
    let mut book =
        SourcedOrderbook::new(canonical.clone(), model.clone(), model.recovery_action());

    let seed_id = if matches!(
        model.snapshot_source(),
        Some(aetelier_connect::framework::model::SnapshotSource::ReqOnSocket)
    ) {
        let Some(seed_pos) = deltas.iter().rposition(|d| d.is_snapshot) else {
            return Err(Outcome::Fail(
                "ReqOnSocket venue: no REQ-reply snapshot".into(),
            ));
        };
        let mut seed = deltas[seed_pos].clone();
        let id = seed.update_id;
        seed.symbol = canonical.to_canonical();
        if let Err(e) = book.apply(seed) {
            return Err(Outcome::Fail(format!(
                "REQ-seed apply failed: {}",
                e.reason
            )));
        }
        id
    } else if model.needs_rest() {
        let Some(rest_rel) = desc.rest_fixture else {
            return Err(Outcome::Fail(
                "REST-seeded venue has no rest_fixture".into(),
            ));
        };
        let raw = fixture_raw(rest_rel).map_err(Outcome::Fail)?;
        let mut seed = match adapter.replay_seed(&raw, desc.wire_symbol) {
            Ok(Some(s)) => s,
            Ok(None) => {
                return Err(Outcome::Fail("no seed from the REST fixture".into()));
            }
            Err(e) => return Err(Outcome::Fail(format!("seed parse failed: {e}"))),
        };
        let id = seed.update_id;
        seed.symbol = canonical.to_canonical();
        if let Err(e) = book.apply(seed) {
            return Err(Outcome::Fail(format!("seed apply failed: {}", e.reason)));
        }
        id
    } else {
        let Some(seed_pos) = deltas.iter().position(|d| d.is_snapshot) else {
            return Err(Outcome::Fail("self-seed venue: no snapshot frame".into()));
        };
        let mut seed = deltas[seed_pos].clone();
        let id = seed.update_id;
        seed.symbol = canonical.to_canonical();
        if let Err(e) = book.apply(seed) {
            return Err(Outcome::Fail(format!("seed apply failed: {}", e.reason)));
        }
        id
    };

    let mut post: Vec<NormalizedDelta> = deltas
        .into_iter()
        .filter(|d| !d.is_snapshot && d.update_id > seed_id)
        .collect();
    post.sort_by_key(|d| d.update_id);
    Ok((book, post))
}

/// KIND: gap detection (negative path). A DROPPED delta must be DETECTED — the
/// book transitions to Gapped (DAT-OB-T-4), never silently stays Synced. Seeds a
/// clean baseline from the real stream, drops one delta mid-run, and asserts the
/// next apply gaps. `book_delta_application` proving the run reconstructs clean
/// is the precondition: a contiguous run means any single drop MUST break
/// continuity. Only sequence-continuity models can detect a mid-stream drop;
/// FullRefresh (full books), L3 (order-keyed, no sequence), and Monotonic
/// (connection counter — DAT-OB-INV-10) structurally cannot, and ChecksumDelta
/// drop-detection is already certified by `checksum_validation` — so all four
/// are NotApplicable. Anchors DAT-OB-T-4 + DAT-OB-INV-9.
fn gap_detection(desc: &VenueConformance) -> Outcome {
    use aetelier_connect::framework::model::SeqPredicate;
    let adapter = *registry().get(desc.venue).unwrap();
    let model = adapter.book_model("orders");
    match &model {
        ReconstructionModel::FullRefresh => {
            return Outcome::NotApplicable(
                "FullRefresh: every frame is a full book — a dropped delta is invisible"
                    .into(),
            );
        }
        ReconstructionModel::L3 { .. } => {
            return Outcome::NotApplicable(
                "L3 order-keyed: no sequence continuity to break".into(),
            );
        }
        ReconstructionModel::ChecksumDelta { .. } => {
            return Outcome::NotApplicable(
                "checksum venue: drop detection is certified by checksum_validation"
                    .into(),
            );
        }
        ReconstructionModel::SeqDelta {
            predicate: SeqPredicate::Monotonic,
            ..
        } => {
            return Outcome::NotApplicable(
                "Monotonic connection-counter cannot prove per-book contiguity (DAT-OB-INV-10)"
                    .into(),
            );
        }
        ReconstructionModel::SeqDelta { .. } => {}
    }

    let (mut book, post) = match seeded_book_and_deltas(desc) {
        Ok(x) => x,
        Err(outcome) => return outcome,
    };
    if post.len() < 3 {
        return Outcome::NotApplicable(
            "too few post-seed deltas to inject a drop".into(),
        );
    }
    let canonical = TradingPair::new(desc.canonical_base, desc.canonical_quote);
    let k = post.len() / 2;

    // Establish a Synced baseline: apply the contiguous run up to the drop
    // point. book_delta_application already proves this run reconstructs clean,
    // so a gap here means the run is not a single contiguous stream (re-snapshot).
    for d in &post[..k] {
        let mut d = d.clone();
        d.symbol = canonical.to_canonical();
        if book.apply(d).is_err() {
            return Outcome::NotApplicable(
                "post-seed run is not a single contiguous delta stream (re-snapshot?)"
                    .into(),
            );
        }
    }
    if book.state() != OrderBookState::Synced {
        return Outcome::NotApplicable("baseline not Synced before injection".into());
    }

    // DROP post[k]; apply post[k+1]. Its continuity pointer references the
    // dropped delta, so a continuity-tracking model MUST gap here.
    let mut injected = post[k + 1].clone();
    injected.symbol = canonical.to_canonical();
    match book.apply(injected) {
        Err(_) if book.state() == OrderBookState::Gapped => Outcome::Pass,
        Err(_) => Outcome::Fail(format!(
            "drop raised an error but state is {:?}, not Gapped",
            book.state()
        )),
        Ok(_) => Outcome::Fail(
            "a dropped delta was NOT detected: the book stayed Synced after skipping a delta".into(),
        ),
    }
}

/// The kinds registry. Adding an entry here ratchets the kind to every venue
/// via `conformance_suite!`.
pub const KINDS: &[Kind] = &[
    Kind {
        name: "decode_surface",
        atlas: "DAT-CN decode, normalize boundary",
        invariants: &[],
        run: decode_surface,
    },
    Kind {
        name: "seeding_taxonomy",
        atlas: "DAT-RT seeding, SnapshotSource SSoT",
        invariants: &[],
        run: seeding_taxonomy,
    },
    Kind {
        name: "symbol_canonicalization",
        atlas: "venue variation (codec), DAT-RT symbol decode",
        invariants: &[],
        run: symbol_canonicalization,
    },
    Kind {
        name: "trade_continuity",
        atlas: "DAT-TB apply, gap accounting",
        invariants: &["DAT-TB-INV-6"],
        run: trade_continuity,
    },
    Kind {
        name: "trade_invariants",
        atlas: "DAT-TB-INV-12 (independent: positivity + µs-scale)",
        invariants: &["DAT-TB-INV-12"],
        run: trade_invariants,
    },
    Kind {
        name: "book_delta_application",
        atlas: "DAT-OB apply, OrderBookState",
        invariants: &["DAT-OB-INV-2"],
        run: book_delta_application,
    },
    Kind {
        name: "book_invariants",
        atlas: "DAT-OB-INV-16 (independent: not-crossed + positivity + ordering)",
        invariants: &["DAT-OB-INV-16"],
        run: book_invariants,
    },
    Kind {
        name: "gap_detection",
        atlas: "DAT-OB-T-4 gap, DAT-OB-INV-9 (negative path: a dropped delta must Gap)",
        invariants: &["DAT-OB-T-4", "DAT-OB-INV-9"],
        run: gap_detection,
    },
    Kind {
        name: "checksum_validation",
        atlas: "DAT-OB ChecksumDelta, fail-closed on absent checksum",
        invariants: &[],
        run: checksum_validation,
    },
];
