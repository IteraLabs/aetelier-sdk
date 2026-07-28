//! Repo-resident mirror of the data-plane Atlas invariant IDs — the join key
//! between the spec and its emitters.
//!
//! The Atlas (the platform's private FSM + taxonomy spec set) is the source of
//! truth for what each ID means; this module carries only the OPAQUE ids so
//! that every emitter of an invariant — a static conformance kind, a live
//! `SourceMetrics` counter, or (future) a governance query — can declare which
//! invariant it observes, and a coverage check can fail the build when an
//! enforced invariant loses its last emitter or an emitter cites an id that
//! was never registered. Amendment discipline: a spec amendment that mints or
//! retires an enforced id updates this registry in the same change.
//!
//! ID grammar (mirrors the spec): `<PLANE>-<ENTITY>-<ARTIFACT>-<N>` with the
//! entity optional, plane in {CTL, DAT, CMP, NET}, artifact in {S, T, INV,
//! SEQ}, e.g. `DAT-OB-INV-16`, `DAT-SEQ-2`.

/// Enforced invariant registry: every id here declares at least one live
/// emitter surface (a conformance kind and/or a `SourceMetrics` counter).
/// Spec ids WITHOUT a wired emitter are deliberately absent — presence in
/// this list is the claim the coverage check enforces.
pub const ENFORCED: &[&str] = &[
    "DAT-OB-INV-2",  // apply accepts contiguous input and reaches Synced
    "DAT-OB-T-4",    // a sequence gap transitions the book to Gapped
    "DAT-OB-INV-9",  // forward-jump gap predicate rejects a dropped delta
    "DAT-OB-INV-16", // reconstructed book not crossed, positive, ordered
    "DAT-TB-INV-6",  // trade gaps bump trade_gaps/trades_lost, never resync
    "DAT-TB-INV-12", // trade values positive, timestamps micro-second scale
    "DAT-RT-INV-14", // gap/resync/checksum counter discipline
    "DAT-CN-INV-23", // coinbase connection-level sequence continuity oracle
    "DAT-CN-INV-24", // bitso per-book envelope-sequence continuity oracle
    "DAT-SK-INV-1",  // flush failure retains the buffer
    "DAT-SK-T-16",   // gap incident appended to the JSONL gap ledger
    "DAT-RC-INV-2",  // reconcile fetches are incident/sweep-bounded
    "DAT-RC-INV-4",  // recovery counters never rewrite loss accounting
];

/// `SourceMetrics` snapshot field -> the invariant id(s) that counter
/// observes live. Scope: the loss-accounting / recovery / flush families.
/// Fields with no minted invariant yet (`decode_err`, `dropped_frames`,
/// latency gauges) are deliberately unmapped until the spec mints ids for
/// them.
pub const METRIC_EMITTERS: &[(&str, &[&str])] = &[
    ("gaps", &["DAT-RT-INV-14", "DAT-OB-T-4"]),
    ("resyncs", &["DAT-RT-INV-14"]),
    ("checksum_fail", &["DAT-RT-INV-14"]),
    ("trade_gaps", &["DAT-TB-INV-6"]),
    ("trades_lost", &["DAT-TB-INV-6"]),
    ("flush_failures", &["DAT-SK-INV-1"]),
    ("trade_gap_incidents", &["DAT-SK-T-16"]),
    ("trade_gap_window_us", &["DAT-SK-T-16"]),
    ("possible_dropped_trades", &["DAT-SK-T-16"]),
    ("trade_loss_confidence", &["DAT-SK-T-16"]),
    ("book_gap_incidents", &["DAT-CN-INV-23"]),
    ("book_gap_window_us", &["DAT-CN-INV-23"]),
    ("deltas_missed_exact", &["DAT-CN-INV-24"]),
    ("trades_recovered", &["DAT-RC-INV-4"]),
    ("reconcile_fetches", &["DAT-RC-INV-2", "DAT-RC-INV-4"]),
    ("reconcile_failures", &["DAT-RC-INV-4"]),
];

/// Validate an id against the platform grammar
/// `^(CTL|DAT|CMP|NET)-(?:[A-Z]{1,3}-)?(S|T|INV|SEQ)-\d+$` without a regex
/// dependency.
pub fn is_valid_atlas_id(id: &str) -> bool {
    let parts: Vec<&str> = id.split('-').collect();
    let (plane, entity, artifact, number) = match parts.as_slice() {
        [p, a, n] => (*p, None, *a, *n),
        [p, e, a, n] => (*p, Some(*e), *a, *n),
        _ => return false,
    };
    if !matches!(plane, "CTL" | "DAT" | "CMP" | "NET") {
        return false;
    }
    if let Some(e) = entity
        && (e.is_empty() || e.len() > 3 || !e.bytes().all(|b| b.is_ascii_uppercase()))
    {
        return false;
    }
    if !matches!(artifact, "S" | "T" | "INV" | "SEQ") {
        return false;
    }
    !number.is_empty() && number.bytes().all(|b| b.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grammar_accepts_the_spec_examples_and_rejects_malformed() {
        for ok in [
            "DAT-OB-T-3",
            "DAT-OB-INV-3",
            "DAT-SEQ-2",
            "CTL-SV-T-12",
            "CMP-XV-INV-1",
        ] {
            assert!(is_valid_atlas_id(ok), "{ok} should parse");
        }
        for bad in [
            "DAT-OB-INV",
            "DAT-ORDB-INV-1",
            "XXX-OB-INV-1",
            "DAT-ob-INV-1",
            "DAT-OB-X-1",
            "DAT-OB-INV-1a",
            "DAT--INV-1",
            "",
        ] {
            assert!(!is_valid_atlas_id(bad), "{bad} should be rejected");
        }
    }

    #[test]
    fn registry_and_map_ids_are_grammar_valid_and_unique() {
        let mut seen = std::collections::HashSet::new();
        for id in ENFORCED {
            assert!(is_valid_atlas_id(id), "ENFORCED id {id} fails the grammar");
            assert!(seen.insert(*id), "duplicate ENFORCED id {id}");
        }
        for (field, ids) in METRIC_EMITTERS {
            assert!(!field.is_empty());
            for id in *ids {
                assert!(
                    is_valid_atlas_id(id),
                    "metric {field} cites malformed id {id}"
                );
            }
        }
    }
}
