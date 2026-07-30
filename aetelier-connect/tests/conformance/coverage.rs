//! Emitter-coverage meta-tests: the join between the Atlas invariant registry
//! (`framework::atlas`) and its emitter surfaces — static conformance kinds
//! and live `SourceMetrics` counters.
//!
//! What makes the spec self-defending: an enforced invariant that loses its
//! last emitter fails `every_enforced_invariant_has_an_emitter`; an emitter
//! citing an id the registry never enforced fails `no_orphan_citations`; a
//! registry entry whose metric field disappears from the snapshot fails
//! `metric_emitter_fields_exist_on_the_snapshot`. A new invariant therefore
//! reaches its gates by construction: register the id, wire an emitter, or
//! the build says which half is missing.

use std::collections::HashSet;

use aetelier_connect::framework::atlas::{ENFORCED, METRIC_EMITTERS, is_valid_atlas_id};
use aetelier_connect::framework::budget::SourceMetricsSnapshot;

use crate::kinds::KINDS;

fn cited_ids() -> Vec<(&'static str, &'static str)> {
    let mut cited = Vec::new();
    for kind in KINDS {
        for id in kind.invariants {
            cited.push((kind.name, *id));
        }
    }
    for (field, ids) in METRIC_EMITTERS {
        for id in *ids {
            cited.push((*field, *id));
        }
    }
    cited
}

#[test]
fn every_cited_id_is_grammar_valid() {
    for (emitter, id) in cited_ids() {
        assert!(
            is_valid_atlas_id(id),
            "emitter '{emitter}' cites malformed atlas id '{id}'"
        );
    }
}

#[test]
fn every_enforced_invariant_has_an_emitter() {
    let cited: HashSet<&str> = cited_ids().into_iter().map(|(_, id)| id).collect();
    let uncovered: Vec<&&str> =
        ENFORCED.iter().filter(|id| !cited.contains(**id)).collect();
    assert!(
        uncovered.is_empty(),
        "enforced invariants with NO emitter (wire a kind or a metric, or \
         retire the id from ENFORCED): {uncovered:?}"
    );
}

#[test]
fn no_orphan_citations() {
    let enforced: HashSet<&str> = ENFORCED.iter().copied().collect();
    let orphans: Vec<(&str, &str)> = cited_ids()
        .into_iter()
        .filter(|(_, id)| !enforced.contains(id))
        .collect();
    assert!(
        orphans.is_empty(),
        "emitters cite ids missing from atlas::ENFORCED (register them or \
         drop the citation): {orphans:?}"
    );
}

#[test]
fn metric_emitter_fields_exist_on_the_snapshot() {
    let snapshot = serde_json::to_value(SourceMetricsSnapshot::default())
        .expect("snapshot serializes");
    let obj = snapshot.as_object().expect("snapshot is a JSON object");
    for (field, _) in METRIC_EMITTERS {
        assert!(
            obj.contains_key(*field),
            "METRIC_EMITTERS maps '{field}', which is not a SourceMetricsSnapshot field"
        );
    }
}

#[test]
fn certifying_kinds_carry_structured_ids() {
    // The independent-oracle and negative-path kinds exist to certify specific
    // spec ids — they must never regress to prose-only anchoring.
    for name in [
        "trade_continuity",
        "trade_invariants",
        "book_delta_application",
        "book_invariants",
        "gap_detection",
    ] {
        let kind = KINDS
            .iter()
            .find(|k| k.name == name)
            .unwrap_or_else(|| panic!("kind '{name}' missing from KINDS"));
        assert!(
            !kind.invariants.is_empty(),
            "kind '{name}' lost its structured invariant ids"
        );
    }
}
