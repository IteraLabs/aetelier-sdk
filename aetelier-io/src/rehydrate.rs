//! Batch rehydration — offline repair of persisted trade parquets from the
//! venue's REST trades endpoint.
//!
//! Reads every trades parquet in a directory, asks the venue REST API for the
//! same id range (book-scoped), and set-differences by trade id: any print the
//! venue serves that the files lack is a recovered print, written — stamped
//! [`TradeOrigin::Rest`](aetelier_types::trades::TradeOrigin::Rest) — into a NEW `*_rehydrated` file beside the
//! originals (originals are immutable; provenance is per-row via the `origin`
//! column).
//!
//! Set-difference by id equality is the venue-agnostic core: it never assumes
//! id density (bitso's global counter has legitimate per-book gaps), and it
//! works for every venue with an id- or time-anchored REST fetch (the
//! coverage matrix). Venues whose REST retention cannot reach the requested
//! window return nothing there — the report counts that remainder as
//! `unrecoverable` instead of pretending.
//!
//! Requires the `connect` + `parquet` features (the venue fetchers live in
//! `aetelier-connect`).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use aetelier_connect::framework::reconcile::{TradePos, TradesRestFetch};
use aetelier_types::errors::PersistError;
use aetelier_types::trades::Trade;
use aetelier_types::trading_pair::TradingPair;

/// Cap on fetch iterations per run — bounds a runaway walk against a very
/// deep file set (each iteration is one `fetch_after`, itself page-capped).
const MAX_FETCH_ROUNDS: usize = 200;

/// Outcome of one rehydration run.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RehydrateReport {
    pub exchange: String,
    pub wire_symbol: String,
    pub files_scanned: usize,
    pub trades_read: usize,
    /// Venue id range the files cover ([min, max]); the REST sweep spans it.
    pub id_range: Option<(u64, u64)>,
    /// Prints the venue served that the files lacked — recovered and written.
    pub recovered: u64,
    /// Rounds where the venue returned nothing while the range was still
    /// uncovered — retention ran out; the remainder is honestly unreachable.
    pub exhausted_before_range_end: bool,
    /// Path of the written `*_rehydrated` parquet (`None` when nothing was
    /// recovered — no file is written for an already-complete set).
    pub output_path: Option<PathBuf>,
}

/// Rehydrate every trades parquet under `trades_dir` for one (exchange,
/// wire_symbol) stream, using `fetcher` as the venue REST source.
///
/// The fetcher is injected for testability (golden tests use a canned one);
/// production callers resolve it from
/// [`aetelier_connect::framework::reconcile::trades_rest_fetcher`].
pub async fn rehydrate_trades_dir(
    trades_dir: &Path,
    exchange: &str,
    wire_symbol: &str,
    pair: &TradingPair,
    fetcher: &dyn TradesRestFetch,
) -> Result<RehydrateReport, PersistError> {
    // 1. Read the persisted set.
    let mut files = Vec::new();
    for entry in std::fs::read_dir(trades_dir)? {
        let path = entry?.path();
        if path.extension().is_some_and(|e| e == "parquet")
            && !path
                .file_name()
                .is_some_and(|n| n.to_string_lossy().contains("rehydrated"))
        {
            files.push(path);
        }
    }
    files.sort();

    let mut present: BTreeMap<u64, Trade> = BTreeMap::new();
    let mut trades_read = 0usize;
    for f in &files {
        for t in crate::trades::read_trades_parquet(f)? {
            trades_read += 1;
            if let Ok(id) = t.id.parse::<u64>() {
                present.entry(id).or_insert(t);
            }
        }
    }

    let mut report = RehydrateReport {
        exchange: exchange.to_string(),
        wire_symbol: wire_symbol.to_string(),
        files_scanned: files.len(),
        trades_read,
        id_range: None,
        recovered: 0,
        exhausted_before_range_end: false,
        output_path: None,
    };
    let (Some((&min_id, first)), Some((&max_id, _))) =
        (present.first_key_value(), present.last_key_value())
    else {
        return Ok(report); // nothing persisted — nothing to repair
    };
    report.id_range = Some((min_id, max_id));

    // 2. Walk the venue REST over the same range; set-difference by id.
    let mut recovered: Vec<Trade> = Vec::new();
    let mut pos = TradePos {
        id: min_id.saturating_sub(1),
        ts_us: first.source_trade_ts_us.saturating_sub(1_000_000),
    };
    for _ in 0..MAX_FETCH_ROUNDS {
        if pos.id >= max_id {
            break;
        }
        let batch = fetcher
            .fetch_after(wire_symbol, pair, pos)
            .await
            .map_err(|e| PersistError::Parse(format!("rehydrate fetch: {e}")))?;
        if batch.is_empty() {
            // The venue has nothing after `pos` — either we reached the live
            // head (range covered) or retention ends before our range does.
            report.exhausted_before_range_end = pos.id < max_id;
            break;
        }
        let mut advanced = false;
        for t in batch {
            let Ok(id) = t.id.parse::<u64>() else {
                continue;
            };
            if id > pos.id {
                pos = TradePos {
                    id,
                    ts_us: t.source_trade_ts_us,
                };
                advanced = true;
            }
            if id > max_id {
                continue; // beyond the files' coverage — not a repair target
            }
            if let std::collections::btree_map::Entry::Vacant(slot) = present.entry(id) {
                slot.insert(t.clone());
                recovered.push(t);
            }
        }
        if !advanced {
            report.exhausted_before_range_end = pos.id < max_id;
            break;
        }
    }

    // 3. Write the merged, id-ordered set as ONE rehydrated file (originals
    //    untouched; consumers get a single complete dataset with per-row
    //    provenance).
    report.recovered = recovered.len() as u64;
    if !recovered.is_empty() {
        let merged: Vec<Trade> = present.into_values().collect();
        let name = format!(
            "{}_{}_trades_rehydrated_{}.parquet",
            exchange,
            wire_symbol.replace(['/', ':'], "-"),
            chrono::Utc::now().format("%Y%m%d_%H%M%S"),
        );
        let out = trades_dir.join(name);
        crate::trades::write_trades_parquet(&merged, &out)?;
        report.output_path = Some(out);
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aetelier_types::trades::{TradeOrigin, TradeSide};

    fn trade(id: u64, origin: TradeOrigin) -> Trade {
        Trade {
            source_trade_ts_us: 1_700_000_000_000_000 + id * 1_000,
            local_trade_ts_us: 0,
            source_trade_rtt_us: 0,
            pair: TradingPair::new("BTC", "USDT"),
            side: TradeSide::Buy,
            amount: rust_decimal::Decimal::ONE,
            price: rust_decimal::Decimal::from(100),
            exchange: "binance".into(),
            id: id.to_string(),
            origin,
        }
    }

    /// Canned venue: serves the FULL dense range 1..=10 (as the real venue
    /// would), regardless of what the files hold.
    struct CannedFetch;

    #[async_trait::async_trait]
    impl TradesRestFetch for CannedFetch {
        async fn fetch_after(
            &self,
            _wire: &str,
            _pair: &TradingPair,
            pos: TradePos,
        ) -> Result<Vec<Trade>, aetelier_connect::errors::ExchangeError> {
            Ok((pos.id + 1..=10)
                .map(|id| trade(id, TradeOrigin::Rest))
                .collect())
        }
    }

    /// Canned venue whose retention starts at id 6 — older ids are gone.
    struct TruncatedFetch;

    #[async_trait::async_trait]
    impl TradesRestFetch for TruncatedFetch {
        async fn fetch_after(
            &self,
            _wire: &str,
            _pair: &TradingPair,
            pos: TradePos,
        ) -> Result<Vec<Trade>, aetelier_connect::errors::ExchangeError> {
            Ok((pos.id.max(5) + 1..=10)
                .map(|id| trade(id, TradeOrigin::Rest))
                .collect())
        }
    }

    #[tokio::test]
    async fn golden_holes_are_exactly_filled_with_rest_provenance() {
        let dir = tempfile::tempdir().unwrap();
        // Persisted set: ids 1..=10 EXCEPT 4 and 7 (two holes).
        let persisted: Vec<Trade> = (1..=10u64)
            .filter(|id| *id != 4 && *id != 7)
            .map(|id| trade(id, TradeOrigin::Ws))
            .collect();
        crate::trades::write_trades_parquet(
            &persisted,
            &dir.path().join("binance_BTC-USDT_trades_sync_1.parquet"),
        )
        .unwrap();

        let report = rehydrate_trades_dir(
            dir.path(),
            "binance",
            "BTCUSDT",
            &TradingPair::new("BTC", "USDT"),
            &CannedFetch,
        )
        .await
        .unwrap();

        assert_eq!(report.recovered, 2, "exactly the two holes");
        assert!(!report.exhausted_before_range_end);
        let out = report.output_path.expect("rehydrated file written");
        let merged = crate::trades::read_trades_parquet(&out).unwrap();
        assert_eq!(merged.len(), 10, "complete dense set");
        let by_id: std::collections::HashMap<u64, &Trade> = merged
            .iter()
            .map(|t| (t.id.parse::<u64>().unwrap(), t))
            .collect();
        assert_eq!(by_id[&4].origin, TradeOrigin::Rest);
        assert_eq!(by_id[&7].origin, TradeOrigin::Rest);
        assert_eq!(by_id[&5].origin, TradeOrigin::Ws, "originals keep ws");
        // Idempotence: a second run over the same dir (now containing the
        // rehydrated file, which is skipped by name) finds the same holes in
        // the ORIGINALS only — the output is reproducible, not compounding.
        let again = rehydrate_trades_dir(
            dir.path(),
            "binance",
            "BTCUSDT",
            &TradingPair::new("BTC", "USDT"),
            &CannedFetch,
        )
        .await
        .unwrap();
        assert_eq!(again.recovered, 2);
    }

    #[tokio::test]
    async fn retention_shortfall_is_reported_never_papered_over() {
        let dir = tempfile::tempdir().unwrap();
        // Files hold 1..=10 except 3 and 8; the venue only serves ids > 5.
        let persisted: Vec<Trade> = (1..=10u64)
            .filter(|id| *id != 3 && *id != 8)
            .map(|id| trade(id, TradeOrigin::Ws))
            .collect();
        crate::trades::write_trades_parquet(
            &persisted,
            &dir.path().join("binance_BTC-USDT_trades_sync_1.parquet"),
        )
        .unwrap();

        let report = rehydrate_trades_dir(
            dir.path(),
            "binance",
            "BTCUSDT",
            &TradingPair::new("BTC", "USDT"),
            &TruncatedFetch,
        )
        .await
        .unwrap();
        assert_eq!(report.recovered, 1, "only id 8 is recoverable");
        let merged =
            crate::trades::read_trades_parquet(&report.output_path.unwrap()).unwrap();
        assert_eq!(merged.len(), 9, "id 3 stays missing — honestly");
    }
}
