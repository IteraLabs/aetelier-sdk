//! Persisted gap ledger — one JSON line per coverage incident.
//!
//! The sentinel's counters (`SourceMetrics`) carry totals; this ledger carries
//! the per-incident record downstream consumers need offline: governance
//! audits coverage windows, and batch rehydration turns an incident window
//! into a REST back-fill range for venues whose trade loss is only estimable.
//!
//! Append-only JSONL, one file per worker under `<parquet_dir>/gaps/`:
//! an incident is written once, at gap-close, with `O_APPEND` semantics so a
//! crash mid-run loses nothing already recorded. Parquet is deliberately NOT
//! used here — incidents are rare, and an append-per-incident stream fits a
//! line format; the datatype parquet writers stay for bulk market data.

use std::io::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// Why the coverage window opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GapCause {
    /// The runtime requested a resync (book continuity break: sequence gap,
    /// checksum failure, or a connection-gap verdict from a venue tracker).
    Resync,
    /// The venue socket dropped (transport failure / server close).
    Disconnect,
}

/// One closed coverage incident.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GapIncident {
    /// UTC epoch µs when the gap opened (detection / socket loss).
    pub opened_epoch_us: u64,
    /// UTC epoch µs when recovery was proven (first reconstructed event of
    /// the next connection), or the worker stopped with the gap open.
    pub closed_epoch_us: u64,
    /// `closed - opened`.
    pub window_us: u64,
    pub cause: GapCause,
    pub exchange: String,
    /// Venue wire symbol this worker collects.
    pub symbol: String,
    /// Prints possibly dropped in the window — rate-model ESTIMATE, present
    /// only on sources whose trade accounting is Estimated (no dense
    /// sequence). Exact-armed sources count outages via the sequence carry
    /// instead, so this stays `None` there.
    pub possible_dropped_trades: Option<u64>,
}

/// The sentinel's possible-loss estimator: prints possibly dropped during a
/// gap window, from the trailing trade rate. `trailing_trades_60s` is the
/// count of trades observed in the last 60 seconds before the gap; the
/// estimate is `ceil(rate_per_sec × window_secs)`. A quiet book (zero recent
/// trades) estimates zero — honest: its outage most likely lost nothing.
/// This is a labeled ESTIMATE for `possible_dropped_trades`; it must never be
/// surfaced as a count.
pub fn estimate_possible_dropped(trailing_trades_60s: usize, window_us: u64) -> u64 {
    let rate_per_sec = trailing_trades_60s as f64 / 60.0;
    let window_secs = window_us as f64 / 1_000_000.0;
    (rate_per_sec * window_secs).ceil() as u64
}

/// Append-only writer for one worker's ledger file.
///
/// The file lives at `<dir>/gaps/<exchange>_<symbol>_gap_ledger.jsonl`, next
/// to the datatype parquet subdirs, so the collector's output tree carries
/// its own coverage record. Failures are counted (`flush_failures`) and
/// logged by the caller — the ledger must never take the collector down.
#[derive(Debug, Clone)]
pub struct GapLedger {
    path: PathBuf,
}

impl GapLedger {
    /// Build the ledger for a worker writing under `output_dir` (the parquet
    /// sink dir). Creates `gaps/` lazily on first append.
    pub fn new(output_dir: &Path, exchange: &str, symbol: &str) -> Self {
        let file = format!(
            "{}_{}_gap_ledger.jsonl",
            exchange,
            symbol.replace(['/', ':'], "-")
        );
        Self {
            path: output_dir.join("gaps").join(file),
        }
    }

    /// The ledger file path (for tests / reporting).
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append one incident as a single JSON line.
    pub fn append(&self, incident: &GapIncident) -> std::io::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let line = serde_json::to_string(incident)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        writeln!(f, "{line}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn incident(opened: u64, closed: u64, cause: GapCause) -> GapIncident {
        GapIncident {
            opened_epoch_us: opened,
            closed_epoch_us: closed,
            window_us: closed - opened,
            cause,
            exchange: "bitso".into(),
            symbol: "btc_mxn".into(),
            possible_dropped_trades: Some(2),
        }
    }

    #[test]
    fn appends_one_line_per_incident_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let ledger = GapLedger::new(dir.path(), "bitso", "btc_mxn");
        ledger
            .append(&incident(
                1_700_000_000_000_000,
                1_700_000_001_000_000,
                GapCause::Resync,
            ))
            .unwrap();
        // A second writer instance (fresh process) must append, not truncate.
        let again = GapLedger::new(dir.path(), "bitso", "btc_mxn");
        again
            .append(&incident(
                1_700_000_002_000_000,
                1_700_000_003_500_000,
                GapCause::Disconnect,
            ))
            .unwrap();

        let raw = std::fs::read_to_string(ledger.path()).unwrap();
        let rows: Vec<GapIncident> = raw
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].cause, GapCause::Resync);
        assert_eq!(rows[1].cause, GapCause::Disconnect);
        assert_eq!(rows[1].window_us, 1_500_000);
        assert_eq!(rows[0].possible_dropped_trades, Some(2));
    }

    #[test]
    fn estimator_is_rate_times_window_rounded_up_and_quiet_books_estimate_zero() {
        // 30 trades in the last 60s = 0.5/s; a 10s gap → ceil(5) = 5.
        assert_eq!(estimate_possible_dropped(30, 10_000_000), 5);
        // 1 trade/min over a 90s window → ceil(1.5) = 2.
        assert_eq!(estimate_possible_dropped(1, 90_000_000), 2);
        // Sub-print windows round UP (never report "0 possible" for a real
        // window on an active book).
        assert_eq!(estimate_possible_dropped(6, 1_000_000), 1);
        // A quiet book honestly estimates zero.
        assert_eq!(estimate_possible_dropped(0, 600_000_000), 0);
        // Zero-width windows estimate zero.
        assert_eq!(estimate_possible_dropped(100, 0), 0);
    }

    #[test]
    fn symbol_separators_are_path_safe() {
        let dir = tempfile::tempdir().unwrap();
        let ledger = GapLedger::new(dir.path(), "coinbase", "BTC/USD");
        assert!(
            ledger
                .path()
                .file_name()
                .unwrap()
                .to_string_lossy()
                .contains("BTC-USD")
        );
    }
}
