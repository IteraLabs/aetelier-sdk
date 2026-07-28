//! `validate` — post-hoc parquet integrity & invariant checker for the
//! `md_worker` data lake.
//!
//! Walks `--data-dir` (defaults to `/data`) looking for the layout produced
//! by `BufferedSink + ParquetSnapshotFlusher`:
//!
//! ```text
//! /data
//! ├── binance/
//! │   ├── orderbooks/<SYMBOL>_ob_sync_<ts>.parquet
//! │   └── trades/<SYMBOL>_trades_sync_<ts>.parquet
//! ├── coinbase/...
//! └── kraken/...
//! ```
//!
//! For every parquet file it runs a battery of 10 tests, then writes an
//! aggregated report to stdout (human-readable) and to
//! `--report-out` (JSON), and merges the run into a persistent
//! `--state-file` so cumulative statistics survive across runs.
//!
//! The tests cover three orthogonal concerns:
//!
//! * **structure** — files exist per exchange/datatype, are non-empty,
//!   match the expected filename convention, and have rows.
//! * **temporal correctness** — timestamps are non-decreasing, the
//!   `last_ts − first_ts` span lines up with `flush_threshold × grid`
//!   within tolerance, and adjacent files for the same symbol do not
//!   overlap in time.
//! * **content correctness** — orderbook snapshots are not crossed
//!   (best bid < best ask), prices and sizes are strictly positive,
//!   trade IDs are unique within a file, and orderbook snapshot
//!   timestamps are unique.
//!
//! Exit code is `0` if every test passed, `1` if any test failed, `2`
//! on a fatal error (e.g. unreadable state file). Designed to be run
//! periodically from cron — see `serve/validate.Dockerfile`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use anyhow::{Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use tracing_subscriber::EnvFilter;

use aetelier_io::orderbooks::read_ob_parquet;
use aetelier_io::trades::read_trades_parquet;
use aetelier_types::orderbooks::{Orderbook, OrderbookTarget, OrderbookTargetData};
use aetelier_types::trades::Trade;

// ─────────────────────────────────────────────────────────────────────────────
// CLI
// ─────────────────────────────────────────────────────────────────────────────

/// Validate parquet files produced by `md_worker`.
#[derive(Parser, Debug)]
#[command(name = "validate", version, about)]
struct Cli {
    /// Root directory with the per-exchange parquet output.
    #[arg(long, default_value = "/data")]
    data_dir: PathBuf,

    /// Persistent state file used for cumulative statistics across runs.
    #[arg(long, default_value = "/state/validate_state.json")]
    state_file: PathBuf,

    /// Per-run JSON report.
    #[arg(long, default_value = "/state/last_report.json")]
    report_out: PathBuf,

    /// `flush_threshold` from the manifest (number of grid ticks per
    /// flush). Used to compute the expected file time span.
    #[arg(long, default_value_t = 3600)]
    flush_threshold: u64,

    /// Grid period in milliseconds. Combine with `flush_threshold` to
    /// compute `expected_span_ms = flush_threshold * grid_period_ms`.
    #[arg(long, default_value_t = 100)]
    grid_period_ms: u64,

    /// Tolerance for the flush-span test, as a fraction of the
    /// expected span (default 0.20 → ±20 %).
    #[arg(long, default_value_t = 0.20)]
    span_tolerance: f64,

    /// Exchanges expected to have data (used for the "files present"
    /// test). Comma-separated; defaults to the docker-compose set.
    #[arg(long, default_value = "binance,coinbase,kraken", value_delimiter = ',')]
    expect_exchanges: Vec<String>,

    /// Print the per-file detail table in addition to the summary.
    #[arg(long)]
    verbose: bool,

    /// Skip files that have already been validated in a prior run
    /// (matched by absolute path + size). Useful when running on a
    /// rolling cadence over a growing data lake.
    #[arg(long)]
    skip_seen: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
// Test catalogue
// ─────────────────────────────────────────────────────────────────────────────

/// Stable IDs for the 10 invariants the validator enforces.
///
/// Stored verbatim in the state file so historical runs remain
/// interpretable as the test suite evolves.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Hash, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TestId {
    /// T01: every expected `<exchange>/<kind>/` directory has ≥1 file.
    FilesPresent,
    /// T02: each parquet file is non-empty (size > 0, ≥1 row).
    FileNonEmpty,
    /// T03: filename matches `<SYMBOL>_<kind>_sync_<ts>.parquet`.
    FilenameConvention,
    /// T04: distinct timestamps within a file are non-decreasing.
    MonotonicTimestamps,
    /// T05: `last_ts − first_ts` ≈ `flush_threshold × grid` within tolerance.
    FlushSpan,
    /// T06: orderbook snapshot timestamps are unique within a file.
    UniqueOrderbookTs,
    /// T07: trade IDs are unique within a file.
    UniqueTradeIds,
    /// T08: every orderbook snapshot satisfies `best_bid < best_ask`.
    NotCrossedOrderbook,
    /// T09: every price and size is strictly positive.
    PositivePricesSizes,
    /// T10: adjacent files for the same `(exchange, symbol, kind)` do
    /// not overlap in time.
    NoTemporalOverlap,
}

impl TestId {
    fn code(&self) -> &'static str {
        match self {
            TestId::FilesPresent => "T01",
            TestId::FileNonEmpty => "T02",
            TestId::FilenameConvention => "T03",
            TestId::MonotonicTimestamps => "T04",
            TestId::FlushSpan => "T05",
            TestId::UniqueOrderbookTs => "T06",
            TestId::UniqueTradeIds => "T07",
            TestId::NotCrossedOrderbook => "T08",
            TestId::PositivePricesSizes => "T09",
            TestId::NoTemporalOverlap => "T10",
        }
    }

    fn label(&self) -> &'static str {
        match self {
            TestId::FilesPresent => "files_present_per_exchange",
            TestId::FileNonEmpty => "file_non_empty",
            TestId::FilenameConvention => "filename_convention",
            TestId::MonotonicTimestamps => "monotonic_timestamps",
            TestId::FlushSpan => "flush_span_within_tolerance",
            TestId::UniqueOrderbookTs => "unique_orderbook_timestamps",
            TestId::UniqueTradeIds => "unique_trade_ids",
            TestId::NotCrossedOrderbook => "no_crossed_orderbook",
            TestId::PositivePricesSizes => "positive_prices_and_sizes",
            TestId::NoTemporalOverlap => "no_temporal_overlap_between_files",
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-file model
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum DataKind {
    Orderbook,
    Trades,
}

impl DataKind {
    fn dir_name(&self) -> &'static str {
        match self {
            DataKind::Orderbook => "orderbooks",
            DataKind::Trades => "trades",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct FileReport {
    path: String,
    exchange: String,
    symbol: String,
    kind: DataKind,
    size_bytes: u64,
    row_count: u64,
    distinct_ts_count: u64,
    first_ts: u64,
    last_ts: u64,
    span_ms: u64,
    failed: Vec<&'static str>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Cumulative state (persisted across runs)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Serialize, Deserialize)]
struct State {
    /// Append-only run history.
    runs: Vec<RunSummary>,
    /// Cumulative counters across all runs since `state_file` was created.
    cumulative: Cumulative,
    /// `path → size_bytes` of files we have already validated.  Used by
    /// `--skip-seen` and for de-dup of cumulative byte counts.
    known_files: HashMap<String, u64>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct RunSummary {
    run_id: u64,
    started_at_unix_ms: u64,
    ended_at_unix_ms: u64,
    files_seen: u64,
    new_files: u64,
    bytes_seen: u64,
    total_orderbook_snapshots: u64,
    total_trades: u64,
    tests_passed: u64,
    tests_failed: u64,
    failed_test_codes: Vec<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Cumulative {
    runs: u64,
    distinct_files: u64,
    bytes: u64,
    orderbook_snapshots: u64,
    trades: u64,
    per_exchange: BTreeMap<String, ExchangeStats>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ExchangeStats {
    files: u64,
    bytes: u64,
    orderbook_snapshots: u64,
    trades: u64,
}

// ─────────────────────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Per-(exchange, symbol, kind) accumulator of (rows, span_ms, filename).
type PerGroupRows = BTreeMap<(String, String, DataKind), Vec<(u64, u64, String)>>;

/// `(row_count, distinct_ts, first_ts, last_ts, monotonic, failed_codes)`.
type FileValidation = (u64, u64, u64, u64, bool, Vec<&'static str>);

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();
    let started_at_unix_ms = unix_ms();

    let mut state = load_state(&cli.state_file).context("loading state file")?;
    let run_id = state.cumulative.runs + 1;

    let exchanges = discover_exchanges(&cli.data_dir, &cli.expect_exchanges);
    let mut t01_failures: Vec<String> = Vec::new();
    for ex in &cli.expect_exchanges {
        for kind in [DataKind::Orderbook, DataKind::Trades] {
            let dir = cli.data_dir.join(ex).join(kind.dir_name());
            let count = parquet_files_in(&dir).len();
            if count == 0 {
                t01_failures.push(format!(
                    "{}/{}: 0 .parquet files",
                    ex,
                    kind.dir_name()
                ));
            }
        }
    }

    // ── Walk every file and build per-file reports ───────────────────────
    let mut file_reports: Vec<FileReport> = Vec::new();
    let mut new_files = 0u64;
    let mut bytes_seen = 0u64;
    let mut total_ob_snapshots = 0u64;
    let mut total_trades = 0u64;

    // For T10: group end-state per (exchange, symbol, kind).
    let mut per_group: PerGroupRows = BTreeMap::new();

    for ex in &exchanges {
        for kind in [DataKind::Orderbook, DataKind::Trades] {
            let dir = cli.data_dir.join(ex).join(kind.dir_name());
            for path in parquet_files_in(&dir) {
                let path_str = path.to_string_lossy().into_owned();
                let size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

                let already_seen =
                    state.known_files.get(&path_str).copied() == Some(size);
                if cli.skip_seen && already_seen {
                    continue;
                }

                if !already_seen {
                    new_files += 1;
                }
                bytes_seen += size;

                let report = match validate_file(&path, ex, kind, size, &cli) {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::error!(
                            file = %path.display(),
                            error = %e,
                            "validate.read_failed"
                        );
                        // A read failure is itself a test failure — file_non_empty
                        // can't be confirmed, and the file is unreadable.
                        FileReport {
                            path: path_str.clone(),
                            exchange: ex.clone(),
                            symbol: parse_symbol_from_filename(&path).unwrap_or_default(),
                            kind,
                            size_bytes: size,
                            row_count: 0,
                            distinct_ts_count: 0,
                            first_ts: 0,
                            last_ts: 0,
                            span_ms: 0,
                            failed: vec![TestId::FileNonEmpty.code()],
                        }
                    }
                };

                if matches!(kind, DataKind::Orderbook) {
                    total_ob_snapshots += report.distinct_ts_count;
                } else {
                    total_trades += report.row_count;
                }

                per_group
                    .entry((ex.clone(), report.symbol.clone(), kind))
                    .or_default()
                    .push((report.first_ts, report.last_ts, path_str.clone()));

                state.known_files.insert(path_str, size);
                file_reports.push(report);
            }
        }
    }

    // ── T10: temporal overlap between adjacent files in same group ──────
    let mut overlap_failures: Vec<String> = Vec::new();
    for ((ex, sym, kind), mut spans) in per_group.into_iter() {
        spans.sort_by_key(|(first, _, _)| *first);
        for w in spans.windows(2) {
            let (_, prev_last, prev_path) = &w[0];
            let (curr_first, _, curr_path) = &w[1];
            if curr_first <= prev_last {
                overlap_failures.push(format!(
                    "{}/{}/{}: {} (last={}) overlaps {} (first={})",
                    ex,
                    sym,
                    kind.dir_name(),
                    file_basename(prev_path),
                    prev_last,
                    file_basename(curr_path),
                    curr_first,
                ));
            }
        }
    }

    // ── Aggregate test results ───────────────────────────────────────────
    let test_results = aggregate_results(&t01_failures, &file_reports, &overlap_failures);

    let tests_passed = test_results.iter().filter(|r| r.passed).count() as u64;
    let tests_failed = test_results.iter().filter(|r| !r.passed).count() as u64;
    let failed_codes: Vec<String> = test_results
        .iter()
        .filter(|r| !r.passed)
        .map(|r| r.id.code().to_string())
        .collect();

    // ── Update cumulative counters ──────────────────────────────────────
    state.cumulative.runs += 1;
    state.cumulative.distinct_files = state.known_files.len() as u64;
    state.cumulative.bytes = state.known_files.values().sum();
    state.cumulative.orderbook_snapshots += total_ob_snapshots;
    state.cumulative.trades += total_trades;

    for fr in &file_reports {
        let stats = state
            .cumulative
            .per_exchange
            .entry(fr.exchange.clone())
            .or_default();
        stats.files += 1;
        stats.bytes += fr.size_bytes;
        match fr.kind {
            DataKind::Orderbook => stats.orderbook_snapshots += fr.distinct_ts_count,
            DataKind::Trades => stats.trades += fr.row_count,
        }
    }

    let summary = RunSummary {
        run_id,
        started_at_unix_ms,
        ended_at_unix_ms: unix_ms(),
        files_seen: file_reports.len() as u64,
        new_files,
        bytes_seen,
        total_orderbook_snapshots: total_ob_snapshots,
        total_trades,
        tests_passed,
        tests_failed,
        failed_test_codes: failed_codes.clone(),
    };
    state.runs.push(summary.clone());

    // ── Render report ───────────────────────────────────────────────────
    print_report(
        &cli,
        &exchanges,
        &test_results,
        &file_reports,
        &state,
        &summary,
    );

    write_json(
        &cli.report_out,
        &PerRunReport {
            summary: &summary,
            tests: &test_results,
            files: &file_reports,
        },
    )
    .with_context(|| format!("writing per-run report {}", cli.report_out.display()))?;

    save_state(&cli.state_file, &state)
        .with_context(|| format!("saving state {}", cli.state_file.display()))?;

    if tests_failed > 0 {
        std::process::exit(1);
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// File-level validation
// ─────────────────────────────────────────────────────────────────────────────

fn validate_file(
    path: &Path,
    exchange: &str,
    kind: DataKind,
    size_bytes: u64,
    cli: &Cli,
) -> Result<FileReport> {
    let mut failed: Vec<&'static str> = Vec::new();

    // T03: filename convention
    let filename_ok = matches_convention(path, kind);
    if !filename_ok {
        failed.push(TestId::FilenameConvention.code());
    }
    let symbol = parse_symbol_from_filename(path).unwrap_or_default();

    // Read & dispatch
    let (row_count, distinct_ts, first_ts, last_ts, monotonic, sub_failed) = match kind {
        DataKind::Orderbook => validate_orderbook_file(path)?,
        DataKind::Trades => validate_trades_file(path)?,
    };
    failed.extend(sub_failed);

    // T02: file non-empty
    if size_bytes == 0 || row_count == 0 {
        failed.push(TestId::FileNonEmpty.code());
    }
    // T04: monotonic timestamps
    if !monotonic {
        failed.push(TestId::MonotonicTimestamps.code());
    }
    // T05: flush span
    let expected_span =
        (cli.flush_threshold.saturating_sub(1) * cli.grid_period_ms) as f64;
    let span_ms = last_ts.saturating_sub(first_ts);
    if expected_span > 0.0 {
        let lo = expected_span * (1.0 - cli.span_tolerance);
        let hi = expected_span * (1.0 + cli.span_tolerance);
        // For trade files we only assert the upper bound (trades are
        // sparse — flushes can land on files with few trades that span
        // less than the full window).
        let in_range = match kind {
            DataKind::Orderbook => (span_ms as f64) >= lo && (span_ms as f64) <= hi,
            DataKind::Trades => (span_ms as f64) <= hi,
        };
        if !in_range {
            failed.push(TestId::FlushSpan.code());
        }
    }

    Ok(FileReport {
        path: path.to_string_lossy().into_owned(),
        exchange: exchange.to_string(),
        symbol,
        kind,
        size_bytes,
        row_count,
        distinct_ts_count: distinct_ts,
        first_ts,
        last_ts,
        span_ms,
        failed,
    })
}

/// Read an orderbook parquet file and check the orderbook-specific
/// invariants (T06 unique timestamps, T08 not-crossed, T09 positive
/// prices/sizes).
///
/// Returns `(row_count, distinct_ts, first_ts, last_ts, monotonic, failed_codes)`.
fn validate_orderbook_file(path: &Path) -> Result<FileValidation> {
    let data = read_ob_parquet(path, OrderbookTarget::Snapshot)
        .map_err(|e| anyhow::anyhow!("read_ob_parquet: {e}"))?;
    let books: Vec<Orderbook> = match data {
        OrderbookTargetData::Snapshot(v) => v,
        OrderbookTargetData::Delta(_) => {
            anyhow::bail!("expected snapshot variant from read_ob_parquet")
        }
    };

    let row_count = books.len() as u64;
    if books.is_empty() {
        return Ok((0, 0, 0, 0, true, vec![]));
    }

    let first_ts = books.first().map(|o| o.orderbook_ts_us).unwrap_or(0);
    let last_ts = books.last().map(|o| o.orderbook_ts_us).unwrap_or(0);

    // T04: monotonic timestamps (allow equal — T06 handles strict uniqueness)
    let monotonic = books
        .windows(2)
        .all(|w| w[0].orderbook_ts_us <= w[1].orderbook_ts_us);

    // T06: unique timestamps
    let mut seen_ts: HashSet<u64> = HashSet::with_capacity(books.len());
    let mut dupe_ts = false;
    for ob in &books {
        if !seen_ts.insert(ob.orderbook_ts_us) {
            dupe_ts = true;
            break;
        }
    }

    // T08 + T09 in a single pass.
    let mut crossed = false;
    let mut non_positive = false;
    for ob in &books {
        let best_bid = ob.bids.last_key_value().map(|(p, _)| *p);
        let best_ask = ob.asks.first_key_value().map(|(p, _)| *p);
        if let (Some(b), Some(a)) = (best_bid, best_ask)
            && b >= a
        {
            crossed = true;
        }
        for (price, lvl) in ob.bids.iter().chain(ob.asks.iter()) {
            if *price <= rust_decimal::Decimal::ZERO
                || lvl.volume <= rust_decimal::Decimal::ZERO
            {
                non_positive = true;
                break;
            }
        }
        if crossed && non_positive {
            break;
        }
    }

    let mut failed: Vec<&'static str> = Vec::new();
    if dupe_ts {
        failed.push(TestId::UniqueOrderbookTs.code());
    }
    if crossed {
        failed.push(TestId::NotCrossedOrderbook.code());
    }
    if non_positive {
        failed.push(TestId::PositivePricesSizes.code());
    }

    Ok((
        row_count,
        seen_ts.len() as u64,
        first_ts,
        last_ts,
        monotonic,
        failed,
    ))
}

/// Read a trades parquet file and check trade-specific invariants
/// (T07 unique IDs, T09 positive prices/sizes).
fn validate_trades_file(path: &Path) -> Result<FileValidation> {
    let trades: Vec<Trade> = read_trades_parquet(path)
        .map_err(|e| anyhow::anyhow!("read_trades_parquet: {e}"))?;

    let row_count = trades.len() as u64;
    if trades.is_empty() {
        return Ok((0, 0, 0, 0, true, vec![]));
    }

    let first_ts = trades.first().map(|t| t.source_trade_ts_us).unwrap_or(0);
    let last_ts = trades.last().map(|t| t.source_trade_ts_us).unwrap_or(0);
    let monotonic = trades
        .windows(2)
        .all(|w| w[0].source_trade_ts_us <= w[1].source_trade_ts_us);

    // T07: unique IDs
    let mut seen_ids: HashSet<&str> = HashSet::with_capacity(trades.len());
    let mut dupe_id = false;
    let mut distinct_ts: HashSet<u64> = HashSet::new();
    let mut non_positive = false;
    for t in &trades {
        if !seen_ids.insert(t.id.as_str()) {
            dupe_id = true;
        }
        distinct_ts.insert(t.source_trade_ts_us);
        if !(t.price > rust_decimal::Decimal::ZERO
            && t.amount > rust_decimal::Decimal::ZERO)
        {
            non_positive = true;
        }
    }

    let mut failed: Vec<&'static str> = Vec::new();
    if dupe_id {
        failed.push(TestId::UniqueTradeIds.code());
    }
    if non_positive {
        failed.push(TestId::PositivePricesSizes.code());
    }

    Ok((
        row_count,
        distinct_ts.len() as u64,
        first_ts,
        last_ts,
        monotonic,
        failed,
    ))
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregate test results across files
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct TestOutcome {
    id: TestId,
    code: &'static str,
    label: &'static str,
    passed: bool,
    /// Detail (e.g. file paths) when the test failed.
    detail: Vec<String>,
}

fn aggregate_results(
    t01_failures: &[String],
    files: &[FileReport],
    t10_failures: &[String],
) -> Vec<TestOutcome> {
    let collect_for = |id: TestId| -> Vec<String> {
        let code = id.code();
        files
            .iter()
            .filter(|f| f.failed.contains(&code))
            .map(|f| format!("{} (sym={}, ex={})", f.path, f.symbol, f.exchange))
            .collect()
    };

    let mut out: Vec<TestOutcome> = Vec::new();

    // T01 — directory-level
    out.push(TestOutcome {
        id: TestId::FilesPresent,
        code: TestId::FilesPresent.code(),
        label: TestId::FilesPresent.label(),
        passed: t01_failures.is_empty(),
        detail: t01_failures.to_vec(),
    });

    for id in [
        TestId::FileNonEmpty,
        TestId::FilenameConvention,
        TestId::MonotonicTimestamps,
        TestId::FlushSpan,
        TestId::UniqueOrderbookTs,
        TestId::UniqueTradeIds,
        TestId::NotCrossedOrderbook,
        TestId::PositivePricesSizes,
    ] {
        let detail = collect_for(id);
        out.push(TestOutcome {
            id,
            code: id.code(),
            label: id.label(),
            passed: detail.is_empty(),
            detail,
        });
    }

    out.push(TestOutcome {
        id: TestId::NoTemporalOverlap,
        code: TestId::NoTemporalOverlap.code(),
        label: TestId::NoTemporalOverlap.label(),
        passed: t10_failures.is_empty(),
        detail: t10_failures.to_vec(),
    });

    out
}

// ─────────────────────────────────────────────────────────────────────────────
// Reporting
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct PerRunReport<'a> {
    summary: &'a RunSummary,
    tests: &'a [TestOutcome],
    files: &'a [FileReport],
}

fn print_report(
    cli: &Cli,
    exchanges: &[String],
    tests: &[TestOutcome],
    files: &[FileReport],
    state: &State,
    summary: &RunSummary,
) {
    println!("┌─────────────────────────────────────────────────────────────────────");
    println!("│ aetelier-sdk validate — run #{}", summary.run_id);
    println!("├─────────────────────────────────────────────────────────────────────");
    println!("│ data_dir          : {}", cli.data_dir.display());
    println!("│ expect_exchanges  : {}", cli.expect_exchanges.join(", "));
    println!("│ found_exchanges   : {}", exchanges.join(", "));
    println!(
        "│ flush_threshold   : {} (× {} ms grid → ~{:.1} s window)",
        cli.flush_threshold,
        cli.grid_period_ms,
        (cli.flush_threshold * cli.grid_period_ms) as f64 / 1000.0
    );
    println!("├──────────────────────── this run ──────────────────────────────────");
    println!("│ files_seen        : {}", summary.files_seen);
    println!("│ new_files         : {}", summary.new_files);
    println!("│ bytes_seen        : {}", human_bytes(summary.bytes_seen));
    println!(
        "│ ob_snapshots      : {}",
        summary.total_orderbook_snapshots
    );
    println!("│ trades            : {}", summary.total_trades);
    println!(
        "│ tests_passed      : {}/{}",
        summary.tests_passed,
        tests.len()
    );
    println!("│ tests_failed      : {}", summary.tests_failed);
    println!("├──────────────────────── tests ─────────────────────────────────────");
    for t in tests {
        let mark = if t.passed { "PASS" } else { "FAIL" };
        println!("│  [{}] {} {}", mark, t.code, t.label);
        if !t.passed {
            for d in t.detail.iter().take(5) {
                println!("│         · {}", d);
            }
            if t.detail.len() > 5 {
                println!("│         · … and {} more", t.detail.len() - 5);
            }
        }
    }

    if cli.verbose {
        println!("├──────────────────────── per-file ──────────────────────────────────");
        println!(
            "│  {:<8} {:<10} {:<8} {:>10} {:>8} {:>14} failed",
            "exchange", "symbol", "kind", "rows", "span_ms", "size"
        );
        for f in files {
            println!(
                "│  {:<8} {:<10} {:<8} {:>10} {:>8} {:>14} {}",
                f.exchange,
                f.symbol,
                f.kind.dir_name(),
                f.row_count,
                f.span_ms,
                human_bytes(f.size_bytes),
                if f.failed.is_empty() {
                    "·".to_string()
                } else {
                    f.failed.join(",")
                }
            );
        }
    }

    println!("├──────────────────────── cumulative ────────────────────────────────");
    println!("│ total_runs        : {}", state.cumulative.runs);
    println!("│ distinct_files    : {}", state.cumulative.distinct_files);
    println!(
        "│ total_bytes       : {}",
        human_bytes(state.cumulative.bytes)
    );
    println!(
        "│ ob_snapshots      : {}",
        state.cumulative.orderbook_snapshots
    );
    println!("│ trades            : {}", state.cumulative.trades);
    for (ex, s) in &state.cumulative.per_exchange {
        println!(
            "│   {:<8} files={:<5} bytes={:<10} ob={:<7} trades={}",
            ex,
            s.files,
            human_bytes(s.bytes),
            s.orderbook_snapshots,
            s.trades,
        );
    }
    println!("└─────────────────────────────────────────────────────────────────────");
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).ok();
    }
    let json = serde_json::to_string_pretty(value)?;
    fs::write(path, json)?;
    Ok(())
}

fn load_state(path: &Path) -> Result<State> {
    if !path.exists() {
        return Ok(State::default());
    }
    let bytes = fs::read(path)?;
    if bytes.is_empty() {
        return Ok(State::default());
    }
    let state: State = serde_json::from_slice(&bytes)?;
    Ok(state)
}

fn save_state(path: &Path, state: &State) -> Result<()> {
    write_json(path, state)
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn discover_exchanges(root: &Path, expected: &[String]) -> Vec<String> {
    let mut found: Vec<String> = Vec::new();
    if let Ok(rd) = fs::read_dir(root) {
        for e in rd.flatten() {
            if e.file_type().map(|t| t.is_dir()).unwrap_or(false)
                && let Some(name) = e.file_name().to_str()
                && expected.iter().any(|x| x == name)
            {
                found.push(name.to_string());
            }
        }
    }
    found.sort();
    found
}

fn parquet_files_in(dir: &Path) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = Vec::new();
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
                out.push(p);
            }
        }
    }
    out.sort();
    out
}

/// Filename convention: `<SYMBOL>_<TAG>_sync_<TS>.parquet`, with TAG in
/// {`ob`, `trades`, `liquidations`, `funding`, `oi`}.  The first two
/// are what `md_worker` actively writes today; the others are
/// validated for free if/when their feed is enabled.
fn matches_convention(path: &Path, kind: DataKind) -> bool {
    let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
        return false;
    };
    let Some(stem) = name.strip_suffix(".parquet") else {
        return false;
    };
    let parts: Vec<&str> = stem.split('_').collect();
    if parts.len() < 4 {
        return false;
    }
    if parts[parts.len() - 2] != "sync" {
        return false;
    }
    let tag_idx = parts.len() - 3;
    let tag = parts[tag_idx];
    match kind {
        DataKind::Orderbook => tag == "ob",
        DataKind::Trades => tag == "trades",
    }
}

fn parse_symbol_from_filename(path: &Path) -> Option<String> {
    let name = path.file_name()?.to_str()?;
    let stem = name.strip_suffix(".parquet")?;
    let parts: Vec<&str> = stem.split('_').collect();
    if parts.len() < 4 {
        return None;
    }
    // Symbol is everything before the `<tag>_sync_<ts>` suffix.
    let tag_idx = parts.len() - 3;
    Some(parts[..tag_idx].join("_"))
}

fn file_basename(path: &str) -> String {
    Path::new(path)
        .file_name()
        .and_then(|s| s.to_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| path.to_string())
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn human_bytes(n: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut v = n as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{} {}", n, UNITS[0])
    } else {
        format!("{:.2} {}", v, UNITS[i])
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn convention_orderbook() {
        let p = PathBuf::from(
            "/data/binance/orderbooks/BTCUSDC_ob_sync_1700000000000.parquet",
        );
        assert!(matches_convention(&p, DataKind::Orderbook));
        assert!(!matches_convention(&p, DataKind::Trades));
        assert_eq!(parse_symbol_from_filename(&p).as_deref(), Some("BTCUSDC"));
    }

    #[test]
    fn convention_trades_with_dashes() {
        let p = PathBuf::from(
            "/data/coinbase/trades/BTC-USDC_trades_sync_1700000000000.parquet",
        );
        assert!(matches_convention(&p, DataKind::Trades));
        assert!(!matches_convention(&p, DataKind::Orderbook));
        assert_eq!(parse_symbol_from_filename(&p).as_deref(), Some("BTC-USDC"));
    }

    #[test]
    fn convention_rejects_missing_sync_marker() {
        let p =
            PathBuf::from("/data/binance/orderbooks/BTCUSDC_ob_1700000000000.parquet");
        assert!(!matches_convention(&p, DataKind::Orderbook));
    }
}
