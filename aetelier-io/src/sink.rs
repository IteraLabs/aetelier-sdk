//! Concrete [`SnapshotFlusher`](aetelier_connect::workers::SnapshotFlusher) implementation for Parquet output.
//!
//! This module is available when the `connect` and `parquet` features are
//! both enabled. It provides [`ParquetSnapshotFlusher`], which implements
//! the [`SnapshotFlusher`](aetelier_connect::workers::SnapshotFlusher) trait
//! from `aetelier-connect`, allowing workers to persist snapshots to Parquet
//! files without `aetelier-connect` depending on `arrow`/`parquet` directly.

use aetelier_connect::workers::{FlushReport, SnapshotFlusher};
use aetelier_types::errors::PersistError;
use aetelier_types::snapshots::MarketSnapshot;

/// Flushes [`MarketSnapshot`] batches to per-datatype Parquet files.
///
/// Decomposes snapshots into orderbooks, trades, liquidations, funding
/// rates, open interest, and funding settlements, then writes each to a
/// timestamped Parquet file in a subdirectory of the configured output
/// path. Every file lands through the atomic finalize path (staged write,
/// fsync, rename, directory fsync) and is recorded in that leaf's
/// append-only `index.jsonl` (see [`crate::leaf_index`]), so a file a
/// reader can see is complete, hashed, and accounted for.
///
/// Returns a [`FlushReport`] with the total bytes and files written,
/// so that `BufferedSink` can track cumulative I/O for dashboard status.
///
/// # Usage
///
/// ```rust,ignore
/// use aetelier_io::sink::ParquetSnapshotFlusher;
/// use aetelier_connect::workers::{BufferedSink, OutputSink};
///
/// let flusher = ParquetSnapshotFlusher;
/// let sink = BufferedSink::new("./output".into(), Box::new(flusher));
/// ```
pub struct ParquetSnapshotFlusher;

/// Stat a just-written file and return its size in bytes. The write itself
/// succeeded, so a stat failure here is a real filesystem fault — it
/// propagates instead of masquerading as an empty file in the FlushReport.
fn file_bytes(path: &std::path::Path) -> Result<u64, PersistError> {
    Ok(std::fs::metadata(path)?.len())
}

/// Writes one datatype's batch through the atomic finalize path: staged
/// write, fsync, rename into the leaf, leaf-directory fsync, then the
/// `index.jsonl` append. A crash before the rename leaves only `.staging`
/// residue (swept on the next flush); a crash between rename and append
/// leaves the file "pending" — present, unlisted, defined
/// (`leaf_index::pending_files`). A propagated error after finalize makes
/// the retry write a sibling file (`unique_path` suffix) rather than
/// overwrite; downstream ReplacingMergeTree/FINAL ingest dedups, as before.
fn persist_leaf<F>(
    output_path: &std::path::Path,
    datatype: &str,
    rows: u64,
    t_min_us: u64,
    t_max_us: u64,
    write: F,
) -> Result<u64, PersistError>
where
    F: FnOnce(&std::path::Path) -> Result<std::path::PathBuf, PersistError>,
{
    let leaf = output_path.join(datatype);
    std::fs::create_dir_all(&leaf)?;
    crate::leaf_index::acquire_leaf_lock(&leaf)?;
    crate::leaf_index::sweep_staging(&leaf);
    let staging = leaf.join(crate::leaf_index::STAGING_DIR);
    std::fs::create_dir_all(&staging)?;
    let staged = write(&staging)?;
    let path = crate::leaf_index::finalize_into(&leaf, &staged)?;
    let filename = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default()
        .to_string();
    let sha256 = crate::leaf_index::sha256_file(&path)?;
    let bytes = file_bytes(&path)?;
    crate::leaf_index::append_entry(
        &leaf,
        &crate::leaf_index::IndexEntry {
            filename,
            sha256,
            rows,
            t_min_us,
            t_max_us,
            schema_id: format!("{datatype}:sync:1"),
        },
    )?;
    Ok(bytes)
}

impl SnapshotFlusher for ParquetSnapshotFlusher {
    fn flush_snapshots(
        &self,
        snapshots: &[MarketSnapshot],
        output_dir: &str,
    ) -> Result<FlushReport, PersistError> {
        if snapshots.is_empty() {
            return Ok(FlushReport::default());
        }

        let (t_min_us, t_max_us) =
            snapshots.iter().fold((u64::MAX, 0u64), |(lo, hi), s| {
                (lo.min(s.ts_us), hi.max(s.ts_us))
            });

        let crate::snapshots::DecomposedSnapshots {
            orderbooks,
            trades,
            liquidations,
            funding_rates,
            open_interests,
            funding_settlements,
        } = crate::snapshots::decompose_snapshots(snapshots);

        let output_path = std::path::Path::new(output_dir);
        let mut total_bytes: u64 = 0;
        let mut total_files: u32 = 0;

        // All-or-nothing: a write failure propagates (`?`) instead of being
        // swallowed, so the caller (`BufferedSink`) retains the buffer and
        // retries rather than treating a lost batch as flushed. Within each
        // datatype the finalize path makes the file complete-or-absent in
        // the leaf (see `persist_leaf`), so a mid-sequence failure never
        // leaves a partial file where readers look.
        if !orderbooks.is_empty() {
            total_bytes += persist_leaf(
                output_path,
                "orderbooks",
                orderbooks.len() as u64,
                t_min_us,
                t_max_us,
                |staging| {
                    crate::orderbooks::write_ob_parquet(&orderbooks, staging, "sync")
                },
            )?;
            total_files += 1;
        }
        if !trades.is_empty() {
            total_bytes += persist_leaf(
                output_path,
                "trades",
                trades.len() as u64,
                t_min_us,
                t_max_us,
                |staging| {
                    crate::trades::write_trades_parquet_timestamped(
                        &trades, staging, "sync",
                    )
                },
            )?;
            total_files += 1;
        }
        if !liquidations.is_empty() {
            total_bytes += persist_leaf(
                output_path,
                "liquidations",
                liquidations.len() as u64,
                t_min_us,
                t_max_us,
                |staging| {
                    crate::liquidations::write_liquidations_parquet_timestamped(
                        &liquidations,
                        staging,
                        "sync",
                    )
                },
            )?;
            total_files += 1;
        }
        if !funding_rates.is_empty() {
            total_bytes += persist_leaf(
                output_path,
                "fundings",
                funding_rates.len() as u64,
                t_min_us,
                t_max_us,
                |staging| {
                    crate::funding::write_funding_parquet_timestamped(
                        &funding_rates,
                        staging,
                        "sync",
                    )
                },
            )?;
            total_files += 1;
        }
        if !open_interests.is_empty() {
            total_bytes += persist_leaf(
                output_path,
                "open_interests",
                open_interests.len() as u64,
                t_min_us,
                t_max_us,
                |staging| {
                    crate::open_interest::write_oi_parquet_timestamped(
                        &open_interests,
                        staging,
                        "sync",
                    )
                },
            )?;
            total_files += 1;
        }
        if !funding_settlements.is_empty() {
            total_bytes += persist_leaf(
                output_path,
                "funding_settlements",
                funding_settlements.len() as u64,
                t_min_us,
                t_max_us,
                |staging| {
                    crate::funding::write_funding_settlement_parquet_timestamped(
                        &funding_settlements,
                        staging,
                        "sync",
                    )
                },
            )?;
            total_files += 1;
        }

        Ok(FlushReport::new(total_bytes, total_files))
    }
}
