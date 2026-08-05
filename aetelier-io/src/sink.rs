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
/// rates, and open interest, then writes each to a timestamped Parquet
/// file in a subdirectory of the configured output path.
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

impl SnapshotFlusher for ParquetSnapshotFlusher {
    fn flush_snapshots(
        &self,
        snapshots: &[MarketSnapshot],
        output_dir: &str,
    ) -> Result<FlushReport, PersistError> {
        if snapshots.is_empty() {
            return Ok(FlushReport::default());
        }

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
        // retries rather than treating a lost batch as flushed. A failed
        // datatype earlier in the sequence may leave a partial file; the retry
        // re-writes it and the ReplacingMergeTree/FINAL ingest dedups on
        // re-ingest, so no rows are corrupted.
        if !orderbooks.is_empty() {
            let dir = output_path.join("orderbooks");
            std::fs::create_dir_all(&dir)?;
            let path = crate::orderbooks::write_ob_parquet(&orderbooks, &dir, "sync")?;
            total_bytes += file_bytes(&path)?;
            total_files += 1;
        }
        if !trades.is_empty() {
            let dir = output_path.join("trades");
            std::fs::create_dir_all(&dir)?;
            let path =
                crate::trades::write_trades_parquet_timestamped(&trades, &dir, "sync")?;
            total_bytes += file_bytes(&path)?;
            total_files += 1;
        }
        if !liquidations.is_empty() {
            let dir = output_path.join("liquidations");
            std::fs::create_dir_all(&dir)?;
            let path = crate::liquidations::write_liquidations_parquet_timestamped(
                &liquidations,
                &dir,
                "sync",
            )?;
            total_bytes += file_bytes(&path)?;
            total_files += 1;
        }
        if !funding_rates.is_empty() {
            let dir = output_path.join("fundings");
            std::fs::create_dir_all(&dir)?;
            let path = crate::funding::write_funding_parquet_timestamped(
                &funding_rates,
                &dir,
                "sync",
            )?;
            total_bytes += file_bytes(&path)?;
            total_files += 1;
        }
        if !open_interests.is_empty() {
            let dir = output_path.join("open_interests");
            std::fs::create_dir_all(&dir)?;
            let path = crate::open_interest::write_oi_parquet_timestamped(
                &open_interests,
                &dir,
                "sync",
            )?;
            total_bytes += file_bytes(&path)?;
            total_files += 1;
        }
        if !funding_settlements.is_empty() {
            let dir = output_path.join("funding_settlements");
            std::fs::create_dir_all(&dir)?;
            let path = crate::funding::write_funding_settlement_parquet_timestamped(
                &funding_settlements,
                &dir,
                "sync",
            )?;
            total_bytes += file_bytes(&path)?;
            total_files += 1;
        }

        Ok(FlushReport::new(total_bytes, total_files))
    }
}
