//! Atomic-finalize and leaf-index behavior of `ParquetSnapshotFlusher`:
//! complete-or-absent files, the append-only `index.jsonl`, the
//! single-writer lock, the defined "pending" crash state, and the staging
//! orphan sweep.

use aetelier_connect::workers::SnapshotFlusher;
use aetelier_io::leaf_index;
use aetelier_io::sink::ParquetSnapshotFlusher;
use aetelier_types::TradeSide;
use aetelier_types::orderbooks::f64_to_decimal;
use aetelier_types::snapshots::MarketSnapshot;
use aetelier_types::trades::Trade;
use aetelier_types::trading_pair::TradingPair;
use tempfile::tempdir;

fn snapshot_with_trade(ts_us: u64) -> MarketSnapshot {
    let mut snap = MarketSnapshot::empty(ts_us);
    snap.trades.push(Trade {
        source_trade_ts_us: ts_us,
        local_trade_ts_us: 0,
        source_trade_rtt_us: 0,
        pair: TradingPair::new("BTC", "USDT"),
        side: TradeSide::Buy,
        amount: f64_to_decimal(0.001),
        price: f64_to_decimal(23536.30),
        exchange: "kraken".to_string(),
        id: format!("kraken_{ts_us}"),
        origin: Default::default(),
    });
    snap
}

#[test]
fn flush_finalizes_hashes_and_indexes_every_file() {
    let dir = tempdir().unwrap();
    let snaps = vec![
        snapshot_with_trade(1_700_000_000_000_000),
        snapshot_with_trade(1_700_000_000_250_000),
    ];
    let report = ParquetSnapshotFlusher
        .flush_snapshots(&snaps, dir.path().to_str().unwrap())
        .unwrap();
    assert_eq!(report.files_written, 1);

    let leaf = dir.path().join("trades");
    let (entries, torn) = leaf_index::read_index(&leaf).unwrap();
    assert_eq!(torn, 0);
    assert_eq!(entries.len(), 1);
    let entry = &entries[0];
    assert_eq!(entry.rows, 2);
    assert_eq!(entry.t_min_us, 1_700_000_000_000_000);
    assert_eq!(entry.t_max_us, 1_700_000_000_250_000);
    assert_eq!(entry.schema_id, "trades:sync:1");
    assert_eq!(
        entry.sha256,
        leaf_index::sha256_file(&leaf.join(&entry.filename)).unwrap()
    );
    assert!(leaf_index::pending_files(&leaf).unwrap().is_empty());
    let staging = leaf.join(leaf_index::STAGING_DIR);
    assert_eq!(std::fs::read_dir(&staging).unwrap().count(), 0);
}

#[test]
fn unlisted_file_classifies_pending_never_silent() {
    let dir = tempdir().unwrap();
    ParquetSnapshotFlusher
        .flush_snapshots(
            &[snapshot_with_trade(1_700_000_001_000_000)],
            dir.path().to_str().unwrap(),
        )
        .unwrap();
    let leaf = dir.path().join("trades");
    let (entries, _) = leaf_index::read_index(&leaf).unwrap();
    let listed = leaf.join(&entries[0].filename);
    let planted = leaf.join("kraken_BTC-USDT_trades_sync_planted.parquet");
    std::fs::copy(&listed, &planted).unwrap();

    assert_eq!(
        leaf_index::pending_files(&leaf).unwrap(),
        vec!["kraken_BTC-USDT_trades_sync_planted.parquet".to_string()]
    );
}

#[test]
fn legacy_leaf_without_index_has_nothing_pending() {
    let dir = tempdir().unwrap();
    let leaf = dir.path().join("trades");
    std::fs::create_dir_all(&leaf).unwrap();
    std::fs::write(leaf.join("legacy.parquet"), b"legacy bytes").unwrap();

    assert!(leaf_index::pending_files(&leaf).unwrap().is_empty());
    let (entries, torn) = leaf_index::read_index(&leaf).unwrap();
    assert!(entries.is_empty());
    assert_eq!(torn, 0);
}

#[test]
fn staging_orphans_are_swept_on_the_next_flush() {
    let dir = tempdir().unwrap();
    let staging = dir.path().join("trades").join(leaf_index::STAGING_DIR);
    std::fs::create_dir_all(&staging).unwrap();
    let orphan = staging.join("crashed_flush_leftover.parquet");
    std::fs::write(&orphan, b"torn write").unwrap();

    ParquetSnapshotFlusher
        .flush_snapshots(
            &[snapshot_with_trade(1_700_000_002_000_000)],
            dir.path().to_str().unwrap(),
        )
        .unwrap();

    assert!(!orphan.exists());
    let leaf = dir.path().join("trades");
    assert!(leaf_index::pending_files(&leaf).unwrap().is_empty());
}

#[test]
fn second_writer_on_the_same_leaf_is_refused() {
    use fs4::fs_std::FileExt;

    let dir = tempdir().unwrap();
    ParquetSnapshotFlusher
        .flush_snapshots(
            &[snapshot_with_trade(1_700_000_003_000_000)],
            dir.path().to_str().unwrap(),
        )
        .unwrap();

    let lock_path = dir.path().join("trades").join(leaf_index::LOCK_FILE);
    let foreign = std::fs::OpenOptions::new()
        .write(true)
        .open(&lock_path)
        .unwrap();
    assert!(
        !foreign.try_lock_exclusive().unwrap(),
        "the flusher must still hold the leaf's exclusive flock"
    );
}

#[test]
fn torn_index_tail_is_counted_not_fatal() {
    let dir = tempdir().unwrap();
    ParquetSnapshotFlusher
        .flush_snapshots(
            &[snapshot_with_trade(1_700_000_004_000_000)],
            dir.path().to_str().unwrap(),
        )
        .unwrap();
    let leaf = dir.path().join("trades");
    let index = leaf.join(leaf_index::INDEX_FILE);
    let mut bytes = std::fs::read(&index).unwrap();
    bytes.extend_from_slice(b"{\"filename\":\"torn");
    std::fs::write(&index, bytes).unwrap();

    let (entries, torn) = leaf_index::read_index(&leaf).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(torn, 1);
}
