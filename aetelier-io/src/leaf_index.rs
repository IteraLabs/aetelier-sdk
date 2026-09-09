//! Per-leaf write-side integrity index and the atomic parquet finalize path.
//!
//! A leaf is one datatype directory of the sink layout (`<output_dir>/trades`,
//! `<output_dir>/orderbooks`, …). Each leaf carries:
//!
//! - `index.jsonl` — one appended JSON line per finalized parquet file:
//!   `{filename, sha256, rows, t_min_us, t_max_us, schema_id}`. Append-only,
//!   `O_APPEND` + fsync per line; the file is never rewritten, so a torn run
//!   loses at most the in-flight line (the gap-ledger durability model,
//!   DAT-SK-INV-20).
//! - `.staging/` — where a parquet file is written before it is fsynced,
//!   renamed beside its siblings, and the leaf directory fsynced. A file in
//!   the leaf is therefore complete or absent; a crash between rename and
//!   index append leaves it PRESENT BUT UNLISTED — the defined "pending"
//!   state ([`crate::leaf_index::pending_files`]), never silent data.
//! - `.index.lock` — an advisory `flock`, taken exclusively once per process
//!   and held for the process lifetime: one writer per leaf, enforced loudly
//!   instead of last-rename-wins.
//!
//! Absence of `index.jsonl` marks a legacy leaf; every reader tolerates it.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use aetelier_types::errors::PersistError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Index filename inside a leaf.
pub const INDEX_FILE: &str = "index.jsonl";
/// Staging subdirectory a parquet file is written into before finalize.
pub const STAGING_DIR: &str = ".staging";
/// Advisory lock file enforcing single-writer per leaf.
pub const LOCK_FILE: &str = ".index.lock";

/// One finalized parquet file, as recorded in `index.jsonl`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexEntry {
    /// Basename of the parquet file inside the leaf.
    pub filename: String,
    /// SHA-256 of the finalized file bytes, lowercase hex, hashed after the
    /// rename so it attests what is actually on disk.
    pub sha256: String,
    /// Rows written into the file.
    pub rows: u64,
    /// Grid coverage of the flushed batch (UTC epoch microseconds): the
    /// smallest snapshot `ts_us` in the batch, not per-event bounds (parquet
    /// footer statistics stay the exact per-event source).
    pub t_min_us: u64,
    /// Largest snapshot `ts_us` in the flushed batch.
    pub t_max_us: u64,
    /// Registry-minted schema id (`<datatype>.v1`, gticket_0038 D5/D7); the
    /// registry row for it carries the parquet-schema fingerprint.
    pub schema_id: String,
}

fn held_locks() -> &'static Mutex<HashMap<PathBuf, File>> {
    static HELD: OnceLock<Mutex<HashMap<PathBuf, File>>> = OnceLock::new();
    HELD.get_or_init(|| Mutex::new(HashMap::new()))
}

fn try_exclusive_flock(file: &File) -> std::io::Result<bool> {
    use fs4::fs_std::FileExt;
    file.try_lock_exclusive()
}

/// Takes the leaf's writer lock, once per process, held until process exit.
/// A second process contending for the same leaf errors loudly here — the
/// lost-update alternative (two writers, last rename wins) is never allowed
/// to happen silently.
pub fn acquire_leaf_lock(leaf: &Path) -> Result<(), PersistError> {
    let mut held = held_locks().lock().expect("leaf lock registry poisoned");
    if held.contains_key(leaf) {
        return Ok(());
    }
    let lock_path = leaf.join(LOCK_FILE);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)?;
    if !try_exclusive_flock(&file)? {
        return Err(PersistError::Parse(format!(
            "leaf {} already has a writer (flock on {} held elsewhere)",
            leaf.display(),
            LOCK_FILE
        )));
    }
    held.insert(leaf.to_path_buf(), file);
    Ok(())
}

/// Removes leftovers a crashed flush stranded in `.staging`, warning with
/// the count. Safe on every flush: staging is exclusively this process's
/// (the leaf lock is already held) and is emptied by each finalize.
pub fn sweep_staging(leaf: &Path) {
    let staging = leaf.join(STAGING_DIR);
    let Ok(entries) = fs::read_dir(&staging) else {
        return;
    };
    let mut swept: u32 = 0;
    for entry in entries.flatten() {
        if entry.path().is_file() && fs::remove_file(entry.path()).is_ok() {
            swept += 1;
        }
    }
    if swept > 0 {
        tracing::warn!(
            leaf = %leaf.display(),
            orphans = swept,
            "leaf_index.staging_orphans_swept"
        );
    }
}

/// Moves a staged parquet file into the leaf: fsync the file, rename it
/// beside its siblings (a name collision gets the `unique_path` suffix, so
/// nothing is overwritten), fsync the leaf directory so the rename itself
/// survives power loss.
pub fn finalize_into(leaf: &Path, staged: &Path) -> Result<PathBuf, PersistError> {
    let name = staged.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
        PersistError::Parse(format!("staged path {} has no filename", staged.display()))
    })?;
    File::open(staged)?.sync_all()?;
    let target = crate::naming::unique_path(leaf, name);
    fs::rename(staged, &target)?;
    File::open(leaf)?.sync_all()?;
    Ok(target)
}

/// SHA-256 of a file's bytes, lowercase hex.
pub fn sha256_file(path: &Path) -> Result<String, PersistError> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

/// Appends one entry to the leaf's `index.jsonl` (`O_APPEND` + fsync). The
/// leaf lock must already be held by this process.
pub fn append_entry(leaf: &Path, entry: &IndexEntry) -> Result<(), PersistError> {
    let mut line = serde_json::to_string(entry)?;
    line.push('\n');
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(leaf.join(INDEX_FILE))?;
    file.write_all(line.as_bytes())?;
    file.sync_all()?;
    Ok(())
}

/// Reads a leaf's index. Returns the parsed entries plus the count of torn
/// (unparseable) lines — at most the in-flight line of a crashed append.
/// A missing index file is a legacy leaf: `(vec![], 0)`.
pub fn read_index(leaf: &Path) -> Result<(Vec<IndexEntry>, usize), PersistError> {
    let file = match File::open(leaf.join(INDEX_FILE)) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((vec![], 0)),
        Err(e) => return Err(e.into()),
    };
    let mut entries = Vec::new();
    let mut torn = 0usize;
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<IndexEntry>(&line) {
            Ok(entry) => entries.push(entry),
            Err(_) => torn += 1,
        }
    }
    Ok((entries, torn))
}

/// Parquet files present in the leaf but absent from its index — the defined
/// "pending" state a crash between rename and append produces. Readers
/// ignore pending files until a repair pass adopts them; on a legacy leaf
/// (no index) nothing is pending.
pub fn pending_files(leaf: &Path) -> Result<Vec<String>, PersistError> {
    let index_path = leaf.join(INDEX_FILE);
    if !index_path.exists() {
        return Ok(vec![]);
    }
    let (entries, _) = read_index(leaf)?;
    let listed: std::collections::HashSet<&str> =
        entries.iter().map(|e| e.filename.as_str()).collect();
    let mut pending = Vec::new();
    for entry in fs::read_dir(leaf)?.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.ends_with(".parquet") && !listed.contains(name) {
            pending.push(name.to_string());
        }
    }
    pending.sort();
    Ok(pending)
}
