//! Stub: the real `md_worker` binary has moved to the `aetelier-sdk` crate so
//! it can wire `aetelier_io::ParquetSnapshotFlusher` (which would create a
//! cyclic Cargo dependency if added here, since `aetelier-io` already
//! optionally depends on `aetelier-connect`).
//!
//! Run the production binary with:
//!
//! ```bash
//! cargo run -p aetelier-sdk --bin md_worker --features parquet -- \
//!   --config configs/manifest.toml
//! ```

fn main() {
    eprintln!(
        "aetelier-connect's `md_worker` bin has moved to the `aetelier-sdk` crate. \
         Run: cargo run -p aetelier-sdk --bin md_worker --features parquet"
    );
    std::process::exit(2);
}
