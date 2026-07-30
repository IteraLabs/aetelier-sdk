//! Multi-worker Bybit market snapshot collection.
//!
//! Reads a worker manifest TOML and spawns one [`MarketWorker`] per symbol.
//! Each worker independently connects to Bybit's WSS, synchronizes market
//! data to a 100 ms grid, and flushes hourly Parquet files to a per-symbol
//! output directory.
//!
//! The process runs until the configured `duration_hours` elapses or
//! Ctrl-C is pressed, whichever comes first.
//!
//! # Usage
//!
//! ```bash
//! cargo run -p aetelier-connect --example multi_sync_workers --features parquet \
//!   -- --config aetelier-connect/examples/multi_sync/multi_sync_workers.toml
//! ```

use aetelier_types::config::WorkerManifest;
use std::path::Path;

// ─────────────────────────────────────────────────────────────────────────────
// CLI
// ─────────────────────────────────────────────────────────────────────────────

/// Parse `--config <path>` from command-line arguments.
fn parse_config_path() -> String {
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--config" {
            if i + 1 < args.len() {
                return args[i + 1].clone();
            }
            eprintln!("error: --config requires a path argument");
            std::process::exit(1);
        }
        i += 1;
    }
    eprintln!("usage: bybit_workers --config <path/to/manifest.toml>");
    std::process::exit(1);
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .compact()
        .init();

    let config_path = parse_config_path();
    let _manifest = WorkerManifest::from_toml(Path::new(&config_path))?;

    Ok(())
}
