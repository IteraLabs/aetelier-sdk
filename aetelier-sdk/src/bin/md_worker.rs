//! `md_worker` — synchronised market-data collector with Parquet persistence.
//!
//! Connects to one or more exchange WebSocket feeds (per the TOML manifest),
//! decodes events, feeds them through a `MarketSynchronizer`, and emits
//! grid-aligned `MarketSnapshot`s to the configured output sinks.
//!
//! When built with the `parquet` feature (the default for the
//! `aetelier-md-worker` Docker image), this binary wires
//! [`aetelier_io::ParquetSnapshotFlusher`] into the per-worker sink set so
//! that `[[collect.output]] type = "parquet"` entries in the manifest
//! actually persist `orderbooks/`, `trades/`, `liquidations/`, `fundings/`,
//! and `open_interests/` parquet files to disk. Without that wiring, the
//! parquet sink is silently dropped at sink-build time (see
//! `aetelier_connect::workers::output::build_sinks`).
//!
//! # Usage
//!
//! ```bash
//! md_worker --config configs/manifest.toml
//! RUST_LOG=info md_worker --config configs/manifest.toml
//! ```
//!
//! The worker runs until interrupted (Ctrl-C / SIGINT) or until the optional
//! `session.duration_hours` from the manifest expires.

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use tokio::{sync::watch, task::JoinSet};
use tracing_subscriber::EnvFilter;

use aetelier_connect::config::workers::{MarketWorkerConfig, MarketWorkerManifest};
use aetelier_connect::workers::market_worker::MarketWorker;

#[cfg(feature = "parquet")]
use aetelier_io::ParquetSnapshotFlusher;

/// Synchronised market data collection worker.
#[derive(Parser, Debug)]
#[command(name = "md_worker", version, about)]
struct Cli {
    /// Path to a TOML worker manifest file.
    #[arg(short, long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── Tracing ─────────────────────────────────────────────────────────
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(true)
        .with_thread_ids(false)
        .init();

    let cli = Cli::parse();
    let manifest = MarketWorkerManifest::from_toml(&cli.config)?;
    let configs = manifest.resolve_all();

    if configs.is_empty() {
        anyhow::bail!("manifest contains no worker entries");
    }

    tracing::info!(
        workers = configs.len(),
        config = %cli.config.display(),
        parquet_sink = cfg!(feature = "parquet"),
        "md_worker.starting"
    );

    // ── Shutdown signal ─────────────────────────────────────────────────
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Ctrl-C handler
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
        tracing::info!("md_worker.shutdown_requested");
        let _ = shutdown_tx_clone.send(true);
    });

    // Optional session duration
    if let Some(dur_secs) = manifest.duration_secs() {
        let shutdown_tx_clone = shutdown_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs_f64(dur_secs)).await;
            tracing::info!(
                duration_secs = dur_secs,
                "md_worker.session_duration_elapsed"
            );
            let _ = shutdown_tx_clone.send(true);
        });
    }

    // ── Spawn workers ───────────────────────────────────────────────────
    let mut join_set = JoinSet::new();

    for (i, cfg) in configs.into_iter().enumerate() {
        let rx = shutdown_rx.clone();
        let label = format!("{}:{}", cfg.common.exchange, cfg.common.symbol);

        // Build the worker, wiring a `ParquetSnapshotFlusher` when the
        // `parquet` feature is enabled. Without the flusher, any
        // `OutputSinkConfig::Parquet { dir }` entry in the manifest is
        // silently dropped by `build_sinks` and no parquet files are
        // written to disk.
        let worker = build_worker(cfg)?;

        tracing::info!(
            worker = i,
            label = %label,
            "md_worker.spawning_worker"
        );

        // Stagger startup to avoid thundering-herd on exchange endpoints.
        if i > 0 {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        join_set.spawn(async move { worker.run(rx).await });
    }

    // ── Await all workers ───────────────────────────────────────────────
    let mut total_snapshots: u64 = 0;

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(report)) => {
                tracing::info!(
                    exchange = %report.ingestion.exchange,
                    symbol = %report.ingestion.symbol,
                    total_events = report.ingestion.total_events,
                    snapshots = report.snapshots_produced,
                    flushes = report.flushes,
                    elapsed_secs = format!("{:.1}", report.ingestion.elapsed_secs),
                    reconnects = report.ingestion.reconnect_count,
                    "md_worker.worker_finished"
                );
                total_snapshots += report.snapshots_produced;
            }
            Ok(Err(e)) => {
                tracing::error!(error = %e, "md_worker.worker_error");
            }
            Err(e) => {
                tracing::error!(error = %e, "md_worker.worker_panic");
            }
        }
    }

    tracing::info!(
        total_snapshots = total_snapshots,
        "md_worker.all_workers_finished"
    );

    Ok(())
}

/// Construct a [`MarketWorker`] with the appropriate sink wiring for the
/// active feature set.
///
/// With `parquet`, the [`ParquetSnapshotFlusher`] from `aetelier-io` is
/// passed into [`MarketWorker::from_config_with_flusher`] so that any
/// `Parquet` sink declared in the manifest receives a concrete
/// `SnapshotFlusher` and actually writes parquet files. Without the
/// `parquet` feature, the worker uses the channel/terminal-only path.
#[cfg(feature = "parquet")]
fn build_worker(cfg: MarketWorkerConfig) -> anyhow::Result<MarketWorker> {
    Ok(MarketWorker::from_config_with_flusher(
        cfg,
        Some(Box::new(ParquetSnapshotFlusher)),
    )?)
}

#[cfg(not(feature = "parquet"))]
fn build_worker(cfg: MarketWorkerConfig) -> anyhow::Result<MarketWorker> {
    MarketWorker::from_config(cfg)
}
