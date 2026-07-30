//! MarketWorker example — synchronised market snapshots to Parquet.
//!
//! Connects to a live exchange WebSocket feed, synchronises events to a
//! 200 ms grid using [`ClockMode::ExternalClock`], and writes the resulting
//! [`MarketSnapshot`]s to Parquet files.
//!
//! # Usage
//!
//! ```bash
//! cargo run -p aetelier-connect --example run_market_worker --features parquet \
//!   -- --config aetelier-connect/examples/md_worker/md_worker_binance.toml
//! ```
//!
//! Parquet files land in the folder indicated in [[defaults.output]].[dir]
//! with subdirectories per data type (`trades/`, `orderbooks/`, etc.).

use clap::Parser;
use std::{path::PathBuf, time::Duration};
use tokio::{sync::watch, task::JoinSet};
use tracing_subscriber::EnvFilter;

use aetelier_connect::{
    config::workers::MarketWorkerManifest, workers::market_worker::MarketWorker,
};

// ──────────────────────────────────────────────────────────────── CLI Interface ──── //
#[derive(Parser, Debug)]
#[command(name = "run_market_worker", version, about)]
struct Cli {
    /// Path to a TOML worker manifest file.
    #[arg(short, long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ────────────────────────────────────────────────────────── Tracing  Config ──── //

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(true)
        .init();

    let cli = Cli::parse();
    let manifest = MarketWorkerManifest::from_toml(&cli.config)?;
    let configs = manifest.resolve_all();

    if configs.is_empty() {
        anyhow::bail!("manifest is empty");
    }

    tracing::info!(
        workers = configs.len(),
        config = %cli.config.display(),
        "examples.market_worker.starting"
    );

    // ────────────────────────────────────────────────────────── Shutdown Signal ──── //

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let shutdown_tx_clone = shutdown_tx.clone();

    // ctrl + c command
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
        tracing::info!("examples.market_worker.shutdown_requested");
        let _ = shutdown_tx_clone.send(true);
    });

    // timed shutdown
    if let Some(dur_secs) = manifest.duration_secs() {
        let shutdown_tx_clone = shutdown_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs_f64(dur_secs)).await;
            tracing::info!(
                duration_secs = dur_secs,
                "examples.market_worker.session_elapsed"
            );
            let _ = shutdown_tx_clone.send(true);
        });
    }

    // ──────────────────────────────────────────────────────────── Spawn Workers ──── //

    let mut join_set = JoinSet::new();

    for (i, cfg) in configs.into_iter().enumerate() {
        let rx = shutdown_rx.clone();
        let label = format!("{}:{}", cfg.common.exchange, cfg.common.symbol);

        let worker = MarketWorker::from_config(cfg)?;

        tracing::info!(
            worker = i,
            label = %label,
            "example.market_worker.spawning"
        );

        if i > 0 {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        join_set.spawn(async move { worker.run(rx).await });
    }

    // ──────────────────────────────────────────────────────────── Await Results ──── //
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
                    elapsed = format!("{:.1}s", report.ingestion.elapsed_secs),
                    reconnects = report.ingestion.reconnect_count,
                    "example.market_worker.finished"
                );
                total_snapshots += report.snapshots_produced;
            }
            Ok(Err(e)) => tracing::error!(error = %e, "example.market_worker.error"),
            Err(e) => tracing::error!(error = %e, "example.market_worker.panic"),
        }
    }

    tracing::info!(
        total_snapshots = total_snapshots,
        "example.market_worker.all_done"
    );

    Ok(())
}
