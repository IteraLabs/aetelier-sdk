//! DataWorker example — raw event ingestion with terminal output.
//!
//! Connects to a live exchange WebSocket feed, decodes events without
//! normalisation, and prints them via the terminal sink.
//!
//! # Usage
//!
//! ```bash
//! cargo run -p aetelier-connect --example run_data_worker \
//!   -- --config aetelier-connect/examples/data_worker/data_worker_config.toml
//! ```
//!
//! Set `RUST_LOG=debug` to see individual events:
//!
//! ```bash
//! RUST_LOG=debug cargo run -p aetelier-connect --example run_data_worker \
//!   -- --config aetelier-connect/examples/data_worker/data_worker_config.toml
//! ```

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing_subscriber::EnvFilter;

use aetelier_connect::config::workers::DataWorkerManifest;
use aetelier_connect::workers::data_worker::DataWorker;

/// DataWorker example — raw event ingestion.
#[derive(Parser, Debug)]
#[command(name = "run_data_worker", version, about)]
struct Cli {
    /// Path to a TOML worker manifest file.
    #[arg(short, long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(true)
        .init();

    let cli = Cli::parse();
    let manifest = DataWorkerManifest::from_toml(&cli.config)?;
    let configs = manifest.resolve_all();

    if configs.is_empty() {
        anyhow::bail!("manifest contains no worker entries");
    }

    tracing::info!(
        workers = configs.len(),
        config = %cli.config.display(),
        "example.data_worker.starting"
    );

    // ── Shutdown signal ─────────────────────────────────────────────────
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
        tracing::info!("example.data_worker.shutdown_requested");
        let _ = shutdown_tx_clone.send(true);
    });

    if let Some(dur_secs) = manifest.duration_secs() {
        let shutdown_tx_clone = shutdown_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs_f64(dur_secs)).await;
            tracing::info!(
                duration_secs = dur_secs,
                "example.data_worker.session_elapsed"
            );
            let _ = shutdown_tx_clone.send(true);
        });
    }

    // ── Spawn workers ───────────────────────────────────────────────────
    let mut join_set = JoinSet::new();

    for (i, cfg) in configs.into_iter().enumerate() {
        let rx = shutdown_rx.clone();
        let label = format!("{}:{}", cfg.common.exchange, cfg.common.symbol);

        let worker = DataWorker::from_config(cfg)?;

        tracing::info!(
            worker = i,
            label = %label,
            "example.data_worker.spawning"
        );

        if i > 0 {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        join_set.spawn(async move { worker.run(rx).await });
    }

    // ── Await results ───────────────────────────────────────────────────
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(report)) => {
                tracing::info!(
                    exchange = %report.exchange,
                    symbol = %report.symbol,
                    total_events = report.total_events,
                    elapsed = format!("{:.1}s", report.elapsed_secs),
                    rate = format!("{:.1} evt/s", report.effective_rate),
                    reconnects = report.reconnect_count,
                    "example.data_worker.finished"
                );
            }
            Ok(Err(e)) => tracing::error!(error = %e, "example.data_worker.error"),
            Err(e) => tracing::error!(error = %e, "example.data_worker.panic"),
        }
    }

    Ok(())
}
