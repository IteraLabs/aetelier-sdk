//! `data_worker` — lean raw-data ingestion binary.
//!
//! Connects to one or more exchange WebSocket feeds, decodes events
//! without any preprocessing, and publishes them to topic-keyed
//! broadcast channels.  No normalisation, no delta-application, no
//! Parquet writes — just raw ingestion with gap detection.
//!
//! # Usage
//!
//! ```bash
//! # Single-worker from a TOML manifest:
//! data_worker --config path/to/manifest.toml
//!
//! # With a custom log level:
//! RUST_LOG=info data_worker --config manifest.toml
//! ```
//!
//! The worker runs until interrupted (Ctrl-C / SIGINT) or until the
//! optional `session.duration_hours` expires.

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing_subscriber::EnvFilter;

use aetelier_connect::workers::data_worker::DataWorker;
use aetelier_types::config::WorkerManifest;

/// Lean raw-data ingestion worker for cryptocurrency exchange feeds.
#[derive(Parser, Debug)]
#[command(name = "data_worker", version, about)]
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
    let manifest = WorkerManifest::from_toml(&cli.config)?;
    let configs = manifest.resolve_all();

    if configs.is_empty() {
        anyhow::bail!("manifest contains no worker entries");
    }

    tracing::info!(
        workers = configs.len(),
        config = %cli.config.display(),
        "data_worker.starting"
    );

    // ── Shutdown signal ─────────────────────────────────────────────────
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Ctrl-C handler
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
        tracing::info!("data_worker.shutdown_requested");
        let _ = shutdown_tx_clone.send(true);
    });

    // Optional session duration
    if let Some(dur_secs) = manifest.duration_secs() {
        let shutdown_tx_clone = shutdown_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs_f64(dur_secs)).await;
            tracing::info!(
                duration_secs = dur_secs,
                "data_worker.session_duration_elapsed"
            );
            let _ = shutdown_tx_clone.send(true);
        });
    }

    // ── Spawn workers ───────────────────────────────────────────────────
    let mut join_set = JoinSet::new();

    for (i, cfg) in configs.into_iter().enumerate() {
        let rx = shutdown_rx.clone();
        let label = format!("{}:{}", cfg.exchange.name, cfg.symbol.name);

        // Build the topic registry before spawning —
        // this is the handle downstream consumers would use to subscribe.
        let worker = DataWorker::new(cfg);
        let registry = worker.build_registry();

        tracing::info!(
            worker = i,
            label = %label,
            topics = ?registry.topics(),
            "data_worker.spawning_worker"
        );

        // Stagger startup to avoid thundering-herd on exchange endpoints
        if i > 0 {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        join_set.spawn(async move { worker.run_legacy(rx, registry).await });
    }

    // ── Await all workers ───────────────────────────────────────────────
    let mut total_events: u64 = 0;

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(report)) => {
                tracing::info!(
                    exchange = %report.exchange,
                    symbol = %report.symbol,
                    total_events = report.total_events,
                    elapsed_secs = format!("{:.1}", report.elapsed_secs),
                    rate = format!("{:.1}/s", report.effective_rate),
                    reconnects = report.reconnect_count,
                    "data_worker.worker_finished"
                );
                total_events += report.total_events;
            }
            Ok(Err(e)) => {
                tracing::error!(error = %e, "data_worker.worker_error");
            }
            Err(e) => {
                tracing::error!(error = %e, "data_worker.worker_panic");
            }
        }
    }

    tracing::info!(
        total_events = total_events,
        "data_worker.all_workers_finished"
    );

    Ok(())
}
