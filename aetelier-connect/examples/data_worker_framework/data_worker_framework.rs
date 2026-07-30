//! Live validation of the framework `DataWorker` path: builds a `DataWorker`
//! with `framework_ingest = true`, subscribes to its normalized domain topics,
//! runs it against a live socket, and counts the `Book`/`Trade` messages that
//! flow onto the domain channel.
//!
//! Usage: cargo run -p aetelier-connect --example data_worker_framework -- [VENUE] [SECONDS] [SYMBOL]
//!   e.g. data_worker_framework bybit 15 BTCUSDT
//!        data_worker_framework binance 15 BTCUSDT

use std::time::Duration;

use tokio::sync::watch;

use aetelier_connect::config::workers::DataWorkerManifest;
use aetelier_connect::workers::DataWorker;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let mut args = std::env::args().skip(1);
    let venue = args.next().unwrap_or_else(|| "bybit".to_string());
    let secs: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(15);
    let symbol = args.next().unwrap_or_else(|| "BTCUSDT".to_string());

    let toml = format!(
        r#"
[collect]
exchange = "{venue}"
framework_ingest = true

[collect.datatypes.orderbook]
enabled = true
depth = 50

[collect.datatypes.trades]
enabled = true

[[collect.output]]
type = "channel"

[[workers]]
symbol = "{symbol}"
"#
    );

    let cfg = DataWorkerManifest::from_str(&toml)?
        .resolve_all()
        .pop()
        .expect("one worker");
    let worker = DataWorker::from_config(cfg)?;

    // Clone the domain registry and subscribe BEFORE running (broadcast only
    // delivers to receivers that exist when a message is published).
    let registry = worker
        .domain_registry()
        .expect("framework_ingest + channel output exposes a domain registry")
        .clone();
    let ob_topic = format!("orderbook.50.{symbol}");
    let trade_topic = format!("trade.all.{symbol}");
    let mut ob_rx = registry.subscribe(&ob_topic).expect("orderbook topic");
    let mut tr_rx = registry.subscribe(&trade_topic).expect("trade topic");

    println!(
        "live framework DataWorker: {venue} {symbol} for {secs}s — topics {ob_topic} / {trade_topic}"
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker_handle = tokio::spawn(worker.run(shutdown_rx));

    let mut books = 0u64;
    let mut trades = 0u64;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => break,
            m = ob_rx.recv() => if let Ok(msg) = m {
                books += 1;
                if books % 50 == 1 {
                    println!("book #{books:<5} topic {} exchange {}", msg.topic, msg.exchange);
                }
            },
            m = tr_rx.recv() => if let Ok(msg) = m {
                trades += 1;
                if trades % 25 == 1 {
                    println!("trade #{trades:<5} topic {}", msg.topic);
                }
            },
        }
    }

    let _ = shutdown_tx.send(true);
    let report = worker_handle
        .await?
        .unwrap_or_else(|e| panic!("worker error: {e}"));
    println!(
        "done: {books} book + {trades} trade domain messages | worker total_events={} reconnects={}",
        report.total_events, report.reconnect_count,
    );
    if books == 0 && trades == 0 {
        anyhow::bail!("no domain messages — framework DataWorker produced nothing");
    }
    Ok(())
}
