//! Live validation of the framework ingestion + reconstruction path for any
//! registered venue, with the same reconnect-on-resync behaviour
//! `MarketWorker::run_framework` uses.
//!
//! Resolves the venue adapter, spawns it against the live WebSocket, runs the
//! reconstruction `SourceRuntime`, and prints the reconstructed top-of-book +
//! trade flow. When a self-seeded book gaps, the runtime ends with
//! `ResyncRequired` and this loop reconnects (a fresh subscribe re-seeds).
//!
//! Usage: cargo run -p aetelier-connect --example framework_live [VENUE] [SECONDS] [SYMBOLS]
//!   e.g. framework_live binance 15 BTCUSDT,ETHUSDT
//!        framework_live okx 20 BTC-USDT

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};

use aetelier_connect::framework::budget::SourceMetrics;
use aetelier_connect::framework::registry::registry;
use aetelier_connect::framework::rest::RestSnapshot;
use aetelier_connect::framework::runtime::{
    ReconstructedEvent, RuntimeOutcome, SourceRuntime,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let mut args = std::env::args().skip(1);
    let venue = args.next().unwrap_or_else(|| "binance".to_string());
    let secs: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(15);
    let symbols_arg = args.next().unwrap_or_else(|| "BTCUSDT".to_string());
    let wire_symbols: Vec<String> = symbols_arg
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let adapter = *registry()
        .get(venue.as_str())
        .unwrap_or_else(|| panic!("no registered adapter for venue '{venue}'"));
    let model = adapter.book_model("orders");
    // The adapter is the single source of truth for the seeding taxonomy:
    // SnapshotSource drives needs_rest + recovery, and the seeder mechanism
    // ships on the same adapter.
    let recovery = model.recovery_action();
    let seeder: Option<Arc<dyn RestSnapshot>> = adapter.rest_seeder();
    if model.needs_rest() && seeder.is_none() {
        panic!("venue '{venue}' declares a REST-seeded model but no rest_seeder");
    }
    let codec = adapter.profile().symbol_codec.clone();

    println!(
        "live: {venue} {symbols} for {secs}s — model {model:?}",
        symbols = wire_symbols.join(","),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let stop_tx = shutdown_tx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(secs)).await;
        let _ = stop_tx.send(true);
    });

    let mut book_updates = 0u64;
    let mut trades = 0u64;
    let mut reconnects = 0u32;
    let mut last_print = tokio::time::Instant::now();

    while !*shutdown_rx.borrow() {
        let (dev_tx, dev_rx) = mpsc::channel(4096);
        let (recon_tx, mut recon_rx) = mpsc::channel(4096);
        let adapter_handle = adapter.spawn(
            wire_symbols.clone(),
            dev_tx,
            shutdown_rx.clone(),
            SourceMetrics::default(),
        );
        let runtime = SourceRuntime::new(
            venue.clone(),
            codec.clone(),
            wire_symbols.clone(),
            model.clone(),
            recovery,
            SourceMetrics::default(),
        );
        let runtime_handle = tokio::spawn(runtime.run(
            dev_rx,
            seeder.clone(),
            recon_tx,
            shutdown_rx.clone(),
        ));

        while let Some(ev) = recon_rx.recv().await {
            match ev {
                ReconstructedEvent::Book { pair, ts_us, book } => {
                    book_updates += 1;
                    if last_print.elapsed() >= Duration::from_secs(1) {
                        last_print = tokio::time::Instant::now();
                        let best_bid = book.bids.iter().next_back();
                        let best_ask = book.asks.iter().next();
                        if let (Some((bp, bl)), Some((ap, al))) = (best_bid, best_ask) {
                            println!(
                                "{pair} #{book_updates:<5} bid {bp} x{:<10} | ask {ap} x{:<10} | spread {} | ts {ts_us} | depth {}/{}",
                                bl.volume,
                                al.volume,
                                ap - bp,
                                book.bids.len(),
                                book.asks.len(),
                            );
                        }
                    }
                }
                ReconstructedEvent::Trade(_) => trades += 1,
            }
        }

        drop(recon_rx);
        let _ = adapter_handle.await;
        let outcome = runtime_handle.await.unwrap_or(RuntimeOutcome::Finished);
        if *shutdown_rx.borrow() {
            break;
        }
        reconnects += 1;
        println!("-- reconnect #{reconnects} (outcome {outcome:?}) --");
        tokio::time::sleep(Duration::from_millis(400)).await;
    }

    let _ = shutdown_tx.send(true);
    println!(
        "done: {book_updates} book updates, {trades} trades, {reconnects} reconnects"
    );
    if book_updates == 0 {
        anyhow::bail!("no book updates — reconstruction did not produce a live book");
    }
    Ok(())
}
