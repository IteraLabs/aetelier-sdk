#![cfg(feature = "parquet")]

use aetelier_connect::config::workers::MarketWorkerManifest;
use aetelier_connect::workers::market_worker::MarketWorker;
use aetelier_io::ParquetSnapshotFlusher;
use tokio::sync::watch;

fn stage_fixture_hour(root: &std::path::Path) -> usize {
    let fixture = format!(
        "{}/../aetelier-connect/datasets/hyperliquid/btc_book_trade.jsonl",
        env!("CARGO_MANIFEST_DIR")
    );
    let lines: Vec<String> = std::fs::read_to_string(fixture)
        .unwrap()
        .lines()
        .take(400)
        .map(str::to_string)
        .collect();
    let key = root.join("market_data/20260805/12/l2Book/BTC.lz4");
    std::fs::create_dir_all(key.parent().unwrap()).unwrap();
    let payload = lines.join("\n");
    std::fs::write(
        &key,
        aetelier_entrepot::codec::encode_lz4(payload.as_bytes()),
    )
    .unwrap();
    lines.len()
}

fn parquet_files(dir: &std::path::Path) -> Vec<String> {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.ends_with(".parquet"))
                .collect()
        })
        .unwrap_or_default()
}

#[tokio::test]
async fn entrepot_manifest_replays_to_parquet_and_terminates() {
    let archive = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let staged = stage_fixture_hour(archive.path());
    assert!(staged > 0);

    let toml = format!(
        r#"
[collect]
exchange = "hyperliquid"
market_type = "perpetual"
transport = "entrepot"
framework_ingest = true

[collect.entrepot]
root = "{root}"
start = "2026-08-05"
end = "2026-08-05"

[collect.datatypes.orderbook]
enabled = true
depth = 20

[collect.datatypes.trades]
enabled = true

[collect.sync]
sync_mode = "on_orderbook"
flush_threshold = 50

[collect.sync.update_frequency]
value = 500
unit = "Millis"

[[collect.output]]
type = "parquet"
dir = "{out}"

[[workers]]
symbol = "BTC"
"#,
        root = archive.path().display(),
        out = out.path().display(),
    );

    let cfg = MarketWorkerManifest::from_str(&toml)
        .unwrap()
        .resolve_all()
        .remove(0);
    let worker = MarketWorker::from_config_with_flusher(
        cfg,
        Some(Box::new(ParquetSnapshotFlusher)),
    )
    .unwrap();

    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let report =
        tokio::time::timeout(std::time::Duration::from_secs(60), worker.run(shutdown_rx))
            .await
            .expect("a finite source must terminate the worker, never reconnect forever")
            .unwrap();

    assert!(report.snapshots_produced > 0);

    let ob_files = parquet_files(&out.path().join("orderbooks"));
    assert!(!ob_files.is_empty(), "orderbook parquet written");
    for name in &ob_files {
        assert!(
            name.starts_with("hyperliquid_BTC-USDC_ob_sync_20260805"),
            "filename stamps from the data's own day, got: {name}"
        );
    }

    let trade_files = parquet_files(&out.path().join("trades"));
    assert!(!trade_files.is_empty(), "trade parquet written");
    for name in &trade_files {
        assert!(
            name.starts_with("hyperliquid_BTC-USDC_trades_sync_20260805"),
            "filename stamps from the data's own day, got: {name}"
        );
    }
}
