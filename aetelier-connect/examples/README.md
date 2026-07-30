# Examples

Every example runs against live public venue endpoints — no API keys, no accounts, no configuration beyond the command line. Commands run from the workspace root.

## framework_live — the 60-second quickstart

Connects to a venue, seeds the order book, and prints reconstructed top-of-book rows in real time.

```
cargo run -p aetelier-connect --example framework_live binance 8 BTCUSDT
```

Expected output within a few seconds:

```
live: binance BTCUSDT for 8s — model SeqDelta { predicate: RangeInclusive, source: RestSnapshot }
BTC/USDT #2     bid 66327.05000000 x1.43759000 | ask 66327.06000000 x2.34211000 | spread 0.01000000 | ts 1784669353249446 | depth 5000/5000
BTC/USDT #14    bid 66327.05000000 x0.90601000 | ask 66327.06000000 x2.34273000 | spread 0.01000000 | ts 1784669354314000 | depth 5010/4997
```

Runtime: as many seconds as requested plus a ~2s connect/seed preamble. Any of the 12 supported venues works as the first argument.

## data_worker_framework — raw event stream

The DataWorker path: decoded venue events without grid synchronization.

```
cargo run -p aetelier-connect --example data_worker_framework -- binance 8 BTCUSDT
```

Runtime: as requested. Prints decoded trade and book events as they arrive.

## run_market_worker — config-driven collection to Parquet

The full MarketWorker: TOML manifest in, grid-synchronized Parquet files out.

```
cargo run -p aetelier-connect --example run_market_worker --features parquet -- --config aetelier-connect/examples/md_worker/md_worker_binance.toml
```

Venue manifests for binance, bybit, coinbase, gateio, kraken, and okx sit beside it. Runtime: until interrupted; files land in the configured output directory.

## read_market_worker — Parquet read-back

Closes the write-read loop: reads the files a MarketWorker run produced and prints per-symbol summary statistics (row counts, best bid/ask, spread, VWAP, buy/sell flow, grid spacing).

```
cargo run -p aetelier-connect --example read_market_worker --features parquet -- --dir datasets/collected/bybit
```

Runtime: seconds. Point `--dir` at any directory a MarketWorker wrote.

## run_data_worker — config-driven raw collection

```
cargo run -p aetelier-connect --example run_data_worker -- --config aetelier-connect/examples/data_worker/data_worker_config.toml
```

Runtime: until interrupted.

## multi_sync_workers — several venues in one process

```
cargo run -p aetelier-connect --example multi_sync_workers --features parquet -- --config aetelier-connect/examples/multi_sync/multi_sync_workers.toml
```

Runtime: until interrupted; one synchronized worker per configured venue.

## capture_fixture — raw wire capture

Records raw WebSocket frames to JSONL through each venue's production protocol hooks — the tool behind the conformance fixture corpus.

```
cargo run -p aetelier-connect --example capture_fixture -- binance BTCUSDT 60 /tmp/binance_capture.jsonl
```

Runtime: as requested.
