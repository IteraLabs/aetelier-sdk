# aetelier-sdk

The curated facade over the aetelier market-data engine: one dependency that re-exports the recommended path by name — canonical types, the `MarketWorker` collection entry point, Parquet persistence, and telemetry — plus whole-crate re-exports for power users.

## Usage

```rust
use aetelier_sdk::{Trade, TradeSide, TradingPair};

let trade = Trade::builder()
    .source_trade_ts_us(1_700_000_000_000_000)
    .pair(TradingPair::new("BTC", "USDT"))
    .side(TradeSide::Buy)
    .amount(0.5)
    .price(42_000.0)
    .exchange("binance".into())
    .id("t-1".into())
    .build()
    .expect("all fields set");

assert_eq!(trade.pair.to_canonical(), "BTC/USDT");
```

The `parquet` feature forwards to `aetelier-io` and unlocks the persistence re-exports. The `md_worker` binary in this crate is the config-driven collector: a TOML manifest in, grid-synchronized Parquet files out.

## Facade policy

Named re-exports cover the framework-first happy path; the legacy per-venue client surface is deliberately not re-exported. The whole-crate re-exports (`aetelier_sdk::aetelier_connect`, ...) remain available when something outside the curated surface is needed.

## Tests

The crate-level doc test compiles the quickstart against root imports only; CI runs it on every push.
