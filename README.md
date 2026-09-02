![aetelier](assets/images/aetelier_banner.png)

<br>

[![Build][badge-build]][url-build]
[![Rust][badge-rust]][url-rust]
[![Apache-2.0 licensed][badge-license]][url-license]

[badge-build]: https://github.com/IteraLabs/aetelier-sdk/actions/workflows/ci.yml/badge.svg
[url-build]: https://github.com/IteraLabs/aetelier-sdk/actions/workflows/ci.yml
[badge-rust]: https://img.shields.io/badge/rust-1.88%2B-orange.svg?maxAge=3600
[url-rust]: https://github.com/IteraLabs/aetelier-sdk
[badge-license]: https://img.shields.io/badge/license-Apache--2.0-00baf5.svg
[url-license]: LICENSE

<br>

# aetelier-sdk

A Rust engine for high-frequency market-microstructure data: live connectivity to 13 crypto exchanges, verified order-book reconstruction, trade loss accounting, grid synchronization, and columnar Parquet I/O — built for quant researchers who need to trust every row.

Every venue decoder is certified by a conformance suite that replays real captured wire frames through the production decode path on every CI run: seeding, delta application, sequence-gap detection, checksum validation, trade continuity, and physical book invariants (never crossed, strictly positive, ordered). Exact decimal arithmetic in memory; analytics-friendly Float64 Parquet on disk.

Machine-readable entry point: [AGENTS.md](AGENTS.md).

## 60-second quickstart

Live top-of-book from Binance, no API keys, no configuration:

```
cargo run -p aetelier-connect --example framework_live binance 8 BTCUSDT
```

```
live: binance BTCUSDT for 8s — model SeqDelta { predicate: RangeInclusive, source: RestSnapshot }
BTC/USDT #2     bid 66327.05000000 x1.43759000 | ask 66327.06000000 x2.34211000 | spread 0.01000000 | ts 1784669353249446 | depth 5000/5000
BTC/USDT #14    bid 66327.05000000 x0.90601000 | ask 66327.06000000 x2.34273000 | spread 0.01000000 | ts 1784669354314000 | depth 5010/4997
```

As a library, the curated facade exposes the whole engine through one crate:

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

More examples — config-driven collection to Parquet, read-back statistics, multi-venue sync, raw wire capture — in [aetelier-connect/examples](aetelier-connect/examples/README.md).

## Install

Depend on the workspace via git, pinned to the release tag:

```toml
[dependencies]
aetelier-sdk = { git = "https://github.com/IteraLabs/aetelier-sdk", tag = "v0.1.0", features = ["parquet"] }
```

The six crates carry the layout and publish order for a registry release; [CONTRIBUTING.md](CONTRIBUTING.md) documents both.

## Workspace

| Crate | Role |
|---|---|
| `aetelier-types` | Canonical no-IO schema crate: trades, order books, liquidations, funding, configs. `forbid(unsafe_code)`. |
| `aetelier-connect` | Exchange connectivity: WSS transports, per-venue adapters, reconstruction runtime, synchronizers, workers. |
| `aetelier-io` | Columnar persistence: Parquet/JSON/CSV writers and readers, dataset tooling, batch rehydration. |
| `aetelier-telemetry` | Metrics and log surfaces for collectors. |
| `aetelier-entrepot` | Object-store transport for aetelier collectors: signed S3 access, listing, retrieval, and archive codecs. |
| `aetelier-sdk` | The curated facade: one dependency, named re-exports of the recommended path. |

## Venues

Thirteen venues run the same certified framework path — twelve spot plus hyperliquid perpetuals. Certification means the venue's real captured frames replay through the production decoder in CI across the full conformance matrix: decode surface, seeding taxonomy, symbol canonicalization, book delta application, gap detection, checksum validation where the venue supplies one, trade continuity where the venue's ids permit exact accounting, and physical book/trade invariants.

| Venue | Market | Protocol | Book model |
|---|---|---|---|
| binance | Spot | `binance-spot-v3` | SeqDelta |
| bitget | Spot | `bitget-v2` | SeqDelta |
| bitso | Spot | `bitso-v3` | L3 |
| bybit | Spot | `bybit-v5` | SeqDelta |
| coinbase | Spot | `coinbase-adv-v3` | SeqDelta |
| gateio | Spot | `gateio-v4` | FullRefresh |
| htx | Spot | `htx-v2` | SeqDelta |
| hyperliquid | Perpetual | `hyperliquid-v1` | FullRefresh |
| kraken | Spot | `kraken-v2` | ChecksumDelta |
| kucoin | Spot | `kucoin-v1` | SeqDelta |
| okx | Spot | `okx-v5` | SeqDelta |
| poloniex | Spot | `poloniex-v2` | SeqDelta |
| upbit | Spot | `upbit-v1` | FullRefresh |

**Last integration checkpoint: 2026-08-05** — the capture date of the newest venue's conformance fixture (hyperliquid, cycle #5). `Protocol` is the venue's pinned wire revision: a bump the venue ships that this repo has not adopted fails at boot rather than decoding wrong. `Book model` is the reconstruction the adapter declares — full-refresh snapshots, sequence-validated deltas, checksum-validated deltas, or per-order L3.

## Data honesty

The engine never fabricates: unknown trade sides are dropped and counted, never coerced; malformed rows are typed errors, never silent defaults; trade loss is counted exactly where venue sequences allow and labeled as an estimate where they do not; every recovered print carries its provenance (`origin: ws | rest`). Loss, gap, and recovery counters ride every collector as first-class metrics.

## Open core

This repository is the open data layer of the Aetelier platform: everything needed to collect, verify, and persist market data on infrastructure you control. The managed orchestration, hosted dashboards, and billing services are a separate hosted product at [aetelier.xyz](https://aetelier.xyz).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development loop, testing tiers, and the crate publish order. Security reports: [SECURITY.md](SECURITY.md).

## License

Apache-2.0 — see [LICENSE](LICENSE).
