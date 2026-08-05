# aetelier-connect

Exchange connectivity and reconstruction: WebSocket transports, per-venue protocol adapters, the order-book and trade-book reconstruction runtime, grid synchronizers, and the collection workers. This crate is where wire frames become verified market data.

## Venues

Thirteen venues run the same framework path: twelve spot venues — binance, bitget, bitso, bybit, coinbase, gateio, htx, kraken, kucoin, okx, poloniex, upbit — plus hyperliquid perpetuals (bare-coin symbols, USDC-margined, full-refresh snapshot book). Each is certified by the conformance suite in `tests/conformance/`, which replays real captured frames (committed under `datasets/`) through the production decode path on every CI run: decode surface, seeding taxonomy, symbol canonicalization, book delta application, book runtime replay, datatype isolation, gap detection, checksum validation, trade continuity, and physical book and trade invariants.

```rust
use aetelier_connect::framework::registry::registry;

assert!(registry().get("binance").is_some());
assert!(registry().get("upbit").is_some());
```

## Architecture

- `framework/` — the venue-generic engine: `transport` (WSS pump with staleness and RTT), `adapters/` (13 venue protocol implementations), `model` (reconstruction models and book state machines), `runtime` (seed/reseed/escalation), `feed` (per-subscription FSM), `budget` (SourceMetrics counters), `reconcile` (REST trade recovery), `atlas` (invariant-ID registry backing the conformance coverage check).
- `synchronizers/` — grid alignment: `MarketSynchronizer` (clock modes, timestamp-membership attribution, emission hold-back), `ObSynchronizer`, `EventSynchronizer`.
- `workers/` — `MarketWorker` (the recommended entry point: TOML manifest in, synchronized Parquet out), `DataWorker`, the sink layer (buffered Parquet, channels, terminal), and the gap ledger.
- `clients/` — reconnect policy (jittered backoff, circuit breaker, health monitoring) and the legacy per-venue clients, deprecated in favor of the framework path.
- `sources/` — the per-venue wire-type layer for all thirteen venues: decoders, event enums, and response structs, one folder per venue, imported by the framework adapters. The six original venues additionally carry a `client/` module — the deprecated pre-framework ingestion path, retained while the extended datatypes (funding, open interest, liquidations) complete their migration to the framework engine.

## Running

Live quickstart, no credentials:

```text
cargo run -p aetelier-connect --example framework_live binance 8 BTCUSDT
```

The full example catalog — collection to Parquet, read-back statistics, multi-venue sync, wire capture — is in examples/README.md.

## Tests

Workspace tests plus the conformance suite (`tests/conformance.rs`) run in CI on every push; the conformance matrix hard-fails if any certified venue loses a passing kind.
