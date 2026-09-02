# Changelog

Every promotion into `main` adds one line under `Unreleased` → `Promotions`:
date, source branch, effect. Released version entries are frozen; a released
entry is edited only to correct a factual error, never to restate its scope.

## Unreleased

### Promotions

- 2026-08-22 · `staging` (#14) — staging promoted to `main`.
- 2026-08-22 · `chore/pr-template` (#13) — pull-request template gains evidence sections.
- 2026-08-22 · `fix/storage-type-correctness` (#12) — decode bounds, archive client deadlines, delta column declaration order.
- 2026-08-19 · `feat/entrepot-transport` (#11) — `aetelier-entrepot` object-store transport crate, Hyperliquid S3 archive replay, `entrepot` binary, bounded backfill retries, decode-panic containment.
- 2026-08-14 · `chore/hyperliquid-monitoring-tune` (#9) — fleet heartbeat, settlement window contract, arrival-clock settlement drain.
- 2026-08-10 · `hotfix/pin-actions-scrub-comments` (#8) — release workflow actions pinned to commit SHAs.
- 2026-08-10 · hyperliquid WSS (#7) — hyperliquid registered as the thirteenth framework adapter (perpetual, FullRefresh); funding and open-interest settlement path; stage-2 conformance.
- 2026-08-05 · `feature/delta-before-snapshot-guards` — out-of-band seeding taxonomy, in-band buffering exit, seed deadline.

### Fixed

- Delta orderbook Parquet declared its `side` and `exchange` columns in the opposite order to the values it wrote, so every consumer resolving by column name read exchange strings as sides and sides as exchanges. The declaration now matches the data, the sibling snapshot writer, and both readers. Readers that resolve positionally — including this crate's own — are unaffected in either direction, since no byte position moved; positional tolerance cannot express a rename, so a file written before this fix is identified by its embedded schema, where `side` precedes `exchange`.

### Added

- Decode bounds on untrusted input: a gzip WebSocket frame that inflates past 64 MiB and an archive object that decompresses past 256 MiB are refused rather than allocated, so a compression bomb cannot exhaust a long-running collector.
- Deadlines on the archive HTTP client: a 15 s connect bound so a black-holed endpoint surfaces instead of parking, and a 30 s idle-read bound that fires only when no bytes arrive. No total request timeout is set, so multi-gigabyte object downloads are never killed mid-transfer.

### Changed

- A failed response-body read on the archive path is now classified and retried by the same transport policy as a failed send, instead of terminating the run; the two paths share one retry budget.

## v0.1.0 — 2026-07-30

The first public release: the public front door of the aetelier market-data engine. Five crates — types, connect, io, telemetry, and the sdk facade — with 12 conformance-certified venues, exact loss accounting, and columnar Parquet persistence.

### Added

- Venue conformance harness: every venue is certified by replaying its own captured wire frames through the production decode path, across a ratcheting matrix of test kinds — decode surface, seeding taxonomy, symbol canonicalization, book delta application, sequence-gap detection (a dropped delta must gap the book, proven per venue), checksum validation, trade continuity, and independent physical oracles (books never crossed, strictly positive, ordered; trades positive with microsecond-scale timestamps). The suite runs in CI on every push; a certified venue losing a passing kind fails the build.
- Data-completeness instrumentation in every collector: exact trade-loss counters where venue sequences are dense (nine venues, including across reconnects), labeled estimates where they are not; book gap incidents and windows; a crash-safe JSONL gap ledger beside the Parquet output; connection-level gap oracles for Coinbase (split sockets, heartbeat-attributed sequence tracking) and Bitso (dense per-book envelope sequence).
- Live reconciliation: REST-recovered trades merge into their still-buffered grid rows within a configurable emission hold-back window, deduplicated and stamped with `origin: rest`; recovery never rewrites loss accounting.
- Batch rehydration: a `rehydrate` engine and binary that set-differences persisted trade parquets against the venue's REST history by id, writes a dense `*_rehydrated` file beside the immutable originals, and reports honestly what retention makes unrecoverable.
- Trade provenance: `Trade.origin` (`ws` | `rest`) in memory and as a Parquet column appended last; readers tolerate files written before the column existed.
- Grid synchronization guarantees: timestamp-membership attribution (every event lands in the row that closes its own period — no look-ahead leakage, no backlog dumping), as-of-boundary books with true timestamps, idempotent `finalize()` with a real terminal state, and counted late-event drops.
- A snapshot broadcast channel: the synchronized stream is subscribable in-process (`MarketWorker::snapshot_channel`), never blocking the collector, with receiver-side lag quantified exactly.
- The curated facade: `aetelier-sdk` re-exports the recommended path by name — canonical types, `MarketWorker`, Parquet persistence, telemetry, and the unified `AetelierError` — with whole-crate re-exports for power users.
- Tier-0 CI: format, clippy with denied warnings, workspace nextest on default and parquet legs, doc tests, rustdoc with denied warnings, per-feature check via cargo hack, cargo deny, an MSRV job at 1.88, and a macOS leg.
- First-class datasets: the captured fixture corpus ships in-repo with a provenance manifest, powering both the conformance suite and offline replay.

### Changed

- Trades and liquidations carry exact `rust_decimal::Decimal` amounts and prices in memory; the JSON wire format (floats) and the Parquet physical type (Float64) are unchanged.
- Timestamps are UTC epoch microseconds everywhere, under `_us` field and column names; venue wire units convert exactly once at the adapter boundary.
- The WebSocket transport runs tokio-tungstenite 0.30 on a single rustls 0.23 line.
- The error taxonomy is fully typed: no stringly-typed results in public signatures, no anyhow in library targets of the IO and telemetry crates, venue transport errors wrapped opaquely.
- Ingestion is framework-first: every shipped configuration runs the 12-venue framework engine; the legacy per-venue clients are deprecated with forwarding re-exports.
- Seeding derives from each adapter's declared reconstruction model — a single source of truth replaced string-matched venue special-casing.
- The private platform wire contract and the remote agent moved out of the workspace into their own repository; this repo builds from a bare clone with no private dependencies.
- Every venue's wire layer lives in `sources/<venue>/` — one folder per venue for all twelve: the six later venues' decoders, event enums, and response structs moved out of their adapter files into the same layout the original six always had; adapters are uniform composition files importing from `sources/`.

### Fixed

- Reconnect backoff resets on recovery (`on_connected`/`on_message_received` are wired into the framework loop), the circuit breaker is reachable, and all three reconnect mechanisms carry test coverage with an injected clock.
- Parquet readers reject malformed input with typed errors: an unknown trade side or malformed symbol is an error, never a panic and never a silently fabricated value.
- A misconfigured Parquet output (declared sink, no flusher) is a startup error instead of a silent data drop; flush failures retain the buffer for retry on every path.
- Adapters drop and count trades with unrecognized sides instead of coercing them.
- OKX book reconstruction migrated off the venue's deprecated checksum to sequence continuity — caught live by the conformance capture before it could gap-loop production.
- A live Bitso decode defect (trade timestamps read from the wrong wire field) was caught by the first independent-oracle conformance run and fixed the same day.

### Documented

- The README layer is the front door: root README with a credential-free live quickstart and real captured output, five real per-crate READMEs whose code blocks compile as doc tests in CI, an indexed example catalog with exact commands, and CONTRIBUTING with the true CI gate and the crates.io publish order.
