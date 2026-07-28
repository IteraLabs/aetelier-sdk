# aetelier-io

Columnar persistence for the aetelier engine: Parquet, JSON, and CSV writers and readers for every datatype, dataset tooling, and batch trade rehydration. The `parquet` feature gates the arrow/parquet dependency so `aetelier-connect` stays free of it.

## Design

Writers persist exactly what the engine reconstructed: order books, trades, liquidations, funding, and open interest land in per-datatype timestamped Parquet files. Decimal values collapse to Float64 at this boundary — the documented persistence convention. Readers return typed errors on malformed input: an unknown trade side or a malformed symbol is a `PersistError`, never a panic and never a fabricated default. The trades schema carries an `origin` column (ws | rest) appended last; readers tolerate its absence in files written before it existed.

## Module map

- `trades`, `orderbooks`, `liquidations`, `funding`, `open_interest` — per-datatype Parquet writers and readers plus JSON/CSV variants.
- `sink` — `ParquetSnapshotFlusher`, the all-or-nothing flush implementation behind the worker's buffered sink.
- `rehydrate` — batch repair: reads persisted trade parquets, fetches the venue's REST trades over the same id range, set-differences by id, and writes a dense `*_rehydrated` file beside the immutable originals, reporting anything retention makes unrecoverable.
- `flush` — the legacy extension-trait flush path, retained for compatibility.

## Usage

```text
cargo run -p aetelier-connect --example read_market_worker --features parquet -- --dir datasets/collected/bybit
```

reads a collection directory this crate wrote and prints per-symbol summary statistics.

## Tests

Golden-file tests cover the write and read paths, including corrupted-input rejection and rehydration hole-filling; CI runs the parquet feature leg on every push.
