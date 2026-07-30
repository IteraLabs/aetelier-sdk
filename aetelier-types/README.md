# aetelier-types

The canonical schema crate of the aetelier workspace: every market-data type the engine speaks — trades, order books, liquidations, funding, open interest, trading pairs, worker configuration — with zero IO and `forbid(unsafe_code)`. Downstream crates depend on these definitions; nothing here opens a socket or a file.

## Design

Timestamps are UTC epoch microseconds platform-wide, carried in `_us`-suffixed fields. Trade and liquidation amounts and prices are exact `rust_decimal::Decimal` in memory, serialized as JSON floats and persisted as Parquet Float64 at the IO boundary. Builders validate every field and return typed errors; an incomplete build names the missing field.

```rust
use aetelier_types::trades::{Trade, TradeSide};
use aetelier_types::trading_pair::TradingPair;

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

## Module map

- `trades` — `Trade`, `TradeSide`, `TradeOrigin` (ws | rest provenance), builder.
- `orderbooks` — `Orderbook` (BTreeMap levels, best bid/ask/mid accessors), `Level`, decimal conversion helpers.
- `liquidations`, `funding`, `open_interest` — the extended datatypes.
- `trading_pair` — canonical `BASE/QUOTE` pairs with venue codec support.
- `config` — worker and market manifests (TOML-backed).
- `errors` — the typed error taxonomy shared by builders and IO.
- `time` — `TimestampUs`, the platform timestamp newtype.

## Tests

Unit tests ride each module; the workspace CI runs them on every push alongside doc tests, clippy with denied warnings, and format checks.
