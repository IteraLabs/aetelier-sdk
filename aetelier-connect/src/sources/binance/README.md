
# Binance — Source Reference

## Official Documentation

| Resource | URL |
|---|---|
| Spot WebSocket Streams | <https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams> |
| Spot REST — Depth Snapshot | <https://developers.binance.com/docs/binance-spot-api-docs/rest-api#order-book> |
| Futures (USDⓈ-M) WebSocket | <https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams> |

---

## Supported Data Types

| Data Type | Implemented | WSS Stream | Notes |
|---|---|---|---|
| Order Book | Yes | `{sym}@depth@100ms` | Diff depth stream (full incremental deltas) |
| Public Trades | Yes | `{sym}@trade` | Individual trades, real-time |
| Liquidations | No | — | Available on Futures API only (REST) |
| Funding Rates | No | — | Available on Futures API only (REST) |
| Open Interest | No | — | Available on Futures API only (REST) |

---

## Order Book

### Wire Protocol

The implementation uses the **diff depth stream** (`{sym}@depth@100ms`), not the partial book depth stream (`{sym}@depth<levels>@100ms`).

| Property | Diff Depth (implemented) | Partial Book Depth (not used) |
|---|---|---|
| Stream name | `{sym}@depth@100ms` | `{sym}@depth5@100ms` / `@depth10` / `@depth20` |
| Valid depth values | N/A — returns all changed levels | **5, 10, 20** only |
| Payload | Only levels that changed since last push | Top N levels (full replacement) |
| Update speed | 100ms (configured), also supports 1000ms | 100ms or 1000ms |
| Requires REST snapshot | **Yes** — must seed with `/api/v3/depth` | No — each push is self-contained |

### Book Initialization Protocol

Because the diff depth stream delivers only incremental changes, a REST snapshot is required to establish the initial book state. The `BookInitializer` pipeline stage handles this:

```
1. Subscribe to {sym}@depth@100ms
2. Buffer incoming depthUpdate events
3. Concurrently fetch GET /api/v3/depth?symbol={SYM}&limit=5000
4. Emit the REST snapshot as a synthetic DepthSnapshot event
5. Reconcile buffered diffs:
   - Discard diffs where u ≤ snapshot.lastUpdateId (stale)
   - Forward diffs where u > snapshot.lastUpdateId (valid)
6. Transition to Synced state — forward all subsequent diffs directly
```

### Depth Filtering & Truncation

| Stage | Truncation | Explanation |
|---|---|---|
| **Wire (server-side)** | None | Diff depth stream sends all changed levels at any price depth |
| **In-memory (BTreeMap)** | `max_depth` pruning | After every `apply_snapshot()` and `apply_delta()`, `OrderbookDelta::prune_to_depth()` trims each side to at most `max_depth` levels. Lowest bids and highest asks are evicted first. When `max_depth` is `None` (default), no pruning occurs. |
| **Persistence (Parquet/CSV/JSON)** | Bounded by in-memory | `produce_snapshot()` persists whatever levels remain in the (already-pruned) BTreeMap |

The config field `depth` (e.g. `depth = 25` in TOML) is **not sent to the Binance API**. It serves two purposes:

- Internal topic naming: `orderbook.{depth}.{symbol}` as a canonical routing key
- Truncation ceiling: controls level capping at both data paths (see below)

This differs from Bybit and Kraken, where `depth` controls server-side filtering directly.

#### Truncation per data path

| Path | Where depth is applied | Mechanism |
|---|---|---|
| **MarketWorker** (`feed_binance()`) | `DepthSnapshot` only | `.take(ob_depth)` on the sorted REST arrays — positional truncation is correct because the snapshot arrives sorted best-to-worst. `DepthUpdate` (diff deltas) are **not truncated** because entries are unordered changed levels at arbitrary prices; positional truncation would silently drop valid updates. |
| **OrderbookDelta** (standalone / programmatic) | After every `process()` call | `prune_to_depth()` trims the BTreeMap by price — evicts lowest bids and highest asks, armed via `.with_max_depth(Some(depth))`. The framework path auto-arms this from `datatypes.orderbook.depth` (`SourceRuntime::with_max_depth`); checksum venues keep their recipe depth. |

> **Depth wiring (as-built):** the framework / persistence path auto-wires `datatypes.orderbook.depth` onto every reconstructed book via `SourceRuntime::with_max_depth` → `SourcedOrderbook::arm_config_depth`, so a standalone `OrderbookDelta` prunes to the subscribed depth without a manual `.with_max_depth`. Checksum venues (OKX/Kraken) are spared — their book is held at the checksum recipe depth. The legacy `MarketWorker` path applies `ob_depth` in `feed_binance()`.

### Sequence Reconciliation

Each `depthUpdate` event carries two sequence markers:

| Field | Serde Key | Meaning |
|---|---|---|
| `first_update_id` | `U` | First update ID in this event batch |
| `last_update_id` | `u` | Final update ID in this event batch |

Binance's documented reconciliation rule: drop events where `u ≤ snapshot.lastUpdateId`, then verify `U ≤ lastUpdateId+1 ≤ u` for the first valid event. Subsequent events must have contiguous `U`/`u` ranges.

---

## Public Trades

### Wire Protocol

| Property | Value |
|---|---|
| Stream name | `{sym}@trade` |
| Push frequency | Real-time (per-trade) |
| Event type field | `"e": "trade"` |
| Alternative stream | `{sym}@aggTrade` (not implemented — aggregates trades within 100ms) |

### Response Fields

| Field | Serde Key | Type | Description |
|---|---|---|---|
| `event_type` | `e` | String | Always `"trade"` |
| `event_time` | `E` | u64 | Event time (Unix ms) |
| `symbol` | `s` | String | e.g. `"BTCUSDT"` |
| `trade_id` | `t` | u64 | Exchange-assigned trade ID |
| `price` | `p` | String | Trade price (string for precision) |
| `quantity` | `q` | String | Trade quantity (string for precision) |
| `trade_time` | `T` | u64 | Trade execution time (Unix ms) |
| `is_buyer_maker` | `m` | bool | `true` → taker sold; `false` → taker bought |

---

## Connection Details

| Property | Value |
|---|---|
| Endpoint | `wss://stream.binance.com:9443/ws` |
| Auth required | No (public streams) |
| Subscription method | `{"method": "SUBSCRIBE", "params": [...], "id": 1}` |
| Keep-alive interval | 15s (client-side pong) |
| Server ping interval | Every 20s (must pong within 60s) |

### Rate Limits & Connection Constraints

| Constraint | Limit |
|---|---|
| Connections per 5 min per IP | 300 |
| Streams per connection | 1024 |
| Inbound messages per second | 5 |
| Connection lifetime | 24 hours (auto-disconnect) |
| Ping/pong timeout | Pong within 60s of server ping |

---

## Decoder Dispatch

The `BinanceDecoder` routes on the `"e"` (event type) field:

| Event type value | Mapped to | Notes |
|---|---|---|
| `"depthUpdate"` | `BinanceWssEvent::DepthUpdate` | From `@depth@100ms` stream |
| `"trade"` | `BinanceWssEvent::TradeData` | From `@trade` stream |
| (no `"e"` field) | `Ok(None)` — silently consumed | Subscription acks, errors |

`BinanceWssEvent::DepthSnapshot` is **never produced by the decoder** — it is synthesized by the `BookInitializer` pipeline stage from the REST response.

---

## Config Reference

```toml
[collect]
exchange = "binance"

[collect.datatypes.orderbook]
enabled = true
depth = 25          # Internal topic label only — NOT sent to API

[collect.datatypes.trades]
enabled = true

[collect.datatypes.liquidations]
enabled = false     # Not implemented for Binance

[collect.datatypes.funding_rates]
enabled = false     # Not implemented for Binance

[collect.datatypes.open_interest]
enabled = false     # Not implemented for Binance
```
