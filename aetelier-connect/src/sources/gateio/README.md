# Gate.io — Source Reference

Gate.io V4 public WebSocket source (spot). Public market-data channels require **no authentication**.

## Official Documentation

- WebSocket v4 overview: <https://www.gate.io/docs/developers/apiv4/ws/en/>
- Order book channel: <https://www.gate.io/docs/developers/apiv4/ws/en/#limited-level-full-order-book-snapshot>
- Trades channel: <https://www.gate.io/docs/developers/apiv4/ws/en/#public-trades-channel>

## Supported Data Types

| Data type   | Channel           | Wired |
|-------------|-------------------|-------|
| Order book  | `spot.order_book` | Yes — full limited-depth snapshot, every 100 ms |
| Public trades | `spot.trades`   | Yes |
| Liquidations / Funding / Open interest | — | No — not wired for Gate.io spot |

## Order Book — `spot.order_book`

### Wire Protocol

`spot.order_book` pushes a **full limited-depth snapshot at a fixed
interval** (100 ms) — no incremental reconstruction. The incremental
`spot.order_book_update` channel (REST-seeded `U`/`u` sequencing) is
intentionally **not** used.

```json
{
  "time": 1606295412, "time_ms": 1606295412213,
  "channel": "spot.order_book", "event": "update",
  "result": {
    "t": 1606295412123, "lastUpdateId": 48791820, "s": "BTC_USDT",
    "bids": [["19079.55","0.0195"]], "asks": [["19080.24","0.1638"]]
  }
}
```

A level is a 2-element string array `[price, amount]`. `t` is the book
timestamp in Unix milliseconds.

### Depth

Subscriptions pass a depth `level` that Gate restricts to **1, 5, 10, 20,
50, 100**. The configured `depth` is snapped **up** to the nearest allowed
level at subscribe time.

## Public Trades — `spot.trades`

Unlike most venues, `result` is a **single** trade object (not an array).

```json
{
  "time": 1606292218, "time_ms": 1606292218231,
  "channel": "spot.trades", "event": "update",
  "result": {
    "id": 309143071, "create_time": 1606292218,
    "create_time_ms": "1606292218213.4578", "side": "sell",
    "currency_pair": "BTC_USDT", "amount": "16.47", "price": "0.4705"
  }
}
```

### Response Fields

| Field            | Meaning |
|------------------|---------|
| `id`             | Exchange-assigned trade id |
| `create_time_ms` | Unix milliseconds with a sub-ms fraction (string) |
| `side`           | **Taker** direction (`buy` / `sell`) |
| `amount`         | Size in base units (string) |
| `price`          | Price (string) |

## Connection Details

- Endpoint: `wss://api.gateio.ws/ws/v4/` (no auth)
- Subscribe: one frame **per channel** — `{"time":<unix_s>,"channel":"spot.order_book","event":"subscribe","payload":[pair, level, "100ms"]}`. `spot.trades` takes `payload:[pair]`.
- Heartbeat: the client sends an app-level `{"time":<unix_s>,"channel":"spot.ping"}` every 10 s; the `spot.pong` reply is consumed.

## Decoder Dispatch

Data frames carry `event == "update"` and a `channel`:

| Channel           | `GateioWssEvent` |
|-------------------|------------------|
| `spot.order_book` | `OrderbookData`  |
| `spot.trades`     | `TradeData`      |
| `event != "update"` (acks, `spot.pong`, errors) | control frame → `Ok(None)` |

## Config Reference

```toml
[collect]
exchange = "gateio"

[collect.datatypes.orderbook]
enabled = true
depth = 20            # snapped up to 1/5/10/20/50/100

[collect.datatypes.trades]
enabled = true

[[workers]]
symbol = "BTC_USDT"    # Gate.io pairs are underscored
```

See [`configs/md_worker_gateio.toml`](../../../../aetelier-sdk/configs/md_worker_gateio.toml) for a full runnable example.
