# OKX — Source Reference

OKX V5 public WebSocket source (spot). Public market-data channels require **no authentication**.

## Official Documentation

- WebSocket overview: <https://www.okx.com/docs-v5/en/#websocket-api>
- Order book channels: <https://www.okx.com/docs-v5/en/#websocket-api-public-channel-order-book-channel>
- Trades channel: <https://www.okx.com/docs-v5/en/#websocket-api-public-channel-trades-channel>

## Supported Data Types

| Data type   | Channel  | Wired |
|-------------|----------|-------|
| Order book  | `books5` | Yes — full top-5 snapshot, every 100 ms |
| Public trades | `trades` | Yes |
| Liquidations / Funding / Open interest | — | No — not wired for OKX spot |

## Order Book — `books5`

### Wire Protocol

`books5` pushes a **full top-5 snapshot every 100 ms** — no incremental
reconstruction, no checksum, no sequence reconciliation. The richer
incremental `books` channel (snapshot + deltas with `seqId`/`prevSeqId`
gap detection and a CRC32 checksum) is intentionally **not** used.

```json
{
  "arg": { "channel": "books5", "instId": "BTC-USDT" },
  "data": [{
    "asks": [["31685.1","0.0001","0","1"]],
    "bids": [["31684.9","0.01","0","1"]],
    "ts": "1626537446491", "seqId": 1234567
  }]
}
```

A level is a 4-element string array `[price, size, deprecated, numOrders]`.
Asks ascend by price; bids descend. `ts` is Unix milliseconds (string).

## Public Trades — `trades`

```json
{
  "arg": { "channel": "trades", "instId": "BTC-USDT" },
  "data": [{
    "instId": "BTC-USDT", "tradeId": "216970876",
    "px": "31684.5", "sz": "0.00001186", "side": "buy", "ts": "1626531038288"
  }]
}
```

### Response Fields

| Field     | Meaning |
|-----------|---------|
| `tradeId` | Exchange-assigned trade id (string) |
| `px`      | Price (string) |
| `sz`      | Size in base units (string) |
| `side`    | **Taker** direction: `buy` = lifted the ask, `sell` = hit the bid |
| `ts`      | Unix milliseconds (string) |

## Connection Details

- Endpoint: `wss://ws.okx.com:8443/ws/v5/public` (production, no auth)
- Subscribe frame: `{"op":"subscribe","args":[{"channel":"books5","instId":"BTC-USDT"}]}` — one arg per channel × instrument.
- Heartbeat: the client sends the **literal text** `ping` every 20 s; the server replies with literal `pong`. OKX disconnects after 30 s of client silence.

## Decoder Dispatch

| Frame                                | `OkxWssEvent` |
|--------------------------------------|---------------|
| `arg.channel` = `books*` / `bbo-tbt` | `OrderbookData` |
| `arg.channel` = `trades`             | `TradeData` |
| `event` = `subscribe` / `error`, or literal `pong` | control frame → `Ok(None)` |

## Config Reference

```toml
[collect]
exchange = "okx"

[collect.datatypes.orderbook]
enabled = true
depth = 50            # informational: books5 always returns top-5

[collect.datatypes.trades]
enabled = true

[[workers]]
symbol = "BTC-USDT"    # OKX instrument ids are hyphenated
```

See [`configs/md_worker_okx.toml`](../../../../aetelier-sdk/configs/md_worker_okx.toml) for a full runnable example.
