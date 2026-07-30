
## Bybit Manual

- **Official Docs (Perps):** <https://bybit-exchange.github.io/docs/v5/ws/connect>

### Perps

### Order Book — `orderbook.{depth}.{symbol}`

| Category | Valid Depths | Push Frequency |
|---|---|---|
| Linear / Spot / Inverse | **1, 50, 200, 1000** | 10ms, 20ms, 100ms, 200ms |
| Options | **25, 100** | 20ms, 100ms |

Snapshot on initial subscription, then deltas. Depth-1 snapshots are re-sent every 3s even if unchanged. A qty of `0` means remove the level. RPI orders excluded.

### Public Trades — `publicTrade.{symbol}`

Options use `publicTrade.{baseCoin}`. Real-time push with up to 1024 trades per message. No explicit rate limit on public WS streams.

### Liquidations — `allLiquidation.{symbol}`

Push frequency: 500ms. Supported on **linear and inverse only**. Fields: timestamp, symbol, side (Buy = long liquidation), executed size, bankruptcy price.

### Open Interest & Funding Rates — via `tickers.{symbol}`

No dedicated OI or funding websocket — both are delivered through the ticker stream. Fields include `openInterest`, `openInterestValue`, `fundingRate`, `nextFundingTime`, `fundingIntervalHour`, `fundingCap`. Ticker push: 100ms derivatives, 50ms spot.

### Connection Limits

- Max 500 connections per 5 min per domain
- Max 1000 per IP for market data
- 21,000 char limit on subscription args
- Heartbeat ping every 20s, disconnects after 10 min idle

