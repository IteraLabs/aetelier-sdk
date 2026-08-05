# aetelier datasets

Small, real captures of exchange wire data — the reproducible ground truth the
test suite replays. Every file here is bytes an exchange actually sent (public
market data, no credentials), captured over the venue's real protocol, trimmed
to a snapshot-anchored window, and committed so anyone can reproduce the tests
against the exact same input.

## Why these exist

Hand-authored JSON that *mimics* a venue's wire format drifts from reality and
can't catch a real protocol change. These captures let the decoder/normalizer
tests assert against genuine frames instead — the input is real, and the
expected values are pinned from the captured frame, so the oracle stays exact.
The venue conformance suite (`tests/conformance.rs`) and the per-venue parsing
tests (`tests/sources/`) both read from here.

## Provenance

Captured live via `examples/capture_fixture` (which drives each adapter's real
`ProtocolHooks` — endpoint, subscribe frames, heartbeat, frame codec — the same
path the runtime uses) for the WSS streams, and the venues' REST endpoints for
the seed snapshots. Symbol is BTC-quoted per venue (Upbit is KRW-BTC;
Hyperliquid is the bare perp coin `BTC`). Captured 2026-07 across the venue
conformance cycles; hyperliquid captured 2026-08-05 (cycle #5).

| Venue | File | Kind | Frames | Size |
|---|---|---|---:|---:|
| binance | `binance/btcusdt_depth_trade.jsonl` | WSS depth + trade | 1500 | 402K |
| binance | `binance/btcusdt_rest_snapshot.json` | REST depth snapshot | 1 | 312K |
| bitget | `bitget/btcusdt_book_trade.jsonl` | WSS book + trade | 700 | 303K |
| bitso | `bitso/btcmxn_book_trade.jsonl` | WSS L3 diff + trade | 915 | 197K |
| bitso | `bitso/btcmxn_rest_l3.json` | REST L3 snapshot (top-300/side) | 1 | 48K |
| bybit | `bybit/btcusdt_book_trade.jsonl` | WSS book + trade | 800 | 203K |
| coinbase | `coinbase/btcusd_book_trade.jsonl` | WSS book + trade | 201 | 895K |
| coinbase | `coinbase/btcusd_l2_heartbeats.jsonl` | WSS book-socket window: level2 + heartbeats + the 2 sequenced acks (captured 2026-07-16; snapshot depth-truncated to 150/side; `sequence_num` contiguous 0..799) | 800 | 705K |
| gateio | `gateio/btcusdt_book_trade.jsonl` | WSS book + trade | 400 | 379K |
| htx | `htx/btcusdt_book_trade.jsonl` | WSS book + trade (gzip, mid-stream REQ seed) | 231 | 127K |
| htx | `htx/btcusdt_trades_density.jsonl` | trade.detail frames only, selected from a live 600s window (2026-07-16; 50 trades, tradeId dense +1 after per-tick reverse — the loss-accounting density evidence) | 43 | 12K |
| hyperliquid | `hyperliquid/btc_book_trade.jsonl` | WSS full-book + trade (600s window, captured 2026-08-05; 112 l2Book snapshots all 20×20, 728 trade frames / 1345 prints, sides A:796 B:549; perp `BTC`, decimal-string px/sz) | 861 | 560K |
| kraken | `kraken/btcusd_book_trade.jsonl` | WSS book + trade | 1800 | 326K |
| kraken | `kraken/books_btcusd.jsonl` | WSS book (CRC32 reference) | 146 | 27K |
| kucoin | `kucoin/btcusdt_book_trade.jsonl` | WSS book + trade | 707 | 187K |
| kucoin | `kucoin/btcusdt_rest_l2.json` | REST L2 snapshot (mid-stream) | 1 | 4K |
| okx | `okx/btcusdt_book_trade.jsonl` | WSS book + trade | 600 | 278K |
| okx | `okx/books_btcusdt.jsonl` | WSS book (CRC32 reference) | 59 | 50K |
| poloniex | `poloniex/btcusdt_book_trade.jsonl` | WSS book + trade | 700 | 145K |
| upbit | `upbit/krwbtc_book_trade.jsonl` | WSS full-book + trade | 250 | 560K |

The `books_*` captures (kraken, okx) carry real per-frame CRC32 checksums and
bless the checksum algorithms in `src/framework/checksum.rs`; the rest are
combined book+trade windows used for reconstruction and normalization.

## Regenerating a capture

```
cargo run --example capture_fixture -- <venue> <wire_symbol> <seconds> <out.jsonl>
```

Coinbase runs split sockets in production: the `coinbase` venue arg captures
the book socket (level2 + heartbeats, the sequence-tracked one) and
`coinbase-trades` captures the market_trades socket.

Binance's parity capture (REST snapshot + a depth/trade window whose
`lastUpdateId` lands inside the delta stream) has a dedicated helper:
`datasets/binance/capture.sh`.

Seed-alignment note: for sequence-seeded venues the seed must land *inside* the
captured delta window, or offline reconstruction can't bridge to it. KuCoin's
REST `level2_100` snapshot is fetched a few seconds into the WSS capture; HTX's
in-band REQ is sent mid-stream by the capture bin (at t≈6s) for the same reason.
Bitso is L3 (order-keyed, seeded 0-based), so its REST snapshot is
alignment-immune.

