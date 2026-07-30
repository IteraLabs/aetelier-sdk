#!/usr/bin/env bash
# Regenerate the Binance parity fixtures: a REST depth snapshot plus a window of
# WSS depth+trade frames whose ids straddle the snapshot's lastUpdateId.
#
# Usage: ./capture_binance_fixture.sh [symbol] [seconds]   (defaults: btcusdt 8)
# Requires: websocat, curl. Writes <symbol>_depth_trade.jsonl + <symbol>_rest_snapshot.json.
set -euo pipefail

SYMBOL="${1:-btcusdt}"
DUR="${2:-8}"
DIR="$(cd "$(dirname "$0")" && pwd)"
UPPER="$(printf '%s' "$SYMBOL" | tr '[:lower:]' '[:upper:]')"
SUB="{\"method\":\"SUBSCRIBE\",\"params\":[\"${SYMBOL}@depth@100ms\",\"${SYMBOL}@trade\"],\"id\":1}"

# Fetch the snapshot ~2s in, so its lastUpdateId lands inside the delta stream.
( sleep 2 && curl -s "https://api.binance.com/api/v3/depth?symbol=${UPPER}&limit=5000" \
    > "$DIR/${SYMBOL}_rest_snapshot.json" ) &

# timeout wraps websocat directly so the capture always terminates.
{ printf '%s\n' "$SUB"; sleep "$DUR"; } \
  | timeout "$((DUR + 2))" websocat "wss://stream.binance.com:9443/ws" \
  > "$DIR/${SYMBOL}_depth_trade.jsonl"
wait

echo "captured $(wc -l < "$DIR/${SYMBOL}_depth_trade.jsonl") frames -> $DIR/${SYMBOL}_depth_trade.jsonl"
