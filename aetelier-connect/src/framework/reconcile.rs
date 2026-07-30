//! Live trade reconciliation — the metered half of the completeness product.
//!
//! When the sentinel proves (or suspects) a gap, the reconciler fetches the
//! venue's REST trades AFTER the last print the live stream delivered, and
//! injects any missing prints back into the synchronized stream stamped
//! [`TradeOrigin::Rest`]. Combined with the synchronizer's emission hold-back
//! window W, recovered prints land in their true — still buffered — grid rows.
//!
//! Triggering is bounded by design: a fetch runs on a gap-recovery incident
//! (once per incident, paced by the reconnect ladder) plus an optional slow
//! periodic sweep. NEVER per-trade/per-jump polling — on venues with global
//! trade counters that would poll on every print.
//!
//! Venue coverage (probed matrix, 2026-07-17): id-anchored exact
//! fetches for Binance (`fromId`, keyless) and Bitso (`marker&sort=asc`);
//! time-anchored fetch with id filtering for Coinbase (`start`/`end`, dense
//! ids, 100/page). Venues whose REST retention cannot repair a gap (bybit 60,
//! poloniex 1000, htx ~2h, kucoin ~100) simply have no fetcher here — the
//! sentinel still reports, honestly, that their gaps are unrecoverable live.

use std::sync::Arc;
use std::time::Duration;

use aetelier_types::trades::{Trade, TradeOrigin, TradeSide};
use aetelier_types::trading_pair::TradingPair;

use crate::errors::ExchangeError;
use crate::framework::model::epoch_to_us;

/// Per-request timeout — a hung venue REST call must never stall the loop.
const FETCH_TIMEOUT_SECS: u64 = 10;

/// Cap on pages walked in one reconcile pass (a 90s outage on a very active
/// book stays comfortably inside this; deeper holes are batch-rehydration's
/// job).
const MAX_PAGES: usize = 5;

/// The position the reconciler fetches AFTER: the last trade the live stream
/// delivered for a pair, as (venue id, venue ts µs).
#[derive(Debug, Clone, Copy, Default)]
pub struct TradePos {
    pub id: u64,
    pub ts_us: u64,
}

/// Fetch venue trades strictly after `pos` for `wire_symbol`, ascending by
/// venue id. Implemented per venue over the REST coverage matrix.
#[async_trait::async_trait]
pub trait TradesRestFetch: Send + Sync {
    async fn fetch_after(
        &self,
        wire_symbol: &str,
        pair: &TradingPair,
        pos: TradePos,
    ) -> Result<Vec<Trade>, ExchangeError>;
}

fn http() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(FETCH_TIMEOUT_SECS))
        .build()
        .expect("reqwest client builds with a timeout")
}

fn invalid(msg: String) -> ExchangeError {
    ExchangeError::IoError(std::io::Error::new(std::io::ErrorKind::InvalidData, msg))
}

/// The venue fetcher registry. `None` = the venue's REST retention cannot
/// repair a live gap (documented above) — reconciliation is honestly
/// unavailable there.
pub fn trades_rest_fetcher(venue: &str) -> Option<Arc<dyn TradesRestFetch>> {
    match venue {
        "binance" => Some(Arc::new(BinanceTradesFetch)),
        "bitso" => Some(Arc::new(BitsoTradesFetch)),
        "coinbase" => Some(Arc::new(CoinbaseTradesFetch)),
        _ => None,
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Binance — GET /api/v3/historicalTrades?symbol&fromId&limit (keyless,
// id-anchored exact; probed live 2026-07-17)
// ─────────────────────────────────────────────────────────────────────────

pub struct BinanceTradesFetch;

#[async_trait::async_trait]
impl TradesRestFetch for BinanceTradesFetch {
    async fn fetch_after(
        &self,
        wire_symbol: &str,
        pair: &TradingPair,
        pos: TradePos,
    ) -> Result<Vec<Trade>, ExchangeError> {
        #[derive(serde::Deserialize)]
        struct Row {
            id: u64,
            price: String,
            qty: String,
            time: u64,
            #[serde(rename = "isBuyerMaker")]
            is_buyer_maker: bool,
        }
        let client = http();
        let mut out = Vec::new();
        let mut from = pos.id.saturating_add(1);
        for _ in 0..MAX_PAGES {
            let url = format!(
                "https://api.binance.com/api/v3/historicalTrades?symbol={wire_symbol}&fromId={from}&limit=1000"
            );
            let rows: Vec<Row> = client
                .get(&url)
                .send()
                .await
                .map_err(|e| invalid(format!("binance trades fetch: {e}")))?
                .error_for_status()
                .map_err(|e| invalid(format!("binance trades fetch: {e}")))?
                .json()
                .await
                .map_err(|e| invalid(format!("binance trades parse: {e}")))?;
            let n = rows.len();
            for r in rows {
                if r.id <= pos.id {
                    continue;
                }
                from = from.max(r.id + 1);
                out.push(Trade {
                    source_trade_ts_us: epoch_to_us(r.time),
                    local_trade_ts_us: 0,
                    source_trade_rtt_us: 0,
                    pair: pair.clone(),
                    // The taker is the non-maker side: buyer-is-maker ⇒ the
                    // taker sold.
                    side: if r.is_buyer_maker {
                        TradeSide::Sell
                    } else {
                        TradeSide::Buy
                    },
                    amount: r.qty.parse().unwrap_or_default(),
                    price: r.price.parse().unwrap_or_default(),
                    exchange: "binance".to_string(),
                    id: r.id.to_string(),
                    origin: TradeOrigin::Rest,
                });
            }
            if n < 1000 {
                break; // caught up to the live head
            }
        }
        Ok(out)
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Bitso — GET /v3/trades?book&marker&sort=asc&limit=100 (id-anchored exact;
// tid == the WSS `i`, verified live)
// ─────────────────────────────────────────────────────────────────────────

pub struct BitsoTradesFetch;

#[async_trait::async_trait]
impl TradesRestFetch for BitsoTradesFetch {
    async fn fetch_after(
        &self,
        wire_symbol: &str,
        pair: &TradingPair,
        pos: TradePos,
    ) -> Result<Vec<Trade>, ExchangeError> {
        #[derive(serde::Deserialize)]
        struct Row {
            tid: u64,
            price: String,
            amount: String,
            maker_side: String,
            created_at: String,
        }
        #[derive(serde::Deserialize)]
        struct Envelope {
            success: bool,
            #[serde(default)]
            payload: Vec<Row>,
        }
        let client = http();
        let mut out = Vec::new();
        let mut marker = pos.id;
        for _ in 0..MAX_PAGES {
            let url = format!(
                "https://api.bitso.com/v3/trades/?book={wire_symbol}&marker={marker}&sort=asc&limit=100"
            );
            let env: Envelope = client
                .get(&url)
                .send()
                .await
                .map_err(|e| invalid(format!("bitso trades fetch: {e}")))?
                .error_for_status()
                .map_err(|e| invalid(format!("bitso trades fetch: {e}")))?
                .json()
                .await
                .map_err(|e| invalid(format!("bitso trades parse: {e}")))?;
            if !env.success {
                return Err(invalid("bitso trades fetch: success=false".into()));
            }
            let n = env.payload.len();
            for r in env.payload {
                if r.tid <= pos.id {
                    continue;
                }
                marker = marker.max(r.tid);
                let ts_ms = chrono::DateTime::parse_from_str(
                    &r.created_at,
                    "%Y-%m-%dT%H:%M:%S%z",
                )
                .map(|dt| dt.timestamp_millis() as u64)
                .unwrap_or(0);
                out.push(Trade {
                    source_trade_ts_us: epoch_to_us(ts_ms),
                    local_trade_ts_us: 0,
                    source_trade_rtt_us: 0,
                    pair: pair.clone(),
                    // Taker is the opposite of the maker side.
                    side: if r.maker_side == "buy" {
                        TradeSide::Sell
                    } else {
                        TradeSide::Buy
                    },
                    amount: r.amount.parse().unwrap_or_default(),
                    price: r.price.parse().unwrap_or_default(),
                    exchange: "bitso".to_string(),
                    id: r.tid.to_string(),
                    origin: TradeOrigin::Rest,
                });
            }
            if n < 100 {
                break;
            }
        }
        Ok(out)
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Coinbase — GET /api/v3/brokerage/market/products/{id}/ticker?limit&start&end
// (time-anchored, dense ids let us filter exactly; 100/page — walk windows)
// ─────────────────────────────────────────────────────────────────────────

pub struct CoinbaseTradesFetch;

#[async_trait::async_trait]
impl TradesRestFetch for CoinbaseTradesFetch {
    async fn fetch_after(
        &self,
        wire_symbol: &str,
        pair: &TradingPair,
        pos: TradePos,
    ) -> Result<Vec<Trade>, ExchangeError> {
        #[derive(serde::Deserialize)]
        struct Row {
            trade_id: String,
            price: String,
            size: String,
            time: String,
            side: String,
        }
        #[derive(serde::Deserialize)]
        struct Envelope {
            #[serde(default)]
            trades: Vec<Row>,
        }
        let client = http();
        let mut out: Vec<Trade> = Vec::new();
        // Window walk: from just before the last delivered print to now.
        let mut start_sec = (pos.ts_us / 1_000_000).saturating_sub(1);
        let now_sec = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        for _ in 0..MAX_PAGES {
            if start_sec >= now_sec {
                break;
            }
            let url = format!(
                "https://api.coinbase.com/api/v3/brokerage/market/products/{wire_symbol}/ticker?limit=100&start={start_sec}&end={now_sec}"
            );
            let env: Envelope = client
                .get(&url)
                .send()
                .await
                .map_err(|e| invalid(format!("coinbase trades fetch: {e}")))?
                .error_for_status()
                .map_err(|e| invalid(format!("coinbase trades fetch: {e}")))?
                .json()
                .await
                .map_err(|e| invalid(format!("coinbase trades parse: {e}")))?;
            if env.trades.is_empty() {
                break;
            }
            let mut max_ts_sec = start_sec;
            for r in &env.trades {
                let id: u64 = r.trade_id.parse().unwrap_or(0);
                if id <= pos.id {
                    continue;
                }
                let Some(side) = TradeSide::from_str_loose(&r.side) else {
                    continue;
                };
                let ts_us = chrono::DateTime::parse_from_rfc3339(&r.time)
                    .map(|dt| dt.timestamp_micros() as u64)
                    .unwrap_or(0);
                max_ts_sec = max_ts_sec.max(ts_us / 1_000_000);
                if out.iter().any(|t| t.id == r.trade_id) {
                    continue;
                }
                out.push(Trade {
                    source_trade_ts_us: ts_us,
                    local_trade_ts_us: 0,
                    source_trade_rtt_us: 0,
                    pair: pair.clone(),
                    side,
                    amount: r.size.parse().unwrap_or_default(),
                    price: r.price.parse().unwrap_or_default(),
                    exchange: "coinbase".to_string(),
                    id: r.trade_id.clone(),
                    origin: TradeOrigin::Rest,
                });
            }
            // The ticker returns the NEWEST 100 in the window; if the window
            // holds more, narrow from the front by advancing start past the
            // oldest fetched print. Stop once a pass adds nothing new.
            if env.trades.len() < 100 || max_ts_sec <= start_sec {
                break;
            }
            start_sec = max_ts_sec;
        }
        out.sort_by(|a, b| {
            let ia: u64 = a.id.parse().unwrap_or(0);
            let ib: u64 = b.id.parse().unwrap_or(0);
            ia.cmp(&ib)
        });
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fetcher_registry_matches_the_probed_matrix() {
        for venue in ["binance", "bitso", "coinbase"] {
            assert!(trades_rest_fetcher(venue).is_some(), "{venue} fetchable");
        }
        // Recent-window-only venues are honestly absent.
        for venue in ["bybit", "poloniex", "htx", "kucoin", "upbit"] {
            assert!(
                trades_rest_fetcher(venue).is_none(),
                "{venue} must not pretend to be repairable live"
            );
        }
    }
}
