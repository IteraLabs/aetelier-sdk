//! Aggregated per-snapshot statistics.
//!
//! [`MarketAggregate`] reduces each [`MarketSnapshot`] to scalar features
//! suitable for low-dimensional analysis, feature engineering, and efficient
//! Parquet storage. The trades section carries the taker-order-flow view
//! (per-side volume, notional, count, and imbalance) so a downstream model
//! sees the same order-flow signal the offline
//! [`TradeAggregate`](crate::trades::TradeAggregate) exposes.

use crate::orderbooks::decimal_to_f64;
use crate::snapshots::MarketSnapshot;
use crate::trades::TradeSide;
use serde::{Deserialize, Serialize};

/// Aggregated statistics for a single [`MarketSnapshot`] period.
///
/// Each field summarizes one aspect of the market state during the
/// grid-aligned period. There are 3 statistics per data source:
///
/// - **Orderbook**: mid-price, spread, volume imbalance
/// - **Trades**: volume, VWAP, count
/// - **Liquidations**: notional, count, directional imbalance
/// - **Funding rate**: rate, annualized rate, time-to-settlement
/// - **Open interest**: contracts, value, change from previous
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketAggregate {
    /// Grid-aligned timestamp in microseconds (from the source snapshot).
    pub ts_us: u64,

    // -- Orderbook -----------------------------------------------------------
    /// Mid-price: `(best_bid + best_ask) / 2`. Zero if no orderbook.
    pub ob_mid_price: f64,
    /// Spread: `best_ask - best_bid`. Zero if no orderbook.
    pub ob_spread: f64,
    /// Volume imbalance over all levels:
    /// `(bid_vol - ask_vol) / (bid_vol + ask_vol)`.
    /// Range [-1, 1]. Zero if no orderbook or zero total volume.
    pub ob_imbalance: f64,

    // -- Trades --------------------------------------------------------------
    /// Total trade volume (sum of `amount`). Zero if no trades.
    pub trade_volume: f64,
    /// Volume-weighted average price: `sum(price * amount) / sum(amount)`.
    /// Zero if no trades.
    pub trade_vwap: f64,
    /// Number of trades in this period.
    pub trade_count: u64,
    /// Total traded notional: `sum(price * amount)`. Zero if no trades.
    pub trade_notional: f64,
    /// Buy-side (taker lifted the ask) volume. Zero if none.
    pub trade_buy_volume: f64,
    /// Buy-side notional. Zero if none.
    pub trade_buy_notional: f64,
    /// Number of buy-side trades.
    pub trade_buy_count: u64,
    /// Sell-side (taker hit the bid) volume. Zero if none.
    pub trade_sell_volume: f64,
    /// Sell-side notional. Zero if none.
    pub trade_sell_notional: f64,
    /// Number of sell-side trades.
    pub trade_sell_count: u64,
    /// Order-flow imbalance over volume:
    /// `(buy_volume - sell_volume) / (buy_volume + sell_volume)`.
    /// Range [-1, 1]. Zero if no trades.
    pub trade_imbalance: f64,
    /// Price of the first trade in the period. Zero if no trades.
    pub trade_px_first: f64,
    /// Price of the last trade in the period. Zero if no trades.
    pub trade_px_last: f64,

    // -- Liquidations --------------------------------------------------------
    /// Total liquidation notional: `sum(price * amount)`. Zero if none.
    pub liq_notional: f64,
    /// Number of liquidation events.
    pub liq_count: u64,
    /// Directional imbalance:
    /// `(buy_notional - sell_notional) / total_notional`.
    /// Zero if no liquidations.
    pub liq_imbalance: f64,

    // -- Funding Rate --------------------------------------------------------
    /// Latest funding rate in the period. Zero if none received.
    pub fr_rate: f64,
    /// Annualized funding rate: `rate * 3 * 365` (assumes 3 settlements/day).
    pub fr_annualized: f64,
    /// Microseconds until next settlement:
    /// `next_funding_ts_us - funding_rate_ts_us`. Zero if no funding data.
    /// Can be negative if past settlement.
    pub fr_next_settlement_delta: i64,

    // -- Open Interest -------------------------------------------------------
    /// Open interest in contract units. Zero if none received.
    pub oi_contracts: f64,
    /// Open interest in quote currency (USD value). Zero if none received.
    pub oi_value: f64,
    /// Change in OI contracts from the previous snapshot.
    /// Zero for the first snapshot in a sequence.
    pub oi_change: f64,
}

impl MarketAggregate {
    /// Compute aggregate statistics from a single [`MarketSnapshot`].
    ///
    /// `prev_oi` is the OI contracts value from the preceding snapshot,
    /// used to compute `oi_change`. Pass `0.0` for the first snapshot.
    pub fn from_snapshot(snap: &MarketSnapshot, prev_oi: f64) -> Self {
        // -- Orderbook -------------------------------------------------------
        let (ob_mid_price, ob_spread, ob_imbalance) = match &snap.orderbook {
            Some(ob) => {
                // Best bid = highest price in bids BTreeMap
                let best_bid = ob.best_bid().map(decimal_to_f64).unwrap_or(0.0);
                // Best ask = lowest price in asks BTreeMap
                let best_ask = ob.best_ask().map(decimal_to_f64).unwrap_or(0.0);

                let mid = if best_bid > 0.0 && best_ask > 0.0 {
                    (best_bid + best_ask) / 2.0
                } else {
                    0.0
                };
                let spread = if best_bid > 0.0 && best_ask > 0.0 {
                    best_ask - best_bid
                } else {
                    0.0
                };

                let bid_vol: f64 =
                    ob.bids.values().map(|l| decimal_to_f64(l.volume)).sum();
                let ask_vol: f64 =
                    ob.asks.values().map(|l| decimal_to_f64(l.volume)).sum();
                let total_vol = bid_vol + ask_vol;
                let imbalance = if total_vol > 0.0 {
                    (bid_vol - ask_vol) / total_vol
                } else {
                    0.0
                };

                (mid, spread, imbalance)
            }
            None => (0.0, 0.0, 0.0),
        };

        // -- Trades ----------------------------------------------------------
        // One pass over the period's trades, split by taker side.
        let trade_count = snap.trades.len() as u64;
        let mut trade_buy_volume = 0.0;
        let mut trade_buy_notional = 0.0;
        let mut trade_buy_count = 0u64;
        let mut trade_sell_volume = 0.0;
        let mut trade_sell_notional = 0.0;
        let mut trade_sell_count = 0u64;
        for t in &snap.trades {
            let amount = decimal_to_f64(t.amount);
            let notional = decimal_to_f64(t.price) * amount;
            match t.side {
                TradeSide::Buy => {
                    trade_buy_volume += amount;
                    trade_buy_notional += notional;
                    trade_buy_count += 1;
                }
                TradeSide::Sell => {
                    trade_sell_volume += amount;
                    trade_sell_notional += notional;
                    trade_sell_count += 1;
                }
            }
        }
        let trade_volume = trade_buy_volume + trade_sell_volume;
        let trade_notional = trade_buy_notional + trade_sell_notional;
        let trade_vwap = if trade_volume > 0.0 {
            trade_notional / trade_volume
        } else {
            0.0
        };
        let trade_imbalance = if trade_volume > 0.0 {
            (trade_buy_volume - trade_sell_volume) / trade_volume
        } else {
            0.0
        };
        let trade_px_first = snap
            .trades
            .first()
            .map(|t| decimal_to_f64(t.price))
            .unwrap_or(0.0);
        let trade_px_last = snap
            .trades
            .last()
            .map(|t| decimal_to_f64(t.price))
            .unwrap_or(0.0);

        // -- Liquidations ----------------------------------------------------
        let liq_count = snap.liquidations.len() as u64;
        let mut buy_notional: f64 = 0.0;
        let mut sell_notional: f64 = 0.0;
        for liq in &snap.liquidations {
            let notional = decimal_to_f64(liq.price) * decimal_to_f64(liq.amount);
            match liq.side {
                TradeSide::Buy => buy_notional += notional,
                TradeSide::Sell => sell_notional += notional,
            }
        }
        let liq_notional = buy_notional + sell_notional;
        let liq_imbalance = if liq_notional > 0.0 {
            (buy_notional - sell_notional) / liq_notional
        } else {
            0.0
        };

        // -- Funding Rate ----------------------------------------------------
        let (fr_rate, fr_annualized, fr_next_settlement_delta) =
            match snap.funding_rate.last() {
                Some(fr) => {
                    let rate = fr.funding_rate;
                    let annualized = rate * 3.0 * 365.0;
                    let delta = if fr.next_funding_ts_us > 0 {
                        fr.next_funding_ts_us as i64 - fr.funding_rate_ts_us as i64
                    } else {
                        0
                    };
                    (rate, annualized, delta)
                }
                None => (0.0, 0.0, 0),
            };

        // -- Open Interest ---------------------------------------------------
        let (oi_contracts, oi_value) = match snap.open_interest.last() {
            Some(oi) => (oi.open_interest, oi.open_interest_value),
            None => (0.0, 0.0),
        };
        let oi_change = oi_contracts - prev_oi;

        Self {
            ts_us: snap.ts_us,
            ob_mid_price,
            ob_spread,
            ob_imbalance,
            trade_volume,
            trade_vwap,
            trade_count,
            trade_notional,
            trade_buy_volume,
            trade_buy_notional,
            trade_buy_count,
            trade_sell_volume,
            trade_sell_notional,
            trade_sell_count,
            trade_imbalance,
            trade_px_first,
            trade_px_last,
            liq_notional,
            liq_count,
            liq_imbalance,
            fr_rate,
            fr_annualized,
            fr_next_settlement_delta,
            oi_contracts,
            oi_value,
            oi_change,
        }
    }
}
