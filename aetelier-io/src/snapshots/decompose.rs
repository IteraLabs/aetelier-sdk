use std::collections::HashSet;

use aetelier_types::funding::{FundingRate, FundingSettlement};
use aetelier_types::liquidations::Liquidation;
use aetelier_types::open_interest::OpenInterest;
use aetelier_types::orderbooks::Orderbook;
use aetelier_types::snapshots::MarketSnapshot;
use aetelier_types::trades::Trade;

#[derive(Default)]
pub struct DecomposedSnapshots {
    pub orderbooks: Vec<Orderbook>,
    pub trades: Vec<Trade>,
    pub liquidations: Vec<Liquidation>,
    pub funding_rates: Vec<FundingRate>,
    pub open_interests: Vec<OpenInterest>,
    pub funding_settlements: Vec<FundingSettlement>,
}

pub fn decompose_snapshots(snapshots: &[MarketSnapshot]) -> DecomposedSnapshots {
    let mut out = DecomposedSnapshots::default();
    let mut seen_fr: HashSet<(String, String, u64, u64)> = HashSet::new();
    let mut seen_oi: HashSet<(String, String, u64, u64)> = HashSet::new();
    let mut seen_fs: HashSet<(String, String, u64)> = HashSet::new();

    for snap in snapshots {
        if let Some(ob) = &snap.orderbook {
            out.orderbooks.push(ob.clone());
        }
        out.trades.extend(snap.trades.iter().cloned());
        out.liquidations.extend(snap.liquidations.iter().cloned());
        for fr in &snap.funding_rate {
            let provenance_free = fr.recv_seq == 0;
            if provenance_free
                || seen_fr.insert((
                    fr.exchange.clone(),
                    fr.pair.to_canonical(),
                    fr.conn_epoch_us,
                    fr.recv_seq,
                ))
            {
                out.funding_rates.push(fr.clone());
            }
        }
        for oi in &snap.open_interest {
            let provenance_free = oi.recv_seq == 0;
            if provenance_free
                || seen_oi.insert((
                    oi.exchange.clone(),
                    oi.pair.to_canonical(),
                    oi.conn_epoch_us,
                    oi.recv_seq,
                ))
            {
                out.open_interests.push(oi.clone());
            }
        }
        for fs in &snap.funding_settlements {
            if seen_fs.insert((
                fs.exchange.clone(),
                fs.pair.to_canonical(),
                fs.funding_time_us,
            )) {
                out.funding_settlements.push(fs.clone());
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use aetelier_types::trading_pair::TradingPair;
    use rust_decimal::Decimal;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn fr_epoch(recv_seq: u64, conn_epoch_us: u64) -> FundingRate {
        FundingRate {
            conn_epoch_us,
            ..fr(recv_seq)
        }
    }

    fn fr(recv_seq: u64) -> FundingRate {
        FundingRate {
            funding_rate_ts_us: 0,
            local_funding_ts_us: 1_700_000_000_000_000,
            recv_seq,
            conn_epoch_us: 1,
            pair: TradingPair::new("BTC", "USDC"),
            funding_rate: d("0.0000125"),
            premium: None,
            interval_hours: 1,
            next_funding_ts_us: 0,
            exchange: "hyperliquid".to_string(),
        }
    }

    fn settlement(funding_time_us: u64) -> FundingSettlement {
        FundingSettlement {
            funding_time_us,
            local_ts_us: funding_time_us + 50_000,
            rtt_us: 1_000,
            pair: TradingPair::new("BTC", "USDC"),
            funding_rate: d("0.0000125"),
            premium: None,
            exchange: "hyperliquid".to_string(),
        }
    }

    #[test]
    fn dedups_carried_forward_samples_by_provenance_key() {
        let mut snap_a = MarketSnapshot::empty(1_000_000);
        snap_a.funding_rate = vec![fr(1), fr(2)];
        let mut snap_b = MarketSnapshot::empty(2_000_000);
        snap_b.funding_rate = vec![fr(2), fr(3)];

        let out = decompose_snapshots(&[snap_a, snap_b]);
        let seqs: Vec<u64> = out.funding_rates.iter().map(|f| f.recv_seq).collect();
        assert_eq!(seqs, vec![1, 2, 3], "wire truth: one row per push");
    }

    #[test]
    fn reconnect_rows_reusing_a_recv_seq_survive_on_their_epoch() {
        let mut before = MarketSnapshot::empty(1_000_000);
        before.funding_rate = vec![fr_epoch(1, 1_788_375_005_000_000)];
        let mut after = MarketSnapshot::empty(2_000_000);
        after.funding_rate = vec![fr_epoch(1, 1_788_375_198_000_000)];

        let out = decompose_snapshots(&[before, after]);
        let epochs: Vec<u64> =
            out.funding_rates.iter().map(|f| f.conn_epoch_us).collect();
        assert_eq!(
            epochs,
            vec![1_788_375_005_000_000, 1_788_375_198_000_000],
            "recv_seq restarts at 1 on reconnect; the epoch keeps both rows"
        );
    }

    #[test]
    fn one_epoch_still_collapses_a_repeated_recv_seq() {
        let mut snap_a = MarketSnapshot::empty(1_000_000);
        snap_a.funding_rate = vec![fr_epoch(2, 1_788_375_005_000_000)];
        let mut snap_b = MarketSnapshot::empty(2_000_000);
        snap_b.funding_rate = vec![fr_epoch(2, 1_788_375_005_000_000)];

        let out = decompose_snapshots(&[snap_a, snap_b]);
        assert_eq!(out.funding_rates.len(), 1);
    }

    #[test]
    fn provenance_free_legacy_rows_are_never_collapsed() {
        let mut snap = MarketSnapshot::empty(1_000_000);
        snap.funding_rate = vec![fr(0), fr(0)];
        let out = decompose_snapshots(&[snap]);
        assert_eq!(out.funding_rates.len(), 2);
    }

    #[test]
    fn dedups_settlements_by_venue_settlement_time() {
        let mut snap_a = MarketSnapshot::empty(1_000_000);
        snap_a.funding_settlements = vec![settlement(3_600_000_000)];
        let mut snap_b = MarketSnapshot::empty(2_000_000);
        snap_b.funding_settlements =
            vec![settlement(3_600_000_000), settlement(7_200_000_000)];

        let out = decompose_snapshots(&[snap_a, snap_b]);
        let times: Vec<u64> = out
            .funding_settlements
            .iter()
            .map(|f| f.funding_time_us)
            .collect();
        assert_eq!(times, vec![3_600_000_000, 7_200_000_000]);
    }
}
