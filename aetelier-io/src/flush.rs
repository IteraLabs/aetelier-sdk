//! Extension traits for flushing synchronized market data to Parquet.
//!
//! This module is only available when the `connect` and `parquet` features
//! are both enabled, allowing `aetelier-io` to extend types from
//! `aetelier-connect` without creating a hard dependency in the reverse
//! direction.

#[cfg(all(feature = "connect", feature = "parquet"))]
use aetelier_connect::synchronizers::MarketSynchronizer;
#[cfg(all(feature = "connect", feature = "parquet"))]
use aetelier_connect::synchronizers::ob_sync::ObSynchronizer;
#[cfg(all(feature = "connect", feature = "parquet"))]
use aetelier_types::{errors::PersistError, snapshots::FlushResult};

/// Extension trait that adds Parquet persistence to types from `aetelier-connect`.
#[cfg(all(feature = "connect", feature = "parquet"))]
pub trait FlushToParquet {
    /// Drain buffered snapshots and write each data source to a
    /// timestamped Parquet file under `output_dir`.
    fn flush_to_parquet(
        &mut self,
        output_dir: &std::path::Path,
    ) -> Result<FlushResult, PersistError>;
}

#[cfg(all(feature = "connect", feature = "parquet"))]
impl FlushToParquet for MarketSynchronizer {
    fn flush_to_parquet(
        &mut self,
        output_dir: &std::path::Path,
    ) -> Result<FlushResult, PersistError> {
        // Retain-on-failure: build the write from a BORROW of the buffer and
        // clear it only after every source has written. A write error leaves
        // the buffer intact so the next flush retries — matching BufferedSink,
        // never the old drain-before-write loss. Note: a partial failure
        // (one source written, a later one errors) re-writes the succeeded
        // sources on the retry, the same duplicate-on-partial property
        // BufferedSink already has; correctness favors no-loss over no-dup.
        if self.buffer.is_empty() {
            return Ok(FlushResult::default());
        }
        let snapshots = &self.buffer;

        let mut orderbooks = Vec::new();
        let mut trades = Vec::new();
        let mut liquidations = Vec::new();
        let mut funding_rates = Vec::new();
        let mut open_interests = Vec::new();

        for snap in snapshots {
            if let Some(ob) = &snap.orderbook {
                orderbooks.push(ob.clone());
            }
            trades.extend(snap.trades.iter().cloned());
            liquidations.extend(snap.liquidations.iter().cloned());
            funding_rates.extend(snap.funding_rate.iter().cloned());
            open_interests.extend(snap.open_interest.iter().cloned());
        }

        let mut result = FlushResult {
            snapshot_count: snapshots.len(),
            ..Default::default()
        };

        // Write orderbooks
        if !orderbooks.is_empty() {
            let ob_dir = output_dir.join("orderbooks");
            std::fs::create_dir_all(&ob_dir)?;
            let path = crate::orderbooks::write_ob_parquet(&orderbooks, &ob_dir, "sync")
                .map_err(|e| PersistError::Parse(e.to_string()))?;
            result.orderbook_path = Some(path);
        }

        // Write trades
        if !trades.is_empty() {
            let trades_dir = output_dir.join("trades");
            std::fs::create_dir_all(&trades_dir)?;
            let path = crate::trades::write_trades_parquet_timestamped(
                &trades,
                &trades_dir,
                "sync",
            )?;
            result.trades_path = Some(path);
        }

        // Write liquidations
        if !liquidations.is_empty() {
            let liq_dir = output_dir.join("liquidations");
            std::fs::create_dir_all(&liq_dir)?;
            let path = crate::liquidations::write_liquidations_parquet_timestamped(
                &liquidations,
                &liq_dir,
                "sync",
            )?;
            result.liquidations_path = Some(path);
        }

        // Write funding rates
        if !funding_rates.is_empty() {
            let fr_dir = output_dir.join("fundings");
            std::fs::create_dir_all(&fr_dir)?;
            let path = crate::funding::write_funding_parquet_timestamped(
                &funding_rates,
                &fr_dir,
                "sync",
            )?;
            result.funding_path = Some(path);
        }

        // Write open interest
        if !open_interests.is_empty() {
            let oi_dir = output_dir.join("open_interests");
            std::fs::create_dir_all(&oi_dir)?;
            let path = crate::open_interest::write_oi_parquet_timestamped(
                &open_interests,
                &oi_dir,
                "sync",
            )?;
            result.open_interest_path = Some(path);
        }

        // Every source wrote — safe to release the buffer now.
        self.buffer.clear();
        Ok(result)
    }
}

// ── ObSynchronizer → Parquet ────────────────────────────────────────────

/// Extension trait that adds Parquet persistence to [`ObSynchronizer`].
///
/// Requires `connect` + `parquet` features.
#[cfg(all(feature = "connect", feature = "parquet"))]
pub trait FlushObSyncToParquet {
    /// Write all buffered orderbook snapshots to a timestamped Parquet file
    /// under `output_dir`, returning its path. `Ok(None)` when the buffer is
    /// empty (no file written). The buffer is released only after a
    /// successful write — a write error retains it for the next flush.
    fn flush_to_parquet(
        &mut self,
        output_dir: &std::path::Path,
    ) -> Result<Option<std::path::PathBuf>, PersistError>;
}

#[cfg(all(feature = "connect", feature = "parquet"))]
impl FlushObSyncToParquet for ObSynchronizer {
    fn flush_to_parquet(
        &mut self,
        output_dir: &std::path::Path,
    ) -> Result<Option<std::path::PathBuf>, PersistError> {
        // Empty guard: write_ob_parquet indexes snapshots[0], so an empty
        // buffer would panic — return None instead.
        if self.buffer.is_empty() {
            return Ok(None);
        }
        // Borrow, write, then release only on success (retain-on-failure).
        let path = crate::orderbooks::write_ob_parquet(&self.buffer, output_dir, "sync")?;
        self.buffer.clear();
        Ok(Some(path))
    }
}

// ── MarketSynchronizer → Aggregate Parquet ──────────────────────────────

/// Extension trait that writes aggregated market statistics to Parquet.
///
/// Requires `connect` + `parquet` features.
#[cfg(all(feature = "connect", feature = "parquet"))]
pub trait FlushAggregateToParquet {
    /// Compute aggregated statistics and write to a Parquet file under
    /// `output_dir`.
    fn flush_aggregate_to_parquet(
        &mut self,
        output_dir: &std::path::Path,
    ) -> Result<std::path::PathBuf, PersistError>;
}

#[cfg(all(feature = "connect", feature = "parquet"))]
impl FlushAggregateToParquet for MarketSynchronizer {
    fn flush_aggregate_to_parquet(
        &mut self,
        output_dir: &std::path::Path,
    ) -> Result<std::path::PathBuf, PersistError> {
        let aggregates = self.market_aggregate();
        crate::snapshots::write_market_aggregate_parquet(&aggregates, output_dir)
    }
}

#[cfg(all(test, feature = "connect", feature = "parquet"))]
mod retain_tests {
    use super::*;
    use aetelier_connect::synchronizers::{MarketSynchronizer, ObSynchronizer};
    use aetelier_types::orderbooks::Orderbook;
    use aetelier_types::snapshots::MarketSnapshot;
    use aetelier_types::trades::Trade;
    use aetelier_types::trading_pair::TradingPair;

    fn pair() -> TradingPair {
        TradingPair::new("BTC", "USDT")
    }

    fn ob(ts: u64) -> Orderbook {
        Orderbook::empty(pair(), "binance")
            .tap(|o: &mut Orderbook| o.orderbook_ts_us = ts)
    }

    // A path whose parent is a regular file — create_dir_all / File::create
    // under it fails, forcing every write_* to error.
    fn unwritable_dir() -> (tempfile::TempDir, std::path::PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let blocker = tmp.path().join("blocker");
        std::fs::write(&blocker, b"x").unwrap();
        let bad = blocker.join("out");
        (tmp, bad)
    }

    #[test]
    fn market_flush_retains_buffer_on_write_error() {
        let mut sync = MarketSynchronizer::new(1_000_000);
        let mut snap = MarketSnapshot::empty(2_000_000);
        snap.orderbook = Some(ob(1_500_000));
        snap.trades = vec![Trade {
            source_trade_ts_us: 1_500_000,
            local_trade_ts_us: 0,
            source_trade_rtt_us: 0,
            pair: pair(),
            side: aetelier_types::TradeSide::Buy,
            amount: rust_decimal::Decimal::from(1),
            price: rust_decimal::Decimal::from(100),
            exchange: "binance".into(),
            id: "1".into(),
            origin: Default::default(),
        }];
        sync.buffer.push(snap);

        let (_tmp, bad) = unwritable_dir();
        assert!(sync.flush_to_parquet(&bad).is_err(), "write must fail");
        assert_eq!(sync.buffer.len(), 1, "buffer retained for retry, not lost");
    }

    #[test]
    fn ob_flush_retains_buffer_on_write_error() {
        let mut sync = ObSynchronizer::new(1_000_000);
        sync.buffer.push(ob(1_500_000));

        let (_tmp, bad) = unwritable_dir();
        // The inherent ObSynchronizer::flush_to_parquet(dir, mode) stub
        // shadows the extension trait, so reach the real method via UFCS.
        assert!(FlushObSyncToParquet::flush_to_parquet(&mut sync, &bad).is_err());
        assert_eq!(sync.buffer.len(), 1, "buffer retained on failure");
    }

    #[test]
    fn ob_flush_empty_buffer_returns_none_not_panic() {
        let mut sync = ObSynchronizer::new(1_000_000);
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(
            FlushObSyncToParquet::flush_to_parquet(&mut sync, tmp.path()).unwrap(),
            None
        );
    }

    #[test]
    fn market_flush_clears_buffer_on_success() {
        let mut sync = MarketSynchronizer::new(1_000_000);
        let mut snap = MarketSnapshot::empty(2_000_000);
        snap.orderbook = Some(ob(1_500_000));
        sync.buffer.push(snap);

        let tmp = tempfile::tempdir().unwrap();
        let r = sync.flush_to_parquet(tmp.path()).unwrap();
        assert_eq!(r.snapshot_count, 1);
        assert!(
            sync.buffer.is_empty(),
            "buffer released after a clean write"
        );
    }
}

#[cfg(test)]
trait Tap: Sized {
    fn tap(mut self, f: impl FnOnce(&mut Self)) -> Self {
        f(&mut self);
        self
    }
}
#[cfg(test)]
impl<T> Tap for T {}
