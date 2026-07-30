//! Orderbook loading targets and update result types.
//!
//! Defines [`OrderbookTarget`] to specify which orderbook representation
//! to load, and result types for update operations.

use crate::orderbooks::{Orderbook, OrderbookDelta};
use crate::trading_pair::TradingPair;

/// Type of orderbook update (snapshot or incremental delta).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderbookUpdateType {
    /// Full orderbook snapshot (reset).
    Snapshot,
    /// Incremental update to existing orderbook.
    Delta,
}

/// Result of applying an update to the orderbook.
#[derive(Debug, Clone)]
pub struct OrderbookUpdate {
    /// Type of update applied (snapshot or delta).
    pub update_type: OrderbookUpdateType,
    /// Count of bid levels that were modified.
    pub bids_modified: usize,
    /// Count of ask levels that were modified.
    pub asks_modified: usize,
    /// Count of levels deleted (size=0).
    pub levels_deleted: usize,
    /// Count of new levels inserted.
    pub levels_inserted: usize,
    /// Whether this was a full snapshot reset.
    pub was_reset: bool,
}

/// Specifies which orderbook representation to load into.
///
/// Use this to tell the reader which data structure you need:
/// - [`OrderbookTarget::Delta`] for live delta processing with `BTreeMap`
/// - [`OrderbookTarget::Snapshot`] for analysis/simulation with `Vec<Level>`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderbookTarget {
    /// Load into [`OrderbookDelta`] (BTreeMap-based, for delta processing)
    Delta,
    /// Load into [`Orderbook`] (`Vec<Level>`-based, for simulation/analysis)
    Snapshot,
}

/// Result of reading an orderbook file in either delta or snapshot format.
///
/// Pattern match on this to extract the representation you need.
#[derive(Debug)]
pub enum OrderbookTargetData {
    /// Orderbook in delta format with `BTreeMap<Decimal, Decimal>` for incremental processing.
    Delta(OrderbookDelta),
    /// Orderbook snapshots with `Vec<Level>` per side for simulation and analysis.
    Snapshot(Vec<Orderbook>),
}

impl OrderbookTargetData {
    /// Returns `true` if this is a `Delta` variant.
    #[inline]
    pub fn is_delta(&self) -> bool {
        matches!(self, Self::Delta(_))
    }

    /// Returns `true` if this is a `Snapshot` variant.
    #[inline]
    pub fn is_snapshot(&self) -> bool {
        matches!(self, Self::Snapshot(_))
    }

    /// Attempt to extract the `OrderbookDelta`, consuming self.
    ///
    /// Returns `None` if this is a `Snapshot` variant.
    pub fn into_delta(self) -> Option<OrderbookDelta> {
        match self {
            Self::Delta(m) => Some(m),
            Self::Snapshot(_) => None,
        }
    }

    /// Attempt to extract the `Vec<Orderbook>`, consuming self.
    ///
    /// Returns `None` if this is a `Delta` variant.
    pub fn into_snapshot(self) -> Option<Vec<Orderbook>> {
        match self {
            Self::Delta(_) => None,
            Self::Snapshot(obs) => Some(obs),
        }
    }

    /// Get the trading pair regardless of variant.
    ///
    /// For `Snapshot`, returns the pair of the first orderbook.
    /// Returns `None` if the `Snapshot` vector is empty.
    pub fn pair(&self) -> Option<&TradingPair> {
        match self {
            Self::Delta(m) => Some(m.pair()),
            Self::Snapshot(obs) => obs.first().map(|ob| &ob.pair),
        }
    }

    /// Number of orderbook snapshots.
    ///
    /// Returns 1 for `Delta` (single aggregated state), or the
    /// actual count for `Snapshot`.
    pub fn len(&self) -> usize {
        match self {
            Self::Delta(_) => 1,
            Self::Snapshot(obs) => obs.len(),
        }
    }

    /// Returns `true` if there are no snapshots (only possible for empty `Snapshot`).
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Delta(_) => false,
            Self::Snapshot(obs) => obs.is_empty(),
        }
    }
}
