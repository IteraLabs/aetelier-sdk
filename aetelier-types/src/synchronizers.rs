//! Synchronization and clock modes.
//!
//! Types for configuring multi-source data synchronization and grid alignment.

use std::fmt;

use serde::{Deserialize, Serialize};

#[cfg(feature = "wasm")]
use tsify_next::Tsify;
#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;

/// How a worker processes incoming exchange events.
///
/// This is the user-visible "badge" shown in the dashboard sidebar:
///
/// - **Raw** — events are forwarded as-is with no synchronisation.
/// - **Clock** — events are aligned to a time grid and emitted as
///   [`MarketSnapshot`](crate::snapshots::MarketSnapshot)s at each period.
///
/// `WorkerMode::Clock` carries the underlying [`ClockMode`] and the grid
/// period in microseconds so the dashboard can render labels like
/// `ON_CLOCK(100MS)` or `ON_ORDERBOOK`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(Tsify), tsify(into_wasm_abi, from_wasm_abi))]
pub enum WorkerMode {
    /// No synchronisation — raw decoded events pass through directly.
    Raw,
    /// Grid-aligned synchronisation driven by the specified clock source.
    Clock {
        /// Which data feed drives the grid.
        clock: ClockMode,
        /// Grid period in microseconds (e.g. 100_000 = 100 ms).
        period_us: u64,
    },
}

impl fmt::Display for WorkerMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkerMode::Raw => write!(f, "RAW"),
            WorkerMode::Clock { clock, period_us } => {
                let label = match clock {
                    ClockMode::OrderbookDriven => "ON_ORDERBOOK",
                    ClockMode::TradeDriven => "ON_TRADE",
                    ClockMode::LiquidationDriven => "ON_LIQUIDATION",
                    ClockMode::ExternalClock => "ON_CLOCK",
                };
                // Convert period_us (microseconds) to a human-readable string.
                let period_str = match *period_us {
                    us if us >= 1_000_000 && us % 1_000_000 == 0 => {
                        format!("{}S", us / 1_000_000)
                    }
                    us if us >= 1_000 && us % 1_000 == 0 => {
                        format!("{}MS", us / 1_000)
                    }
                    us => format!("{}US", us),
                };
                // ON_ORDERBOOK doesn't typically need a period suffix,
                // but we include it for consistency when a period is set.
                match clock {
                    ClockMode::OrderbookDriven
                    | ClockMode::TradeDriven
                    | ClockMode::LiquidationDriven => write!(f, "{}", label),
                    ClockMode::ExternalClock => write!(f, "{}({})", label, period_str),
                }
            }
        }
    }
}

/// Determines which data feed drives the grid clock.
///
/// The clock mode controls which event triggers snapshot emission
/// at grid period boundaries. Non-driver feeds accumulate data passively
/// and their contents are included in whatever snapshot period they fall into.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(Tsify), tsify(into_wasm_abi, from_wasm_abi))]
pub enum ClockMode {
    /// Orderbook updates drive the grid clock.
    ///
    /// This is the default mode and preserves backward compatibility.
    /// Period boundaries are detected per-symbol using the orderbook's
    /// exchange-reported millisecond timestamp.
    OrderbookDriven,

    /// Trade events drive the grid clock.
    ///
    /// Each trade event checks if the trade's timestamp crosses a grid period
    /// boundary. Attribution is by timestamp membership: every event lands in
    /// the period its own timestamp falls in, so a boundary-crossing trade
    /// belongs to the newly opened period, never the completed one.
    TradeDriven,

    /// Liquidation events drive the grid clock.
    ///
    /// Each liquidation event checks if the liquidation's timestamp crosses
    /// a grid period boundary. Attribution is by timestamp membership (see
    /// [`ClockMode::TradeDriven`]): the crossing event belongs to the newly
    /// opened period, never the completed one.
    LiquidationDriven,

    /// External timestamps drive the grid.
    ///
    /// All data feeds are passive; the caller explicitly advances the grid by
    /// providing a timestamp. This decouples snapshot emission from any
    /// particular data feed's arrival rate.
    ExternalClock,
}
