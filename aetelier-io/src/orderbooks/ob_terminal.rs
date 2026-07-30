use aetelier_types::orderbooks::{OrderbookDelta, OrderbookUpdate};
use rust_decimal::Decimal;
use tokio::time::Duration;

/// Running counters over a live orderbook stream.
///
/// Accumulated by [`Stats::record_update`] and rendered by
/// [`print_orderbook_state`]. Counts are monotonic for the lifetime of the
/// stream; a book reset increments `snapshots` rather than resetting them,
/// so the totals stay comparable across reconnects.
///
/// ```
/// use aetelier_io::orderbooks::Stats;
///
/// let stats = Stats::new();
/// assert_eq!(stats.total_updates, 0);
/// assert_eq!(stats.snapshots, 0);
/// ```
pub struct Stats {
    /// Every update seen, snapshots and deltas together.
    pub total_updates: u64,
    /// Updates that reseeded the book from a full snapshot.
    pub snapshots: u64,
    /// Updates that applied incrementally to an existing book.
    pub deltas: u64,
    /// Price levels added across all updates.
    pub inserts: u64,
    /// Price levels removed across all updates.
    pub deletes: u64,
    /// Updates the caller rejected; never incremented by this type.
    pub errors: u64,
}

impl Default for Stats {
    fn default() -> Self {
        Self::new()
    }
}

impl Stats {
    /// A tracker with every counter at zero.
    ///
    /// ```
    /// use aetelier_io::orderbooks::Stats;
    ///
    /// let stats = Stats::new();
    /// assert_eq!(stats.deltas, 0);
    /// assert_eq!(stats.errors, 0);
    /// ```
    pub fn new() -> Self {
        Self {
            total_updates: 0,
            snapshots: 0,
            deltas: 0,
            inserts: 0,
            deletes: 0,
            errors: 0,
        }
    }

    /// Fold one update into the counters.
    ///
    /// Routes the update to `snapshots` or `deltas` by its reset flag, then
    /// adds its inserted and deleted level counts to the running totals.
    ///
    /// ```no_run
    /// use aetelier_io::orderbooks::Stats;
    /// # fn demo(update: &aetelier_types::orderbooks::OrderbookUpdate) {
    /// let mut stats = Stats::new();
    /// stats.record_update(update);
    /// assert_eq!(stats.total_updates, 1);
    /// # }
    /// ```
    pub fn record_update(&mut self, update: &OrderbookUpdate) {
        self.total_updates += 1;
        if update.was_reset {
            self.snapshots += 1;
        } else {
            self.deltas += 1;
        }
        self.inserts += update.levels_inserted as u64;
        self.deletes += update.levels_deleted as u64;
    }
}

/// Print the book's top five levels and stream statistics to stdout.
///
/// Renders, in order: pair and sequencing identifiers, runtime and update
/// counts from `stats`, a five-deep bid/ask ladder, and the derived metrics
/// (mid, spread, spread in basis points, volume imbalance). Prices round to
/// two decimal places and sizes to four for display only — the book itself
/// keeps exact [`rust_decimal::Decimal`] values.
///
/// Metrics that are undefined on a one-sided book print as zero.
///
/// ```no_run
/// use aetelier_io::orderbooks::{print_orderbook_state, Stats};
/// use tokio::time::Duration;
/// # fn demo(book: &aetelier_types::orderbooks::OrderbookDelta) {
/// let stats = Stats::new();
/// print_orderbook_state(book, &stats, Duration::from_secs(30));
/// # }
/// ```
pub fn print_orderbook_state(ob: &OrderbookDelta, stats: &Stats, elapsed: Duration) {
    println!("\n");

    println!(
        "  {} | update_id: {} | seq: {} | deltas since snapshot: {}",
        ob.pair().to_canonical(),
        ob.last_update_id(),
        ob.sequence(),
        ob.delta_count()
    );
    println!(
        "  Runtime: {:.1}s | Updates: {} ({} snapshots, {} deltas)",
        elapsed.as_secs_f64(),
        stats.total_updates,
        stats.snapshots,
        stats.deltas
    );
    println!(
        "  Inserts: {} | Deletes: {} | Errors: {}",
        stats.inserts, stats.deletes, stats.errors
    );

    // Print top 5 levels
    println!(
        "\n  {:>12} {:>12}  │  {:>12} {:>12}",
        "BID SIZE", "BID PRICE", "ASK PRICE", "ASK SIZE"
    );

    println!("\n");

    let top_bids = ob.top_bids(5);
    let top_asks = ob.top_asks(5);

    for i in 0..5 {
        let bid_str = top_bids
            .get(i)
            .map(|(p, s)| format!("{:>12} {:>12}", s.round_dp(4), p.round_dp(2)))
            .unwrap_or_else(|| " ".repeat(25));
        let ask_str = top_asks
            .get(i)
            .map(|(p, s)| format!("{:>12} {:>12}", p.round_dp(2), s.round_dp(4)))
            .unwrap_or_else(|| " ".repeat(25));
        println!("  {}  │  {}", bid_str, ask_str);
    }

    if let Some(mid) = ob.mid_price() {
        println!(
            "\n  Mid: {} | Spread: {} ({:.2} bps) | Imbalance: {:+.4}",
            mid.round_dp(2),
            ob.spread().unwrap_or(Decimal::ZERO).round_dp(4),
            ob.spread_bps().unwrap_or(Decimal::ZERO),
            ob.volume_imbalance().unwrap_or(Decimal::ZERO)
        );
    }
    println!(
        "  Bid levels: {} | Ask levels: {} | Total bid vol: {} | Total ask vol: {}",
        ob.bid_depth(),
        ob.ask_depth(),
        ob.total_bid_volume().round_dp(2),
        ob.total_ask_volume().round_dp(2)
    );
}
