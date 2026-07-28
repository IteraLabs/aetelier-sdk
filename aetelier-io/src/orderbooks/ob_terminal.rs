use aetelier_types::orderbooks::{OrderbookDelta, OrderbookUpdate};
use rust_decimal::Decimal;
use tokio::time::Duration;

/// Simple stats tracker
pub struct Stats {
    pub total_updates: u64,
    pub snapshots: u64,
    pub deltas: u64,
    pub inserts: u64,
    pub deletes: u64,
    pub errors: u64,
}

impl Default for Stats {
    fn default() -> Self {
        Self::new()
    }
}

impl Stats {
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
