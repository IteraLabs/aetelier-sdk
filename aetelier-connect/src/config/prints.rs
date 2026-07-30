use std::path::Path;

// ─────────────────────────────────────────────────────────────────────────────
// Snapshot summary printer
// ─────────────────────────────────────────────────────────────────────────────

/// Print a one-line summary of a `MarketSnapshot`.
pub fn _print_snapshot_line(
    idx: usize,
    ts_us: u64,
    ob_info: Option<(f64, f64, usize, usize)>, // (best_bid, best_ask, bid_depth, ask_depth)
    n_trades: usize,
    n_liqs: usize,
    n_fr: usize,
    n_oi: usize,
) {
    let ob_desc = ob_info
        .map(|(bid, ask, bd, ad)| {
            format!("mid={:.2} bids={} asks={}", (bid + ask) / 2.0, bd, ad)
        })
        .unwrap_or_else(|| "OB=∅".to_string());

    println!(
        "  [{:>3}] ts_us={:<20} | {} | tr={} lq={} fr={} oi={}",
        idx, ts_us, ob_desc, n_trades, n_liqs, n_fr, n_oi,
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// FlushResult printer (moved to aetelier-io)
// ─────────────────────────────────────────────────────────────────────────────

// The print_flush_result function has been moved to aetelier-io as an extension
// for printing `aetelier_types::snapshots::FlushResult` produced by I/O operations.

// ─────────────────────────────────────────────────────────────────────────────
// Aggregate summary printer
// ─────────────────────────────────────────────────────────────────────────────

/// Print a compact table header for MarketAggregate rows.
pub fn print_aggregate_header() {
    println!(
        "  {:>5} {:>14} {:>10} {:>10} {:>8} {:>10} {:>6} {:>12} {:>4}",
        "idx",
        "ts_us",
        "mid_price",
        "spread",
        "imb",
        "trade_vol",
        "tr_n",
        "liq_not",
        "lq_n",
    );
    println!("  {}", "─".repeat(90));
}

/// Print a single MarketAggregate row.
pub fn print_aggregate_row(idx: usize, agg: &aetelier_types::MarketAggregate) {
    println!(
        "  {:>5} {:>14} {:>10.2} {:>10.4} {:>8.4} {:>10.6} {:>6} {:>12.2} {:>4}",
        idx,
        agg.ts_us,
        agg.ob_mid_price,
        agg.ob_spread,
        agg.ob_imbalance,
        agg.trade_volume,
        agg.trade_count,
        agg.liq_notional,
        agg.liq_count,
    );
}

/// Print a load-mode banner.
pub fn print_load_banner(title: &str, path: &Path) {
    println!();
    println!(" ══════════════════════════════════════════════════════════");
    println!("  {}", title);
    println!(" ══════════════════════════════════════════════════════════");
    println!("  File: {}", path.display());
    println!(" ══════════════════════════════════════════════════════════");
    println!();
}
