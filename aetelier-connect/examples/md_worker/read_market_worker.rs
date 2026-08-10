//! Parquet dataset reader & statistics aggregator for MarketWorker output.
//!
//! Scans a dataset root directory (e.g. `datasets/collected/bybit`) for all
//! Parquet files across the 5 data-type subdirectories, reads every file
//! using the existing deserialisers, groups records by symbol (parsed from
//! the filename), and prints per-symbol aggregate statistics.
//!
//! # Filename convention
//!
//! Writers produce files named `{SYMBOL}_{DATATYPE}_{MODE}_{TS}.parquet`,
//! e.g. `BTCUSDT_ob_sync_20260226_153000.123.parquet`.
//!
//! The reader extracts the symbol from the segment before the data-type tag
//! (`_ob_`, `_trades_`, `_liquidations_`, `_funding_`, `_oi_`).
//!
//! # Usage
//!
//! ```bash
//! cargo run -p aetelier-connect --example read_market_worker --features parquet \
//!   -- --dir datasets/collected/bybit
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use clap::Parser;
use rust_decimal::prelude::ToPrimitive;

use aetelier_io::{
    funding::funding_parquet::read_funding_parquet,
    liquidations::liq_parquet::read_liquidations_parquet,
    open_interest::oi_parquet::read_oi_parquet, orderbooks::ob_parquet::read_ob_parquet,
    trades::trades_parquet::read_trades_parquet,
};
use aetelier_types::{
    funding::FundingRate,
    liquidations::Liquidation,
    open_interest::OpenInterest,
    orderbooks::{Orderbook, OrderbookTarget, OrderbookTargetData},
    trades::Trade,
    utils,
};

// ─────────────────────────────────────────────────────────────────────────────
// CLI
// ─────────────────────────────────────────────────────────────────────────────

/// Read Parquet datasets produced by MarketWorker and print per-symbol stats.
#[derive(Parser, Debug)]
#[command(name = "read_market_worker", version, about)]
struct Cli {
    /// Root directory containing data-type subdirectories
    /// (e.g. `datasets/collected/bybit`).
    #[arg(short, long)]
    dir: PathBuf,
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Collect all `.parquet` files in `dir` that contain `datatype_tag` in their
/// filename, grouped by the symbol prefix (everything before the tag).
///
/// Returns a `BTreeMap<symbol, Vec<PathBuf>>` sorted by symbol.
///
/// E.g. for tag `"_ob_"`, the file `BTCUSDT_ob_sync_20260226_153000.parquet`
/// yields symbol `"BTCUSDT"`.
fn collect_and_group(dir: &Path, datatype_tag: &str) -> BTreeMap<String, Vec<PathBuf>> {
    let mut groups: BTreeMap<String, Vec<PathBuf>> = BTreeMap::new();

    let Ok(entries) = std::fs::read_dir(dir) else {
        return groups;
    };

    let mut paths: Vec<PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension().is_some_and(|ext| ext == "parquet")
                && p.file_name()
                    .and_then(|f| f.to_str())
                    .is_some_and(|f| f.contains(datatype_tag))
        })
        .collect();

    paths.sort();

    for path in paths {
        let symbol = path
            .file_name()
            .and_then(|f| f.to_str())
            .and_then(|f| f.find(datatype_tag).map(|idx| f[..idx].to_string()))
            .unwrap_or_else(|| "unknown".to_string());

        groups.entry(symbol).or_default().push(path);
    }

    groups
}

/// Total file count across all symbols in a grouped map.
fn total_files(groups: &BTreeMap<String, Vec<PathBuf>>) -> usize {
    groups.values().map(|v| v.len()).sum()
}

/// Format a microsecond timestamp as a human-readable UTC string.
fn format_ts_us(ts_us: u64) -> String {
    utils::format_ts(ts_us)
}

/// Compute min, max, mean for a slice of f64 values.
fn stats_f64(values: &[f64]) -> (f64, f64, f64) {
    if values.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    let mut min = f64::MAX;
    let mut max = f64::MIN;
    let mut sum = 0.0_f64;
    for &v in values {
        if v < min {
            min = v;
        }
        if v > max {
            max = v;
        }
        sum += v;
    }
    (min, max, sum / values.len() as f64)
}

// ─────────────────────────────────────────────────────────────────────────────
// Section printers — per symbol
// ─────────────────────────────────────────────────────────────────────────────

fn print_orderbook_stats(root: &Path) {
    let dir = root.join("orderbooks");
    let groups = collect_and_group(&dir, "_ob_");

    println!(" ── [1/5] Orderbooks ──");

    if groups.is_empty() {
        println!("  No Parquet files found in {}\n", dir.display());
        return;
    }

    println!(
        "  Files: {} ({} symbols)\n",
        total_files(&groups),
        groups.len()
    );

    for (symbol, files) in &groups {
        println!("  ── {} ({} files) ──", symbol, files.len());

        let mut all_obs: Vec<Orderbook> = Vec::new();
        let mut read_errors = 0_u32;

        for path in files {
            match read_ob_parquet(path, OrderbookTarget::Snapshot) {
                Ok(OrderbookTargetData::Snapshot(obs)) => all_obs.extend(obs),
                Ok(_) => {}
                Err(e) => {
                    eprintln!("    Warning: {}: {:?}", path.display(), e);
                    read_errors += 1;
                }
            }
        }

        if read_errors > 0 {
            println!("    Read errors: {}", read_errors);
        }

        let n = all_obs.len();
        println!("    Snapshots: {}", n);

        if n == 0 {
            println!();
            continue;
        }

        all_obs.sort_by_key(|ob| ob.orderbook_ts_us);

        let first_ts = all_obs[0].orderbook_ts_us;
        let last_ts = all_obs[n - 1].orderbook_ts_us;
        let span_s = (last_ts - first_ts) as f64 / 1e6;

        println!("    First: {} us ({})", first_ts, format_ts_us(first_ts));
        println!("    Last:  {} us ({})", last_ts, format_ts_us(last_ts));
        println!("    Span:  {:.3}s", span_s);

        // Depth
        let bid_depths: Vec<f64> =
            all_obs.iter().map(|ob| ob.bids.len() as f64).collect();
        let ask_depths: Vec<f64> =
            all_obs.iter().map(|ob| ob.asks.len() as f64).collect();
        let (bid_min, bid_max, bid_mean) = stats_f64(&bid_depths);
        let (ask_min, ask_max, ask_mean) = stats_f64(&ask_depths);
        println!(
            "    Bid depth: min={:.0}  max={:.0}  avg={:.1}",
            bid_min, bid_max, bid_mean
        );
        println!(
            "    Ask depth: min={:.0}  max={:.0}  avg={:.1}",
            ask_min, ask_max, ask_mean
        );

        // Mid price & spread
        let mut mid_prices: Vec<f64> = Vec::with_capacity(n);
        let mut spreads: Vec<f64> = Vec::with_capacity(n);
        for ob in &all_obs {
            if let (Some(bid), Some(ask)) = (ob.best_bid(), ob.best_ask()) {
                let (bid, ask) =
                    (bid.to_f64().unwrap_or(0.0), ask.to_f64().unwrap_or(0.0));
                mid_prices.push((bid + ask) / 2.0);
                spreads.push(ask - bid);
            }
        }
        if !mid_prices.is_empty() {
            let (mid_min, mid_max, mid_mean) = stats_f64(&mid_prices);
            let (sp_min, sp_max, sp_mean) = stats_f64(&spreads);
            println!(
                "    Mid price: min={:.2}  max={:.2}  avg={:.2}",
                mid_min, mid_max, mid_mean
            );
            println!(
                "    Spread:    min={:.4}  max={:.4}  avg={:.4}",
                sp_min, sp_max, sp_mean
            );
        }

        // Grid spacing
        if n >= 2 {
            let diffs: Vec<f64> = all_obs
                .windows(2)
                .map(|w| (w[1].orderbook_ts_us - w[0].orderbook_ts_us) as f64)
                .collect();
            let (d_min, d_max, d_mean) = stats_f64(&diffs);
            println!(
                "    Grid: min={:.1}ms  max={:.1}ms  avg={:.1}ms",
                d_min / 1e3,
                d_max / 1e3,
                d_mean / 1e3,
            );
        }

        // Sample first 3
        let preview = 3.min(n);
        for (i, ob) in all_obs.iter().take(preview).enumerate() {
            let bid = ob.best_bid().and_then(|d| d.to_f64()).unwrap_or(0.0);
            let ask = ob.best_ask().and_then(|d| d.to_f64()).unwrap_or(0.0);
            println!(
                "      [{:>3}] mid={:.2}  spread={:.4}  bids={} asks={}",
                i,
                (bid + ask) / 2.0,
                ask - bid,
                ob.bids.len(),
                ob.asks.len(),
            );
        }
        if n > preview {
            println!("      ... ({} more)", n - preview);
        }
        println!();
    }
}

fn print_trade_stats(root: &Path) {
    let dir = root.join("trades");
    let groups = collect_and_group(&dir, "_trades_");

    println!(" ── [2/5] Trades ──");

    if groups.is_empty() {
        println!("  No Parquet files found in {}\n", dir.display());
        return;
    }

    println!(
        "  Files: {} ({} symbols)\n",
        total_files(&groups),
        groups.len()
    );

    for (symbol, files) in &groups {
        println!("  ── {} ({} files) ──", symbol, files.len());

        let mut all_trades: Vec<Trade> = Vec::new();
        let mut read_errors = 0_u32;

        for path in files {
            match read_trades_parquet(path) {
                Ok(trades) => all_trades.extend(trades),
                Err(e) => {
                    eprintln!("    Warning: {}: {:?}", path.display(), e);
                    read_errors += 1;
                }
            }
        }

        if read_errors > 0 {
            println!("    Read errors: {}", read_errors);
        }

        let n = all_trades.len();
        println!("    Trades: {}", n);

        if n == 0 {
            println!();
            continue;
        }

        all_trades.sort_by_key(|t| t.source_trade_ts_us);

        let first_ts = all_trades[0].source_trade_ts_us;
        let last_ts = all_trades[n - 1].source_trade_ts_us;
        let span_s = (last_ts - first_ts) as f64 / 1e6;

        println!("    First: {} us ({})", first_ts, format_ts_us(first_ts));
        println!("    Last:  {} us ({})", last_ts, format_ts_us(last_ts));
        println!("    Span:  {:.3}s", span_s);

        if span_s > 0.0 {
            println!("    Rate:  {:.1} trades/s", n as f64 / span_s);
        }

        let mut buy_vol = 0.0_f64;
        let mut sell_vol = 0.0_f64;
        let mut buy_count = 0_u64;
        let mut sell_count = 0_u64;
        let mut sum_pv = 0.0_f64;
        let mut sum_v = 0.0_f64;

        for t in &all_trades {
            let price = t.price.to_f64().unwrap_or(0.0);
            let amount = t.amount.to_f64().unwrap_or(0.0);
            sum_pv += price * amount;
            sum_v += amount;
            if t.side == aetelier_types::trades::TradeSide::Buy {
                buy_vol += amount;
                buy_count += 1;
            } else {
                sell_vol += amount;
                sell_count += 1;
            }
        }

        let vwap = if sum_v > 0.0 { sum_pv / sum_v } else { 0.0 };

        println!("    Buy:      {} trades, {:.6} qty", buy_count, buy_vol);
        println!("    Sell:     {} trades, {:.6} qty", sell_count, sell_vol);
        println!("    Net flow: {:.6} qty", buy_vol - sell_vol);
        println!("    VWAP:     {:.2}", vwap);
        println!("    Notional: {:.2}", sum_pv);

        let prices: Vec<f64> = all_trades
            .iter()
            .map(|t| t.price.to_f64().unwrap_or(0.0))
            .collect();
        let (p_min, p_max, p_mean) = stats_f64(&prices);
        println!(
            "    Price: min={:.2}  max={:.2}  avg={:.2}",
            p_min, p_max, p_mean
        );
        println!();
    }
}

fn print_liquidation_stats(root: &Path) {
    let dir = root.join("liquidations");
    let groups = collect_and_group(&dir, "_liquidations_");

    println!(" ── [3/5] Liquidations ──");

    if groups.is_empty() {
        println!("  No Parquet files found in {}\n", dir.display());
        return;
    }

    println!(
        "  Files: {} ({} symbols)\n",
        total_files(&groups),
        groups.len()
    );

    for (symbol, files) in &groups {
        println!("  ── {} ({} files) ──", symbol, files.len());

        let mut all_liqs: Vec<Liquidation> = Vec::new();
        let mut read_errors = 0_u32;

        for path in files {
            match read_liquidations_parquet(path) {
                Ok(liqs) => all_liqs.extend(liqs),
                Err(e) => {
                    eprintln!("    Warning: {}: {:?}", path.display(), e);
                    read_errors += 1;
                }
            }
        }

        if read_errors > 0 {
            println!("    Read errors: {}", read_errors);
        }

        let n = all_liqs.len();
        println!("    Records: {}", n);

        if n == 0 {
            println!();
            continue;
        }

        all_liqs.sort_by_key(|l| l.liquidation_ts_us);

        let first_ts = all_liqs[0].liquidation_ts_us;
        let last_ts = all_liqs[n - 1].liquidation_ts_us;
        let span_s = (last_ts - first_ts) as f64 / 1e6;

        println!("    First: {} us ({})", first_ts, format_ts_us(first_ts));
        println!("    Last:  {} us ({})", last_ts, format_ts_us(last_ts));
        println!("    Span:  {:.3}s", span_s);

        let mut buy_notional = 0.0_f64;
        let mut sell_notional = 0.0_f64;
        let mut buy_count = 0_u64;
        let mut sell_count = 0_u64;

        for liq in &all_liqs {
            let notional = (liq.price * liq.amount).to_f64().unwrap_or(0.0);
            if liq.side == aetelier_types::trades::TradeSide::Buy {
                buy_notional += notional;
                buy_count += 1;
            } else {
                sell_notional += notional;
                sell_count += 1;
            }
        }

        println!("    Buy:   {} (notional: {:.2})", buy_count, buy_notional);
        println!("    Sell:  {} (notional: {:.2})", sell_count, sell_notional);
        println!("    Total: {:.2}", buy_notional + sell_notional);
        println!();
    }
}

fn print_funding_stats(root: &Path) {
    let dir = root.join("fundings");
    let groups = collect_and_group(&dir, "_funding_");

    println!(" ── [4/5] Funding Rates ──");

    if groups.is_empty() {
        println!("  No Parquet files found in {}\n", dir.display());
        return;
    }

    println!(
        "  Files: {} ({} symbols)\n",
        total_files(&groups),
        groups.len()
    );

    for (symbol, files) in &groups {
        println!("  ── {} ({} files) ──", symbol, files.len());

        let mut all_rates: Vec<FundingRate> = Vec::new();
        let mut read_errors = 0_u32;

        for path in files {
            match read_funding_parquet(path) {
                Ok(rates) => all_rates.extend(rates),
                Err(e) => {
                    eprintln!("    Warning: {}: {:?}", path.display(), e);
                    read_errors += 1;
                }
            }
        }

        if read_errors > 0 {
            println!("    Read errors: {}", read_errors);
        }

        let n = all_rates.len();
        println!("    Records: {}", n);

        if n == 0 {
            println!();
            continue;
        }

        all_rates.sort_by_key(|r| r.funding_rate_ts_us);

        let first_ts = all_rates[0].funding_rate_ts_us;
        let last_ts = all_rates[n - 1].funding_rate_ts_us;
        let span_s = (last_ts - first_ts) as f64 / 1e6;

        println!("    First: {} us ({})", first_ts, format_ts_us(first_ts));
        println!("    Last:  {} us ({})", last_ts, format_ts_us(last_ts));
        println!("    Span:  {:.3}s", span_s);

        let rates: Vec<f64> = all_rates
            .iter()
            .map(|r| r.funding_rate.to_f64().unwrap_or(0.0))
            .collect();
        let (r_min, r_max, r_mean) = stats_f64(&rates);
        let ann_factor = 3.0 * 365.0;

        println!(
            "    Rate: min={:.6}  max={:.6}  avg={:.6}",
            r_min, r_max, r_mean
        );
        println!(
            "    Ann. (3×365): min={:.2}%  max={:.2}%  avg={:.2}%",
            r_min * ann_factor * 100.0,
            r_max * ann_factor * 100.0,
            r_mean * ann_factor * 100.0,
        );
        println!();
    }
}

fn print_open_interest_stats(root: &Path) {
    let dir = root.join("open_interests");
    let groups = collect_and_group(&dir, "_oi_");

    println!(" ── [5/5] Open Interest ──");

    if groups.is_empty() {
        println!("  No Parquet files found in {}\n", dir.display());
        return;
    }

    println!(
        "  Files: {} ({} symbols)\n",
        total_files(&groups),
        groups.len()
    );

    for (symbol, files) in &groups {
        println!("  ── {} ({} files) ──", symbol, files.len());

        let mut all_oi: Vec<OpenInterest> = Vec::new();
        let mut read_errors = 0_u32;

        for path in files {
            match read_oi_parquet(path) {
                Ok(records) => all_oi.extend(records),
                Err(e) => {
                    eprintln!("    Warning: {}: {:?}", path.display(), e);
                    read_errors += 1;
                }
            }
        }

        if read_errors > 0 {
            println!("    Read errors: {}", read_errors);
        }

        let n = all_oi.len();
        println!("    Records: {}", n);

        if n == 0 {
            println!();
            continue;
        }

        all_oi.sort_by_key(|o| o.open_interest_ts_us);

        let first_ts = all_oi[0].open_interest_ts_us;
        let last_ts = all_oi[n - 1].open_interest_ts_us;
        let span_s = (last_ts - first_ts) as f64 / 1e6;

        println!("    First: {} us ({})", first_ts, format_ts_us(first_ts));
        println!("    Last:  {} us ({})", last_ts, format_ts_us(last_ts));
        println!("    Span:  {:.3}s", span_s);

        let oi_values: Vec<f64> = all_oi
            .iter()
            .map(|o| o.open_interest.to_f64().unwrap_or(0.0))
            .collect();
        let oi_usd: Vec<f64> = all_oi
            .iter()
            .map(|o| {
                o.open_interest_value
                    .and_then(|v| v.to_f64())
                    .unwrap_or(0.0)
            })
            .collect();
        let (oi_min, oi_max, oi_mean) = stats_f64(&oi_values);
        let (usd_min, usd_max, usd_mean) = stats_f64(&oi_usd);

        println!(
            "    OI (contracts): min={:.2}  max={:.2}  avg={:.2}",
            oi_min, oi_max, oi_mean
        );
        println!(
            "    OI (USD):       min={:.2}  max={:.2}  avg={:.2}",
            usd_min, usd_max, usd_mean
        );
        println!();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

fn main() {
    let cli = Cli::parse();
    let root = &cli.dir;

    if !root.is_dir() {
        eprintln!("Error: {} is not a directory", root.display());
        std::process::exit(1);
    }

    println!();
    println!(" ══════════════════════════════════════════════════════════════");
    println!(" MarketWorker Parquet Dataset — Per-Symbol Statistics");
    println!(" ══════════════════════════════════════════════════════════════");
    println!("  Dataset: {}", root.display());
    println!(" ══════════════════════════════════════════════════════════════");
    println!();

    print_orderbook_stats(root);
    print_trade_stats(root);
    print_liquidation_stats(root);
    print_funding_stats(root);
    print_open_interest_stats(root);

    // Summary
    let ob_files = total_files(&collect_and_group(&root.join("orderbooks"), "_ob_"));
    let tr_files = total_files(&collect_and_group(&root.join("trades"), "_trades_"));
    let liq_files = total_files(&collect_and_group(
        &root.join("liquidations"),
        "_liquidations_",
    ));
    let fr_files = total_files(&collect_and_group(&root.join("fundings"), "_funding_"));
    let oi_files = total_files(&collect_and_group(&root.join("open_interests"), "_oi_"));

    let total = ob_files + tr_files + liq_files + fr_files + oi_files;
    let types_present = [ob_files, tr_files, liq_files, fr_files, oi_files]
        .iter()
        .filter(|&&c| c > 0)
        .count();

    println!(" ══════════════════════════════════════════════════════════════");
    println!(
        "  Summary: {} Parquet files across {}/5 data types",
        total, types_present
    );
    println!(
        "    orderbooks={} trades={} liquidations={} funding={} oi={}",
        ob_files, tr_files, liq_files, fr_files, oi_files
    );

    if types_present == 0 {
        println!("  No data found. Run the MarketWorker first.");
    }

    println!(" ══════════════════════════════════════════════════════════════");
    println!();
}
