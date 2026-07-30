//! Batch rehydration CLI — repair persisted trade parquets from venue REST.
//!
//! Scans a trades directory, set-differences the persisted prints against the
//! venue's REST trades endpoint over the same id range, and writes one
//! `*_rehydrated` parquet holding the complete merged set (originals
//! untouched; recovered rows carry `origin = rest`). Prints the report as
//! JSON on stdout.
//!
//! Usage:
//!   rehydrate --trades-dir datasets/collected/binance/trades \
//!             --exchange binance --symbol BTCUSDT
//!
//! Venue coverage follows the live-reconciliation fetcher registry
//! (binance / bitso / coinbase today); venues whose REST retention cannot
//! repair history exit with an explicit error rather than a silent no-op.

use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    name = "rehydrate",
    about = "Repair persisted trade parquets from the venue REST trades endpoint"
)]
struct Args {
    /// Directory holding the trades parquet files to repair.
    #[arg(long)]
    trades_dir: std::path::PathBuf,
    /// Venue id (e.g. binance, bitso, coinbase).
    #[arg(long)]
    exchange: String,
    /// Venue wire symbol (e.g. BTCUSDT, btc_mxn, BTC-USD).
    #[arg(long)]
    symbol: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let Some(fetcher) =
        aetelier_connect::framework::reconcile::trades_rest_fetcher(&args.exchange)
    else {
        anyhow::bail!(
            "venue '{}' has no REST trades fetcher — its retention cannot \
             repair history (see the completeness product's venue matrix)",
            args.exchange
        );
    };
    let adapter = aetelier_connect::framework::registry::registry()
        .get(args.exchange.as_str())
        .copied()
        .ok_or_else(|| anyhow::anyhow!("unknown venue '{}'", args.exchange))?;
    let pair = adapter
        .profile()
        .symbol_codec
        .decode(&args.symbol)
        .ok_or_else(|| {
            anyhow::anyhow!("cannot decode symbol {} for {}", args.symbol, args.exchange)
        })?;

    let report = aetelier_io::rehydrate::rehydrate_trades_dir(
        &args.trades_dir,
        &args.exchange,
        &args.symbol,
        &pair,
        fetcher.as_ref(),
    )
    .await?;

    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
