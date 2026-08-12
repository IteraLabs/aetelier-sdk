use std::path::PathBuf;

use chrono::NaiveDate;
use clap::{Parser, Subcommand};

use aetelier_connect::config::workers::MarketWorkerManifest;
use aetelier_connect::framework::entrepot::{
    EntrepotWindow, discover_coins, planned_keys,
};
use aetelier_entrepot::{ObjectSource, S3Client, S3Config};

const L2BOOK_MIN_BYTES: u64 = 156 * 1024;
const L2BOOK_MAX_BYTES: u64 = 1_258_291;
const CTX_MIN_BYTES: u64 = 2_700_000;
const CTX_MAX_BYTES: u64 = 10_400_000;
const GET_COST_PER_1K: f64 = 0.0004;
const EGRESS_PER_GB_INTERNET: f64 = 0.09;
const EDGE_WALKBACK_CAP: u32 = 45;

#[derive(Parser, Debug)]
#[command(
    name = "entrepot",
    version,
    about = "Costed planning, manifest generation, and cursor status for entrepot backfills; md_worker is the runner"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    #[command(about = "Estimate objects, bytes, and cost for a window before spending")]
    Plan {
        #[arg(long)]
        coin: String,
        #[arg(long)]
        start: NaiveDate,
        #[arg(long)]
        end: NaiveDate,
        #[arg(long, help = "Include asset_ctxs daily objects (FundingRates + OpenInterest)")]
        ctx: bool,
        #[arg(long, default_value = "hyperliquid-archive")]
        bucket: String,
        #[arg(long, default_value = "us-east-1")]
        region: String,
        #[arg(long, help = "LIST the bucket (paid, receipted) for coverage edge + universe; needs AWS creds in env")]
        live: bool,
        #[arg(long, help = "Filter the --live universe names by case-insensitive substring")]
        find: Option<String>,
    },
    #[command(about = "Emit a validated md_worker manifest for the window")]
    Manifest {
        #[arg(long)]
        coin: String,
        #[arg(long)]
        start: NaiveDate,
        #[arg(long)]
        end: NaiveDate,
        #[arg(long, help = "Include asset_ctxs daily objects (FundingRates + OpenInterest)")]
        ctx: bool,
        #[arg(long, help = "Parquet output directory the worker writes into")]
        out: PathBuf,
        #[arg(long, help = "Manifest file path; defaults to entrepot-<coin>-<start>-<end>.toml")]
        manifest_out: Option<PathBuf>,
        #[arg(long, default_value = "hyperliquid-archive")]
        bucket: String,
        #[arg(long, default_value = "us-east-1")]
        region: String,
        #[arg(long, help = "Read a local directory tree instead of S3")]
        root: Option<PathBuf>,
        #[arg(long, help = "Cursor sidecar path; defaults to <out>/<coin>.cursor")]
        cursor: Option<PathBuf>,
        #[arg(long, default_value_t = 1)]
        concurrency: usize,
        #[arg(long, help = "Anonymous access: public buckets, no credentials, no requester-pays")]
        anonymous: bool,
        #[arg(long)]
        endpoint: Option<String>,
        #[arg(long, help = "Stop the run after this many hours (fractional ok, e.g. 0.33 = 20 min); emits [session]")]
        duration_hours: Option<f64>,
    },
    #[command(about = "Report cursor position against the manifest's planned window")]
    Status {
        #[arg(long)]
        manifest: PathBuf,
    },
    #[command(about = "Raw LIST of a bucket prefix: distinct directories or keys (paid, receipted)")]
    List {
        #[arg(long)]
        prefix: String,
        #[arg(long, default_value = "hyperliquid-archive")]
        bucket: String,
        #[arg(long, default_value = "us-east-1")]
        region: String,
        #[arg(long, help = "Print distinct parent directories with object counts instead of keys")]
        dirs: bool,
        #[arg(long, default_value_t = 200)]
        limit: usize,
        #[arg(long, help = "Single delimited LIST page: prints CommonPrefixes (e.g. / for top-level dirs); one request, no pagination")]
        delimiter: Option<String>,
    },
}

struct WindowShape {
    l2book_objects: u64,
    ctx_objects: u64,
    min_bytes: u64,
    max_bytes: u64,
}

fn shape(start: NaiveDate, end: NaiveDate, ctx: bool) -> anyhow::Result<WindowShape> {
    if end < start {
        anyhow::bail!("end {end} precedes start {start}");
    }
    let days = (end - start).num_days() as u64 + 1;
    let l2book_objects = days * 24;
    let ctx_objects = if ctx { days } else { 0 };
    Ok(WindowShape {
        l2book_objects,
        ctx_objects,
        min_bytes: l2book_objects * L2BOOK_MIN_BYTES + ctx_objects * CTX_MIN_BYTES,
        max_bytes: l2book_objects * L2BOOK_MAX_BYTES + ctx_objects * CTX_MAX_BYTES,
    })
}

fn human_bytes(bytes: u64) -> String {
    let gb = bytes as f64 / 1e9;
    if gb >= 1.0 {
        format!("{gb:.2} GB")
    } else {
        format!("{:.1} MB", bytes as f64 / 1e6)
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_plan(
    coin: String,
    start: NaiveDate,
    end: NaiveDate,
    ctx: bool,
    bucket: String,
    region: String,
    live: bool,
    find: Option<String>,
) -> anyhow::Result<()> {
    let s = shape(start, end, ctx)?;
    let objects = s.l2book_objects + s.ctx_objects;
    let request_cost = objects as f64 / 1000.0 * GET_COST_PER_1K;
    let egress_min = s.min_bytes as f64 / 1e9 * EGRESS_PER_GB_INTERNET;
    let egress_max = s.max_bytes as f64 / 1e9 * EGRESS_PER_GB_INTERNET;

    println!("window            {start} .. {end}  ({} days)", (end - start).num_days() + 1);
    println!("coin              {coin}");
    println!("bucket            s3://{bucket}  ({region}, requester-pays)");
    println!("l2Book objects    {}", s.l2book_objects);
    println!("asset_ctxs        {}", s.ctx_objects);
    println!("GET requests      {objects}  (~${request_cost:.4})");
    println!(
        "bytes (est)       {} .. {}",
        human_bytes(s.min_bytes),
        human_bytes(s.max_bytes)
    );
    println!("egress internet   ${egress_min:.2} .. ${egress_max:.2}");
    println!("egress same-region$0.00");

    if live {
        let cfg = S3Config::from_env(&bucket, &region)?;
        let client = S3Client::new(cfg);
        let mut probe = end;
        let mut edge: Option<(NaiveDate, Vec<String>)> = None;
        for _ in 0..EDGE_WALKBACK_CAP {
            let coins = discover_coins(&client, probe).await?;
            if !coins.is_empty() {
                edge = Some((probe, coins));
                break;
            }
            match probe.pred_opt() {
                Some(prev) if prev >= start => probe = prev,
                _ => break,
            }
        }
        match edge {
            Some((date, coins)) => {
                println!("coverage edge     {date}  ({} coins listed)", coins.len());
                match &find {
                    Some(needle) => {
                        let needle = needle.to_lowercase();
                        let matches: Vec<&String> = coins
                            .iter()
                            .filter(|c| c.to_lowercase().contains(&needle))
                            .collect();
                        if matches.is_empty() {
                            println!("matches           none for \"{}\"", find.as_deref().unwrap_or_default());
                        } else {
                            for m in matches {
                                println!("match             {m}");
                            }
                        }
                    }
                    None => {
                        println!("universe          {}", coins.join(" "));
                    }
                }
            }
            None => println!("coverage edge     none found in window (walk-back capped)"),
        }
        if let Some(stats) = client.transfer_snapshot() {
            println!(
                "probe receipt     {} LIST requests, {} bytes",
                stats.list_requests, stats.bytes_in
            );
        }
    }
    Ok(())
}

fn render_manifest(
    coin: &str,
    start: NaiveDate,
    end: NaiveDate,
    ctx: bool,
    out: &std::path::Path,
    bucket: &str,
    region: &str,
    root: Option<&std::path::Path>,
    cursor: &std::path::Path,
    concurrency: usize,
    anonymous: bool,
    endpoint: Option<&str>,
    duration_hours: Option<f64>,
) -> String {
    let source = match root {
        Some(root) => format!("source = \"local\"\nroot = \"{}\"", root.display()),
        None => {
            let mut s = format!(
                "source = \"s3\"\nbucket = \"{bucket}\"\nregion = \"{region}\""
            );
            if anonymous {
                s.push_str("\nanonymous = true");
            } else {
                s.push_str("\nrequester_pays = true");
            }
            if let Some(e) = endpoint {
                s.push_str(&format!("\nendpoint = \"{e}\""));
            }
            s
        }
    };
    let ctx_sections = if ctx {
        "\n[collect.datatypes.funding_rates]\nenabled = true\n\n[collect.datatypes.open_interest]\nenabled = true\n"
    } else {
        ""
    };
    let session = match duration_hours {
        Some(h) => format!("\n[session]\nduration_hours = {h}\n"),
        None => String::new(),
    };
    format!(
        r#"[collect]
exchange = "hyperliquid"
market_type = "perpetual"
transport = "entrepot"
framework_ingest = true

[collect.entrepot]
{source}
start = "{start}"
end = "{end}"
cursor = "{cursor}"
fetch_concurrency = {concurrency}

[collect.reconnect]
initial_delay_ms = 1000
max_delay_ms = 5000
max_attempts = 3

[collect.datatypes.orderbook]
enabled = true
depth = 20
{ctx_sections}
[collect.sync]
sync_mode = "on_orderbook"
flush_threshold = 600

[collect.sync.update_frequency]
value = 500
unit = "Millis"

[[collect.output]]
type = "parquet"
dir = "{out}"

[[workers]]
symbol = "{coin}"
{session}"#,
        cursor = cursor.display(),
        out = out.display(),
    )
}

#[allow(clippy::too_many_arguments)]
fn run_manifest(
    coin: String,
    start: NaiveDate,
    end: NaiveDate,
    ctx: bool,
    out: PathBuf,
    manifest_out: Option<PathBuf>,
    bucket: String,
    region: String,
    root: Option<PathBuf>,
    cursor: Option<PathBuf>,
    concurrency: usize,
    anonymous: bool,
    endpoint: Option<String>,
    duration_hours: Option<f64>,
) -> anyhow::Result<()> {
    shape(start, end, ctx)?;
    let cursor = cursor.unwrap_or_else(|| out.join(format!("{coin}.cursor")));
    let manifest_path = manifest_out.unwrap_or_else(|| {
        PathBuf::from(format!(
            "entrepot-{coin}-{}-{}.toml",
            start.format("%Y%m%d"),
            end.format("%Y%m%d")
        ))
    });
    let toml = render_manifest(
        &coin,
        start,
        end,
        ctx,
        &out,
        &bucket,
        &region,
        root.as_deref(),
        &cursor,
        concurrency.max(1),
        anonymous,
        endpoint.as_deref(),
        duration_hours,
    );
    let parsed = MarketWorkerManifest::from_str(&toml)?;
    let resolved = parsed.resolve_all();
    anyhow::ensure!(
        resolved.len() == 1,
        "manifest resolved {} workers, expected 1",
        resolved.len()
    );
    std::fs::create_dir_all(&out)?;
    std::fs::write(&manifest_path, &toml)?;
    println!("manifest written  {}", manifest_path.display());
    println!("parquet output    {}", out.display());
    println!("cursor            {}", cursor.display());
    println!();
    println!("run it with:");
    println!(
        "  cargo run -q -p aetelier-sdk --features parquet --bin md_worker -- --config {}",
        manifest_path.display()
    );
    if root.is_none() && !anonymous {
        println!();
        println!("requires AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY in the environment");
    }
    Ok(())
}

fn run_status(manifest: PathBuf) -> anyhow::Result<()> {
    let parsed = MarketWorkerManifest::from_toml(&manifest)?;
    let cfg = parsed
        .resolve_all()
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("manifest resolves no workers"))?;
    let section = cfg
        .entrepot
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("manifest has no [collect.entrepot] section"))?;
    let want_ctx = cfg.common.datatypes.funding_rates.enabled
        || cfg.common.datatypes.open_interest.enabled;
    let window = EntrepotWindow {
        start: section.start,
        end: section.end,
        coins: vec![cfg.common.symbol.clone()],
    };
    let keys = planned_keys(&window, want_ctx);
    let total = keys.len();
    let Some(cursor_path) = section.cursor.as_ref() else {
        println!("window            {} .. {}", section.start, section.end);
        println!("planned objects   {total}");
        println!("cursor            none configured — restarts refetch the window");
        return Ok(());
    };
    println!("window            {} .. {}", section.start, section.end);
    println!("planned objects   {total}");
    match std::fs::read_to_string(cursor_path) {
        Ok(raw) => {
            let last = raw.trim().to_string();
            match keys.iter().position(|k| k == &last) {
                Some(pos) => {
                    let done = pos + 1;
                    println!(
                        "completed         {done}/{total}  ({:.1}%)",
                        done as f64 / total as f64 * 100.0
                    );
                    println!("cursor            {last}");
                    println!("remaining         {} objects", total - done);
                }
                None => {
                    println!("cursor            {last}  (NOT IN PLAN — run restarts from zero)");
                }
            }
        }
        Err(_) => {
            println!(
                "cursor            {} absent — run starts from zero",
                cursor_path.display()
            );
        }
    }
    Ok(())
}

async fn run_list(
    prefix: String,
    bucket: String,
    region: String,
    dirs: bool,
    limit: usize,
    delimiter: Option<String>,
) -> anyhow::Result<()> {
    let cfg = S3Config::from_env(&bucket, &region)?;
    let client = S3Client::new(cfg);
    if let Some(delim) = delimiter {
        let page = client.list_delimited(&prefix, &delim).await?;
        println!("prefix            \"{prefix}\"  delimiter \"{delim}\"");
        for cp in &page.common_prefixes {
            println!("dir               {cp}");
        }
        for m in page.objects.iter().take(limit) {
            println!("{:>12}  {}", m.size, m.key);
        }
        if page.is_truncated {
            println!("truncated         more entries exist past this page");
        }
        if let Some(stats) = client.transfer_snapshot() {
            println!(
                "probe receipt     {} LIST requests, {} bytes",
                stats.list_requests, stats.bytes_in
            );
        }
        return Ok(());
    }
    let listed = client.list(&prefix).await?;
    println!("prefix            {prefix}");
    println!("objects           {}", listed.len());
    if dirs {
        let mut counts: std::collections::BTreeMap<String, u64> =
            std::collections::BTreeMap::new();
        for m in &listed {
            let parent = match m.key.rfind('/') {
                Some(i) => m.key[..=i].to_string(),
                None => "<root>".to_string(),
            };
            *counts.entry(parent).or_insert(0) += 1;
        }
        for (dir, n) in counts.iter().take(limit) {
            println!("{n:>8}  {dir}");
        }
        if counts.len() > limit {
            println!("          ... {} more directories", counts.len() - limit);
        }
    } else {
        for m in listed.iter().take(limit) {
            println!("{:>12}  {}", m.size, m.key);
        }
        if listed.len() > limit {
            println!("          ... {} more objects", listed.len() - limit);
        }
    }
    if let Some(stats) = client.transfer_snapshot() {
        println!(
            "probe receipt     {} LIST requests, {} bytes",
            stats.list_requests, stats.bytes_in
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Plan {
            coin,
            start,
            end,
            ctx,
            bucket,
            region,
            live,
            find,
        } => run_plan(coin, start, end, ctx, bucket, region, live, find).await,
        Command::Manifest {
            coin,
            start,
            end,
            ctx,
            out,
            manifest_out,
            bucket,
            region,
            root,
            cursor,
            concurrency,
            anonymous,
            endpoint,
            duration_hours,
        } => run_manifest(
            coin,
            start,
            end,
            ctx,
            out,
            manifest_out,
            bucket,
            region,
            root,
            cursor,
            concurrency,
            anonymous,
            endpoint,
            duration_hours,
        ),
        Command::Status { manifest } => run_status(manifest),
        Command::List {
            prefix,
            bucket,
            region,
            dirs,
            limit,
            delimiter,
        } => run_list(prefix, bucket, region, dirs, limit, delimiter).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> NaiveDate {
        s.parse().unwrap()
    }

    #[test]
    fn shape_counts_objects_and_bounds_bytes() {
        let s = shape(d("2026-07-01"), d("2026-07-07"), true).unwrap();
        assert_eq!(s.l2book_objects, 168);
        assert_eq!(s.ctx_objects, 7);
        assert!(s.min_bytes < s.max_bytes);
        assert!(shape(d("2026-07-02"), d("2026-07-01"), false).is_err());
    }

    #[test]
    fn rendered_manifest_parses_and_resolves_one_worker() {
        let toml = render_manifest(
            "BTC",
            d("2026-07-01"),
            d("2026-07-07"),
            true,
            std::path::Path::new("/tmp/out"),
            "hyperliquid-archive",
            "us-east-1",
            None,
            std::path::Path::new("/tmp/out/BTC.cursor"),
            4,
            false,
            None,
            Some(0.33),
        );
        let cfg = MarketWorkerManifest::from_str(&toml)
            .unwrap()
            .resolve_all()
            .remove(0);
        let reconnect = cfg.common.reconnect.clone().unwrap();
        assert_eq!(reconnect.max_attempts, Some(3));
        let section = cfg.entrepot.unwrap();
        assert_eq!(section.bucket.as_deref(), Some("hyperliquid-archive"));
        assert_eq!(section.requester_pays, Some(true));
        assert_eq!(section.fetch_concurrency, Some(4));
        assert!(cfg.common.datatypes.funding_rates.enabled);
        assert!(cfg.common.datatypes.open_interest.enabled);
        assert_eq!(cfg.common.symbol, "BTC");
    }

    #[test]
    fn rendered_local_manifest_parses_without_credentials_keys() {
        let toml = render_manifest(
            "SOL",
            d("2023-09-16"),
            d("2023-09-16"),
            false,
            std::path::Path::new("/tmp/out"),
            "unused",
            "unused",
            Some(std::path::Path::new("/data/hl-archive")),
            std::path::Path::new("/tmp/out/SOL.cursor"),
            1,
            false,
            None,
            None,
        );
        let cfg = MarketWorkerManifest::from_str(&toml)
            .unwrap()
            .resolve_all()
            .remove(0);
        let section = cfg.entrepot.unwrap();
        assert_eq!(
            section.root,
            Some(std::path::PathBuf::from("/data/hl-archive"))
        );
        assert!(!cfg.common.datatypes.funding_rates.enabled);
    }
}
