use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::LazyLock;

use chrono::NaiveDate;
use futures_util::StreamExt;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use aetelier_entrepot::EntrepotError;
use aetelier_entrepot::codec;
use aetelier_entrepot::source::ObjectSource;

use super::budget::{ConnectionBudget, SourceMetrics};
use super::model::{DomainEvent, ReconstructionModel};
use super::registry::{ExchangeAdapter, ExchangeProfile, TaskExit};
use super::symbol::SymbolCodec;
use crate::errors::ExchangeError;
use aetelier_types::config::markets::market_config::{DeclaredDatatype, DeclaredSet};

pub struct DecodedLine {
    pub events: Vec<DomainEvent>,
    pub local_ts_us: Option<u64>,
}

pub enum LineReject {
    Undecodable(Box<ExchangeError>),
    UnsupportedVersion { found: u64 },
}

pub trait LineDecoder: Send + Sync + 'static {
    fn decode_line(&self, line: &str) -> Result<DecodedLine, LineReject>;
}

pub struct HyperliquidEnvelopeLines;

impl LineDecoder for HyperliquidEnvelopeLines {
    fn decode_line(&self, line: &str) -> Result<DecodedLine, LineReject> {
        let events = super::adapters::hyperliquid::HYPERLIQUID
            .replay_frame(line)
            .map_err(LineReject::Undecodable)?;
        Ok(DecodedLine {
            events,
            local_ts_us: None,
        })
    }
}

/// The probe-verified hyperliquid-archive line (S0, 2026-08-11): a wrapper
/// `{"time":"<RFC3339 ns, no zone>","ver_num":1,"raw":<WSS envelope>}`,
/// byte-stable from 2023-09 through 2026-08. `raw` replays through the live
/// decoder; the wrapper `time` is the node's receipt clock and becomes the
/// event's local timestamp.
#[derive(serde::Deserialize)]
struct ArchiveLine<'a> {
    time: &'a str,
    ver_num: u64,
    #[serde(borrow)]
    raw: &'a serde_json::value::RawValue,
}

pub const ARCHIVE_LINE_VERSION: u64 = 1;

pub struct HyperliquidArchiveLines;

impl LineDecoder for HyperliquidArchiveLines {
    fn decode_line(&self, line: &str) -> Result<DecodedLine, LineReject> {
        let parsed: ArchiveLine = serde_json::from_str(line)
            .map_err(|e| LineReject::Undecodable(Box::new(ExchangeError::from(e))))?;
        if parsed.ver_num != ARCHIVE_LINE_VERSION {
            return Err(LineReject::UnsupportedVersion {
                found: parsed.ver_num,
            });
        }
        let events = super::adapters::hyperliquid::HYPERLIQUID
            .replay_frame(parsed.raw.get())
            .map_err(LineReject::Undecodable)?;
        let local_ts_us =
            chrono::NaiveDateTime::parse_from_str(parsed.time, "%Y-%m-%dT%H:%M:%S%.f")
                .ok()
                .map(|dt| dt.and_utc().timestamp_micros() as u64);
        Ok(DecodedLine {
            events,
            local_ts_us,
        })
    }
}

#[derive(Debug, Clone)]
pub struct EntrepotWindow {
    pub start: NaiveDate,
    pub end: NaiveDate,
    pub coins: Vec<String>,
}

pub fn hyperliquid_l2book_keys(window: &EntrepotWindow) -> Vec<String> {
    let mut keys = Vec::new();
    let mut date = window.start;
    while date <= window.end {
        let day = date.format("%Y%m%d");
        for hour in 0..24 {
            for coin in &window.coins {
                keys.push(format!("market_data/{day}/{hour}/l2Book/{coin}.lz4"));
            }
        }
        date = date
            .succ_opt()
            .expect("date range stays within chrono bounds");
    }
    keys
}

pub fn hyperliquid_asset_ctxs_keys(window: &EntrepotWindow) -> Vec<String> {
    let mut keys = Vec::new();
    let mut date = window.start;
    while date <= window.end {
        keys.push(format!("asset_ctxs/{}.csv.lz4", date.format("%Y%m%d")));
        date = date
            .succ_opt()
            .expect("date range stays within chrono bounds");
    }
    keys
}

pub const ASSET_CTXS_HEADER: &str = "time,coin,funding,open_interest,prev_day_px,day_ntl_vlm,premium,oracle_px,mark_px,mid_px,impact_bid_px,impact_ask_px";

fn parse_l2book_key(key: &str) -> Option<(u32, String)> {
    let mut seg = key.split('/');
    if seg.next()? != "market_data" {
        return None;
    }
    let _day = seg.next()?;
    let hour = seg.next()?.parse::<u32>().ok()?;
    if seg.next()? != "l2Book" {
        return None;
    }
    let coin = seg.next()?.strip_suffix(".lz4")?.to_string();
    if seg.next().is_some() {
        return None;
    }
    Some((hour, coin))
}

pub async fn discover_coins(
    source: &dyn ObjectSource,
    date: NaiveDate,
) -> Result<Vec<String>, EntrepotError> {
    let day = date.format("%Y%m%d");
    let listed = source.list(&format!("market_data/{day}/")).await?;
    let mut coins: Vec<String> = listed
        .iter()
        .filter_map(|m| parse_l2book_key(&m.key))
        .map(|(_, coin)| coin)
        .collect();
    coins.sort();
    coins.dedup();
    Ok(coins)
}

fn key_date(key: &str) -> Option<NaiveDate> {
    let day = match key.strip_prefix("asset_ctxs/") {
        Some(rest) => rest.get(..8)?,
        None => key.split('/').nth(1)?,
    };
    NaiveDate::parse_from_str(day, "%Y%m%d").ok()
}

struct WindowPlan {
    keys: Vec<String>,
    etags: HashMap<String, String>,
    observed_edge: Option<NaiveDate>,
}

pub fn planned_keys(window: &EntrepotWindow, want_ctx: bool) -> Vec<String> {
    let mut keys = Vec::new();
    let mut date = window.start;
    while date <= window.end {
        let day = date.format("%Y%m%d");
        if want_ctx {
            keys.push(format!("asset_ctxs/{day}.csv.lz4"));
        }
        for hour in 0..24 {
            for coin in &window.coins {
                keys.push(format!("market_data/{day}/{hour}/l2Book/{coin}.lz4"));
            }
        }
        date = date
            .succ_opt()
            .expect("date range stays within chrono bounds");
    }
    keys
}

async fn enumerate_window(
    source: &dyn ObjectSource,
    window: &EntrepotWindow,
    want_ctx: bool,
) -> Result<WindowPlan, EntrepotError> {
    if !window.coins.is_empty() {
        return Ok(WindowPlan {
            keys: planned_keys(window, want_ctx),
            etags: HashMap::new(),
            observed_edge: None,
        });
    }
    let mut keys = Vec::new();
    let mut etags = HashMap::new();
    let mut observed_edge = None;
    let mut date = window.start;
    while date <= window.end {
        let day = date.format("%Y%m%d");
        if want_ctx {
            keys.push(format!("asset_ctxs/{day}.csv.lz4"));
        }
        let listed = source.list(&format!("market_data/{day}/")).await?;
        if !listed.is_empty() {
            observed_edge = Some(date);
        }
        let mut hour_coins: Vec<(u32, String)> = listed
            .iter()
            .filter_map(|m| parse_l2book_key(&m.key))
            .collect();
        hour_coins.sort();
        for m in listed {
            if let Some(tag) = m.etag {
                etags.insert(m.key, tag);
            }
        }
        for (hour, coin) in hour_coins {
            keys.push(format!("market_data/{day}/{hour}/l2Book/{coin}.lz4"));
        }
        date = date
            .succ_opt()
            .expect("date range stays within chrono bounds");
    }
    Ok(WindowPlan {
        keys,
        etags,
        observed_edge,
    })
}

const EDGE_PROBE_CAP: u32 = 45;

async fn probe_edge(
    source: &dyn ObjectSource,
    window: &EntrepotWindow,
) -> Option<NaiveDate> {
    let mut date = window.end;
    let mut probes = 0u32;
    loop {
        if probes >= EDGE_PROBE_CAP {
            tracing::warn!(probes, "entrepot.edge_unknown");
            return None;
        }
        let day = date.format("%Y%m%d");
        match source.list(&format!("market_data/{day}/")).await {
            Ok(listed) if !listed.is_empty() => return Some(date),
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(error = %e, "entrepot.edge_probe_failed");
                return None;
            }
        }
        probes += 1;
        if date <= window.start {
            return None;
        }
        date = date.pred_opt()?;
    }
}

fn resume_index(cursor: Option<&Path>, keys: &[String]) -> usize {
    let Some(path) = cursor else {
        return 0;
    };
    let Ok(raw) = std::fs::read_to_string(path) else {
        return 0;
    };
    let last = raw.trim();
    if last.is_empty() {
        return 0;
    }
    match keys.iter().position(|k| k == last) {
        Some(pos) => {
            tracing::info!(cursor = last, resumed_at = pos + 1, "entrepot.cursor_resume");
            pos + 1
        }
        None => {
            tracing::warn!(cursor = last, "entrepot.cursor_unmatched");
            0
        }
    }
}

fn write_cursor(path: &Path, key: &str) {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
        && let Err(e) = std::fs::create_dir_all(parent)
    {
        tracing::warn!(error = %e, "entrepot.cursor_write_failed");
        return;
    }
    let part = PathBuf::from(format!("{}.part", path.display()));
    if let Err(e) =
        std::fs::write(&part, key).and_then(|_| std::fs::rename(&part, path))
    {
        tracing::warn!(error = %e, "entrepot.cursor_write_failed");
    }
}

fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

fn object_absent(err: &EntrepotError) -> bool {
    match err {
        EntrepotError::Status { status, .. } => *status == 404,
        EntrepotError::Io { source, .. } => source.kind() == std::io::ErrorKind::NotFound,
        _ => false,
    }
}

fn guarded_decode<F>(key: &str, decode: F) -> Result<Vec<String>, String>
where
    F: FnOnce() -> Result<Vec<String>, EntrepotError> + std::panic::UnwindSafe,
{
    match std::panic::catch_unwind(decode) {
        Ok(Ok(lines)) => Ok(lines),
        Ok(Err(e)) => Err(e.to_string()),
        Err(panic) => {
            let reason = panic
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "opaque panic".to_string());
            Err(format!("decoder panicked for {key}: {reason}"))
        }
    }
}

fn log_transfer_summary(source: &dyn ObjectSource) {
    if let Some(s) = source.transfer_snapshot() {
        tracing::info!(
            get_requests = s.get_requests,
            list_requests = s.list_requests,
            retries = s.retries,
            bytes_in = s.bytes_in,
            unpaid_responses = s.unpaid_responses,
            integrity_fail = s.integrity_fail,
            "entrepot.transfer_summary"
        );
    }
}

static ENTREPOT_HYPERLIQUID_PROFILE: LazyLock<ExchangeProfile> =
    LazyLock::new(|| ExchangeProfile {
        id: "hyperliquid",
        symbol_codec: SymbolCodec::BareCoin { quote: "USDC" },
        budget: ConnectionBudget::default(),
        schema_version: 1,
        protocol_revision: "hyperliquid-entrepot-v0",
    });

async fn emit_ctx_rows(
    key: &str,
    lines: &[String],
    window: &EntrepotWindow,
    declared: &DeclaredSet,
    tx: &mpsc::Sender<DomainEvent>,
    recv_seq: &mut u64,
    metrics: &SourceMetrics,
) -> Result<(), ()> {
    let Some((header, rows)) = lines.split_first() else {
        return Ok(());
    };
    if header.trim() != ASSET_CTXS_HEADER {
        metrics.bump_ver_rejected();
        tracing::warn!(key, "entrepot.ctx_header_drift");
        return Ok(());
    }
    for row in rows {
        let cols: Vec<&str> = row.split(',').collect();
        if cols.len() != 12 {
            metrics.bump_decode_err();
            tracing::warn!(key, "entrepot.ctx_row_malformed");
            continue;
        }
        let coin = cols[1];
        if !window.coins.is_empty() && !window.coins.iter().any(|c| c == coin) {
            continue;
        }
        let Ok(ts) = chrono::DateTime::parse_from_rfc3339(cols[0]) else {
            metrics.bump_decode_err();
            tracing::warn!(key, "entrepot.ctx_bad_timestamp");
            continue;
        };
        let ts_us = ts.timestamp_micros() as u64;
        let Some(pair) = ENTREPOT_HYPERLIQUID_PROFILE.symbol_codec.decode(coin) else {
            metrics.add_dropped_frames(1);
            continue;
        };
        let mut events: Vec<DomainEvent> = Vec::with_capacity(2);
        if declared.contains(DeclaredDatatype::FundingRates) {
            match cols[2].parse::<rust_decimal::Decimal>() {
                Ok(rate) => events.push(DomainEvent::FundingRate(
                    aetelier_types::funding::FundingRate {
                        funding_rate_ts_us: ts_us,
                        local_funding_ts_us: 0,
                        recv_seq: 0,
                        conn_epoch: 0,
                        pair: pair.clone(),
                        funding_rate: rate,
                        premium: cols[6].parse().ok(),
                        interval_hours: 1,
                        next_funding_ts_us: 0,
                        exchange: "hyperliquid".to_string(),
                    },
                )),
                Err(_) => {
                    metrics.add_dropped_frames(1);
                    tracing::warn!(key, coin, "entrepot.ctx_bad_funding_decimal");
                }
            }
        }
        if declared.contains(DeclaredDatatype::OpenInterest) {
            match cols[3].parse::<rust_decimal::Decimal>() {
                Ok(oi) => events.push(DomainEvent::OpenInterest(
                    aetelier_types::open_interest::OpenInterest {
                        open_interest_ts_us: ts_us,
                        local_oi_ts_us: 0,
                        recv_seq: 0,
                        conn_epoch: 0,
                        pair: pair.clone(),
                        open_interest: oi,
                        open_interest_value: None,
                        mark_px: cols[8].parse().ok(),
                        exchange: "hyperliquid".to_string(),
                    },
                )),
                Err(_) => {
                    metrics.add_dropped_frames(1);
                    tracing::warn!(key, coin, "entrepot.ctx_bad_oi_decimal");
                }
            }
        }
        for mut event in events {
            *recv_seq += 1;
            event.stamp_local(ts_us, 0, *recv_seq);
            metrics.bump_msgs();
            if tx.send(event).await.is_err() {
                return Err(());
            }
        }
    }
    Ok(())
}

pub struct HyperliquidEntrepotAdapter {
    source: Arc<dyn ObjectSource>,
    window: EntrepotWindow,
    decoder: Arc<dyn LineDecoder>,
    fetch_concurrency: usize,
    cursor: Option<PathBuf>,
}

impl HyperliquidEntrepotAdapter {
    pub fn new(source: Arc<dyn ObjectSource>, window: &EntrepotWindow) -> Self {
        Self {
            source,
            window: window.clone(),
            decoder: Arc::new(HyperliquidArchiveLines),
            fetch_concurrency: 1,
            cursor: None,
        }
    }

    pub fn with_decoder(mut self, decoder: Arc<dyn LineDecoder>) -> Self {
        self.decoder = decoder;
        self
    }

    pub fn with_concurrency(mut self, fetch_concurrency: usize) -> Self {
        self.fetch_concurrency = fetch_concurrency.max(1);
        self
    }

    pub fn with_cursor(mut self, cursor: Option<PathBuf>) -> Self {
        self.cursor = cursor;
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct EntrepotOptions {
    pub fetch_concurrency: usize,
    pub cursor: Option<PathBuf>,
}

pub fn build_entrepot_adapter(
    venue: &str,
    source: Arc<dyn ObjectSource>,
    window: &EntrepotWindow,
    opts: &EntrepotOptions,
) -> Option<Box<dyn ExchangeAdapter>> {
    match venue {
        "hyperliquid" => Some(Box::new(
            HyperliquidEntrepotAdapter::new(source, window)
                .with_concurrency(opts.fetch_concurrency.max(1))
                .with_cursor(opts.cursor.clone()),
        )),
        _ => None,
    }
}

impl ExchangeAdapter for HyperliquidEntrepotAdapter {
    fn id(&self) -> &'static str {
        "hyperliquid"
    }

    fn profile(&self) -> &ExchangeProfile {
        &ENTREPOT_HYPERLIQUID_PROFILE
    }

    fn book_model(&self, _channel: &str) -> ReconstructionModel {
        ReconstructionModel::FullRefresh
    }

    fn supported_datatypes(
        &self,
    ) -> &'static [aetelier_types::config::markets::market_config::DeclaredDatatype] {
        use aetelier_types::config::markets::market_config::DeclaredDatatype as DD;
        &[
            DD::Orderbook,
            DD::Trades,
            DD::FundingRates,
            DD::OpenInterest,
        ]
    }

    fn max_declared_depth(&self) -> Option<usize> {
        Some(20)
    }

    fn spawn(
        &self,
        _symbols: Vec<String>,
        declared: DeclaredSet,
        tx: mpsc::Sender<DomainEvent>,
        shutdown: watch::Receiver<bool>,
        metrics: SourceMetrics,
    ) -> JoinHandle<TaskExit> {
        let source = Arc::clone(&self.source);
        let decoder = Arc::clone(&self.decoder);
        let window = self.window.clone();
        let concurrency = self.fetch_concurrency.max(1);
        let cursor_path = self.cursor.clone();
        tokio::spawn(async move {
            let want_ctx = declared.contains(DeclaredDatatype::FundingRates)
                || declared.contains(DeclaredDatatype::OpenInterest);
            let plan =
                match enumerate_window(source.as_ref(), &window, want_ctx).await {
                    Ok(plan) => plan,
                    Err(e) => {
                        tracing::error!(error = %e, "entrepot.enumerate_failed");
                        log_transfer_summary(source.as_ref());
                        return TaskExit::Failed(
                            crate::clients::disconnect::DisconnectReason::TransportError {
                                source: e.to_string().into(),
                            },
                        );
                    }
                };
            let edge = match plan.observed_edge {
                Some(date) => Some(date),
                None if window.coins.is_empty() => None,
                None => probe_edge(source.as_ref(), &window).await,
            };
            tracing::info!(
                edge = edge
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "unknown".to_string()),
                keys = plan.keys.len(),
                "entrepot.coverage_edge"
            );
            let start_idx = resume_index(cursor_path.as_deref(), &plan.keys);
            let tail: Vec<String> = plan.keys[start_idx..].to_vec();
            let mut fetches = futures_util::stream::iter(tail.into_iter().map(|key| {
                let src = Arc::clone(&source);
                async move {
                    let res = src.get_object(&key).await;
                    (key, res)
                }
            }))
            .buffered(concurrency);
            let mut recv_seq: u64 = 0;
            let mut ver_drift_warned = false;
            let exit = loop {
                let Some((key, res)) = fetches.next().await else {
                    break TaskExit::Exhausted;
                };
                if *shutdown.borrow() {
                    break TaskExit::Completed;
                }
                let fetched = match res {
                    Ok(f) => f,
                    Err(e) if object_absent(&e) => {
                        let beyond = match (edge, key_date(&key)) {
                            (Some(edge), Some(date)) => date > edge,
                            (None, _) => true,
                            (_, None) => false,
                        };
                        if beyond {
                            tracing::debug!(
                                key = key.as_str(),
                                "entrepot.object_beyond_edge"
                            );
                            metrics.bump_gaps_beyond_edge();
                        } else {
                            tracing::debug!(
                                key = key.as_str(),
                                "entrepot.object_absent"
                            );
                            metrics.bump_gaps();
                        }
                        if let Some(path) = cursor_path.as_deref() {
                            write_cursor(path, &key);
                        }
                        continue;
                    }
                    Err(e @ EntrepotError::Integrity { .. }) => {
                        tracing::error!(key = key.as_str(), error = %e, "entrepot.integrity_fail");
                        metrics.bump_integrity_fail();
                        if let Some(path) = cursor_path.as_deref() {
                            write_cursor(path, &key);
                        }
                        continue;
                    }
                    Err(e) => {
                        tracing::error!(key = key.as_str(), error = %e, "entrepot.fetch_failed");
                        log_transfer_summary(source.as_ref());
                        return TaskExit::Failed(
                            crate::clients::disconnect::DisconnectReason::TransportError {
                                source: e.to_string().into(),
                            },
                        );
                    }
                };
                if let (Some(pinned), Some(got)) =
                    (plan.etags.get(&key), fetched.etag.as_deref())
                    && pinned != got
                {
                    tracing::warn!(
                        key = key.as_str(),
                        pinned = pinned.as_str(),
                        got,
                        "entrepot.object_republished"
                    );
                    metrics.bump_republished();
                }
                let lines = match guarded_decode(&key, {
                    let key = key.clone();
                    let bytes = fetched.bytes;
                    move || {
                        codec::decode_lz4(&key, &bytes)
                            .and_then(|d| codec::utf8_lines(&key, &d))
                    }
                }) {
                    Ok(lines) => lines,
                    Err(reason) => {
                        tracing::error!(key = key.as_str(), reason, "entrepot.decode_failed");
                        metrics.bump_decode_err();
                        if let Some(path) = cursor_path.as_deref() {
                            write_cursor(path, &key);
                        }
                        continue;
                    }
                };
                if key.starts_with("asset_ctxs/") {
                    if emit_ctx_rows(
                        &key,
                        &lines,
                        &window,
                        &declared,
                        &tx,
                        &mut recv_seq,
                        &metrics,
                    )
                    .await
                    .is_err()
                    {
                        break TaskExit::Completed;
                    }
                } else {
                    let mut closed = false;
                    for line in &lines {
                        match decoder.decode_line(line) {
                            Ok(decoded) => {
                                let local = decoded.local_ts_us.unwrap_or_else(now_us);
                                for mut event in decoded.events {
                                    recv_seq += 1;
                                    event.stamp_local(local, 0, recv_seq);
                                    metrics.bump_msgs();
                                    if tx.send(event).await.is_err() {
                                        closed = true;
                                        break;
                                    }
                                }
                            }
                            Err(LineReject::UnsupportedVersion { found }) => {
                                metrics.bump_ver_rejected();
                                if !ver_drift_warned {
                                    ver_drift_warned = true;
                                    tracing::warn!(
                                        key = key.as_str(),
                                        found,
                                        expected = ARCHIVE_LINE_VERSION,
                                        "entrepot.ver_num_drift"
                                    );
                                } else {
                                    tracing::debug!(
                                        key = key.as_str(),
                                        found,
                                        "entrepot.ver_rejected"
                                    );
                                }
                            }
                            Err(LineReject::Undecodable(e)) => {
                                tracing::warn!(key = key.as_str(), error = %e, "entrepot.line_undecodable");
                                metrics.bump_decode_err();
                            }
                        }
                        if closed {
                            break;
                        }
                    }
                    if closed {
                        break TaskExit::Completed;
                    }
                }
                if let Some(path) = cursor_path.as_deref() {
                    write_cursor(path, &key);
                }
            };
            log_transfer_summary(source.as_ref());
            exit
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    const SOL_FIXTURE: &str = "datasets/hyperliquid-archive/sol_20230916_h9.jsonl.lz4";
    const BTC_FIXTURE: &str = "datasets/hyperliquid-archive/btc_20260801_h9.jsonl.lz4";
    const CTX_FIXTURE: &str =
        "datasets/hyperliquid-archive/asset_ctxs_20230916.csv.lz4";
    const HOUR9_START_US: u64 = 1_694_854_800_000_000;
    const HOUR9_END_US: u64 = 1_694_858_400_000_000;

    fn fixture_bytes(rel: &str) -> Vec<u8> {
        let path = format!("{}/{rel}", env!("CARGO_MANIFEST_DIR"));
        std::fs::read(path).unwrap()
    }

    fn window(coins: &[&str], y: i32, m: u32, d: u32) -> EntrepotWindow {
        EntrepotWindow {
            start: NaiveDate::from_ymd_opt(y, m, d).unwrap(),
            end: NaiveDate::from_ymd_opt(y, m, d).unwrap(),
            coins: coins.iter().map(|c| c.to_string()).collect(),
        }
    }

    fn stage_bytes(root: &std::path::Path, key: &str, bytes: &[u8]) {
        let path = root.join(key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    }

    async fn run_adapter(
        adapter: &HyperliquidEntrepotAdapter,
        declared: DeclaredSet,
    ) -> (Vec<DomainEvent>, TaskExit, SourceMetrics) {
        let (tx, mut rx) = mpsc::channel(8192);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let metrics = SourceMetrics::default();
        let handle =
            adapter.spawn(Vec::new(), declared, tx, shutdown_rx, metrics.clone());
        let mut events = Vec::new();
        while let Some(ev) = rx.recv().await {
            events.push(ev);
        }
        let exit = handle.await.unwrap();
        (events, exit, metrics)
    }

    fn lz4_lines(lines: &[String]) -> Vec<u8> {
        let mut enc = lz4_flex::frame::FrameEncoder::new(Vec::new());
        std::io::Write::write_all(&mut enc, lines.join("\n").as_bytes()).unwrap();
        enc.finish().unwrap()
    }

    fn sol_fixture_lines() -> Vec<String> {
        let decoded =
            aetelier_entrepot::codec::decode_lz4("sol", &fixture_bytes(SOL_FIXTURE))
                .unwrap();
        aetelier_entrepot::codec::utf8_lines("sol", &decoded).unwrap()
    }

    #[test]
    fn key_grammar_matches_the_documented_layout() {
        let mut w = window(&["BTC"], 2026, 8, 5);
        w.end = NaiveDate::from_ymd_opt(2026, 8, 6).unwrap();
        let keys = hyperliquid_l2book_keys(&w);
        assert_eq!(keys.len(), 48);
        assert_eq!(keys[0], "market_data/20260805/0/l2Book/BTC.lz4");
        assert_eq!(keys[9], "market_data/20260805/9/l2Book/BTC.lz4");
        assert_eq!(keys[47], "market_data/20260806/23/l2Book/BTC.lz4");
        assert!(keys.iter().all(|k| !k.contains("/09/")));
    }

    #[test]
    fn asset_ctxs_key_grammar_matches_the_documented_layout() {
        let mut w = window(&[], 2026, 8, 5);
        w.end = NaiveDate::from_ymd_opt(2026, 8, 6).unwrap();
        assert_eq!(
            hyperliquid_asset_ctxs_keys(&w),
            [
                "asset_ctxs/20260805.csv.lz4",
                "asset_ctxs/20260806.csv.lz4"
            ]
        );
    }

    #[test]
    fn decoder_panics_classify_as_decode_failures_not_task_death() {
        let err = guarded_decode("market_data/x/BTC.lz4", || {
            panic!("attempt to add with overflow")
        })
        .unwrap_err();
        assert!(err.contains("panicked"));
        assert!(err.contains("overflow"));
        let ok = guarded_decode("k", || Ok(vec!["line".to_string()])).unwrap();
        assert_eq!(ok, ["line"]);
        let plain = guarded_decode("k", || {
            Err(EntrepotError::Decode {
                key: "k".to_string(),
                reason: "bad frame".to_string(),
            })
        })
        .unwrap_err();
        assert!(plain.contains("bad frame"));
    }

    #[test]
    fn planned_keys_interleave_ctx_first_per_date() {
        let mut w = window(&["BTC"], 2026, 8, 5);
        w.end = NaiveDate::from_ymd_opt(2026, 8, 6).unwrap();
        let keys = planned_keys(&w, true);
        assert_eq!(keys.len(), 50);
        assert_eq!(keys[0], "asset_ctxs/20260805.csv.lz4");
        assert_eq!(keys[1], "market_data/20260805/0/l2Book/BTC.lz4");
        assert_eq!(keys[25], "asset_ctxs/20260806.csv.lz4");
        assert_eq!(keys[49], "market_data/20260806/23/l2Book/BTC.lz4");
        assert_eq!(planned_keys(&w, false), hyperliquid_l2book_keys(&w));
    }

    #[test]
    fn archive_line_decodes_raw_and_stamps_the_node_receipt_clock() {
        let lines = sol_fixture_lines();
        assert_eq!(lines.len(), 150);

        let first = HyperliquidArchiveLines.decode_line(&lines[0]).ok().unwrap();
        assert_eq!(first.events.len(), 1);
        let ts = first.local_ts_us.expect("wrapper time parsed");
        assert_eq!(ts, 1_694_854_801_039_593);
        match &first.events[0] {
            DomainEvent::Book(d) => {
                assert!(d.is_snapshot);
                assert!(!d.bids.is_empty() && d.bids.len() <= 20);
                assert_eq!(d.symbol, "SOL");
            }
            other => panic!("expected a book, got {other:?}"),
        }
    }

    #[test]
    fn archive_books_cap_at_the_same_twenty_levels_as_live() {
        let decoded_file =
            aetelier_entrepot::codec::decode_lz4("btc", &fixture_bytes(BTC_FIXTURE))
                .unwrap();
        let lines = aetelier_entrepot::codec::utf8_lines("btc", &decoded_file).unwrap();
        assert_eq!(lines.len(), 100);
        for line in &lines {
            let decoded = HyperliquidArchiveLines.decode_line(line).ok().unwrap();
            match &decoded.events[0] {
                DomainEvent::Book(d) => {
                    assert_eq!(d.bids.len(), 20);
                    assert_eq!(d.asks.len(), 20);
                    assert_eq!(d.symbol, "BTC");
                }
                other => panic!("expected a book, got {other:?}"),
            }
        }
        let first = HyperliquidArchiveLines.decode_line(&lines[0]).ok().unwrap();
        assert_eq!(
            first.local_ts_us.unwrap(),
            1_785_574_804_715_463,
            "wrapper clock parses across the 2026 era too"
        );
    }

    #[test]
    fn version_drift_is_refused_before_replay() {
        let lines = sol_fixture_lines();
        let mut value: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
        value["ver_num"] = serde_json::json!(2);
        let v2 = value.to_string();
        match HyperliquidArchiveLines.decode_line(&v2) {
            Err(LineReject::UnsupportedVersion { found }) => assert_eq!(found, 2),
            _ => panic!("a v2 line must be refused as unsupported"),
        }
    }

    #[tokio::test]
    async fn replays_a_real_archive_hour_and_exhausts() {
        let dir = tempfile::tempdir().unwrap();
        stage_bytes(
            dir.path(),
            "market_data/20230916/9/l2Book/SOL.lz4",
            &fixture_bytes(SOL_FIXTURE),
        );

        let source = Arc::new(aetelier_entrepot::LocalDirSource::new(dir.path()));
        let adapter =
            HyperliquidEntrepotAdapter::new(source, &window(&["SOL"], 2023, 9, 16));
        let (events, exit, metrics) = run_adapter(&adapter, DeclaredSet::all()).await;

        assert!(matches!(exit, TaskExit::Exhausted));
        assert_eq!(events.len(), 150);
        for ev in &events {
            if let DomainEvent::Book(d) = ev {
                assert!(d.local_orderbook_ts_us >= HOUR9_START_US);
                assert!(d.local_orderbook_ts_us < HOUR9_END_US);
            } else {
                panic!("l2Book objects decode to books only");
            }
        }
        let m = metrics.snapshot();
        assert_eq!(m.msgs, 150);
        assert_eq!(m.decode_err, 0);
        assert_eq!(
            m.gaps, 24,
            "23 unstaged hours plus the absent ctx day object count as gaps"
        );
        assert_eq!(m.gaps_beyond_edge, 0);
        assert_eq!(m.ver_rejected, 0);
    }

    #[tokio::test]
    async fn missing_hours_skip_and_corrupt_objects_count() {
        let dir = tempfile::tempdir().unwrap();
        stage_bytes(
            dir.path(),
            "market_data/20230916/3/l2Book/SOL.lz4",
            &fixture_bytes(SOL_FIXTURE),
        );
        stage_bytes(
            dir.path(),
            "market_data/20230916/4/l2Book/SOL.lz4",
            b"not an lz4 frame",
        );

        let source = Arc::new(aetelier_entrepot::LocalDirSource::new(dir.path()));
        let adapter =
            HyperliquidEntrepotAdapter::new(source, &window(&["SOL"], 2023, 9, 16));
        let (events, exit, metrics) =
            run_adapter(&adapter, DeclaredSet::only(DeclaredDatatype::Orderbook))
                .await;

        assert!(matches!(exit, TaskExit::Exhausted));
        assert_eq!(events.len(), 150);
        let m = metrics.snapshot();
        assert_eq!(m.gaps, 22, "absent hours skip");
        assert_eq!(m.decode_err, 1, "the corrupt hour counts loudly");
    }

    #[tokio::test]
    async fn shutdown_interrupts_with_completed_not_exhausted() {
        let dir = tempfile::tempdir().unwrap();
        let source = Arc::new(aetelier_entrepot::LocalDirSource::new(dir.path()));
        let adapter =
            HyperliquidEntrepotAdapter::new(source, &window(&["BTC"], 2026, 8, 5));

        let (tx, _rx) = mpsc::channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(true);
        let handle = adapter.spawn(
            vec!["BTC".to_string()],
            DeclaredSet::only(DeclaredDatatype::Orderbook),
            tx,
            shutdown_rx,
            SourceMetrics::default(),
        );
        let exit = handle.await.unwrap();
        drop(shutdown_tx);
        assert!(matches!(exit, TaskExit::Completed));
    }

    #[tokio::test]
    async fn universe_mode_enumerates_from_listing_and_orders_across_coins() {
        let dir = tempfile::tempdir().unwrap();
        stage_bytes(
            dir.path(),
            "market_data/20230916/9/l2Book/BTC.lz4",
            &fixture_bytes(BTC_FIXTURE),
        );
        stage_bytes(
            dir.path(),
            "market_data/20230916/9/l2Book/SOL.lz4",
            &fixture_bytes(SOL_FIXTURE),
        );
        stage_bytes(
            dir.path(),
            "market_data/20230917/0/l2Book/BTC.lz4",
            &fixture_bytes(BTC_FIXTURE),
        );

        let source: Arc<dyn ObjectSource> =
            Arc::new(aetelier_entrepot::LocalDirSource::new(dir.path()));
        let coins = discover_coins(
            source.as_ref(),
            NaiveDate::from_ymd_opt(2023, 9, 16).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(coins, ["BTC", "SOL"]);
        let coins_next = discover_coins(
            source.as_ref(),
            NaiveDate::from_ymd_opt(2023, 9, 17).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(coins_next, ["BTC"], "a delisted coin leaves the universe");

        let mut w = window(&[], 2023, 9, 16);
        w.end = NaiveDate::from_ymd_opt(2023, 9, 17).unwrap();
        let adapter = HyperliquidEntrepotAdapter::new(Arc::clone(&source), &w);
        let (events, exit, metrics) =
            run_adapter(&adapter, DeclaredSet::only(DeclaredDatatype::Orderbook))
                .await;

        assert!(matches!(exit, TaskExit::Exhausted));
        assert_eq!(events.len(), 350);
        let symbols: Vec<&str> = events
            .iter()
            .map(|ev| match ev {
                DomainEvent::Book(d) => d.symbol.as_str(),
                other => panic!("expected books, got {other:?}"),
            })
            .collect();
        assert!(symbols[..100].iter().all(|s| *s == "BTC"));
        assert!(symbols[100..250].iter().all(|s| *s == "SOL"));
        assert!(symbols[250..].iter().all(|s| *s == "BTC"));
        let m = metrics.snapshot();
        assert_eq!(m.msgs, 350);
        assert_eq!(m.gaps, 0, "listed keys exist; nothing is fabricated");
        assert_eq!(m.gaps_beyond_edge, 0);
    }

    #[tokio::test]
    async fn absent_objects_past_the_edge_classify_distinctly() {
        let dir = tempfile::tempdir().unwrap();
        stage_bytes(
            dir.path(),
            "market_data/20230916/0/l2Book/SOL.lz4",
            &fixture_bytes(SOL_FIXTURE),
        );

        let source = Arc::new(aetelier_entrepot::LocalDirSource::new(dir.path()));
        let mut w = window(&["SOL"], 2023, 9, 16);
        w.end = NaiveDate::from_ymd_opt(2023, 9, 18).unwrap();
        let adapter = HyperliquidEntrepotAdapter::new(source, &w);
        let (events, exit, metrics) =
            run_adapter(&adapter, DeclaredSet::only(DeclaredDatatype::Orderbook))
                .await;

        assert!(matches!(exit, TaskExit::Exhausted));
        assert_eq!(events.len(), 150);
        let m = metrics.snapshot();
        assert_eq!(m.gaps, 23, "absences inside coverage stay gaps");
        assert_eq!(
            m.gaps_beyond_edge, 48,
            "two whole days past the observed edge classify as beyond-edge"
        );
    }

    #[tokio::test]
    async fn concurrent_fetch_preserves_emission_order() {
        let dir = tempfile::tempdir().unwrap();
        for hour in [3u32, 5, 9] {
            stage_bytes(
                dir.path(),
                &format!("market_data/20230916/{hour}/l2Book/SOL.lz4"),
                &fixture_bytes(SOL_FIXTURE),
            );
        }
        let source: Arc<dyn ObjectSource> =
            Arc::new(aetelier_entrepot::LocalDirSource::new(dir.path()));

        let sequential = HyperliquidEntrepotAdapter::new(
            Arc::clone(&source),
            &window(&["SOL"], 2023, 9, 16),
        );
        let (seq_events, _, _) =
            run_adapter(&sequential, DeclaredSet::only(DeclaredDatatype::Orderbook))
                .await;

        let concurrent = HyperliquidEntrepotAdapter::new(
            Arc::clone(&source),
            &window(&["SOL"], 2023, 9, 16),
        )
        .with_concurrency(4);
        let (con_events, exit, metrics) =
            run_adapter(&concurrent, DeclaredSet::only(DeclaredDatatype::Orderbook))
                .await;

        assert!(matches!(exit, TaskExit::Exhausted));
        assert_eq!(seq_events.len(), 450);
        assert_eq!(con_events.len(), 450);
        let ts = |evs: &[DomainEvent]| -> Vec<u64> {
            evs.iter()
                .map(|ev| match ev {
                    DomainEvent::Book(d) => d.local_orderbook_ts_us,
                    other => panic!("expected books, got {other:?}"),
                })
                .collect()
        };
        assert_eq!(ts(&seq_events), ts(&con_events));
        assert_eq!(metrics.snapshot().msgs, 450);
    }

    #[tokio::test]
    async fn cursor_resumes_after_restart_and_makes_reruns_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        stage_bytes(
            dir.path(),
            "market_data/20230916/3/l2Book/SOL.lz4",
            &fixture_bytes(SOL_FIXTURE),
        );
        stage_bytes(
            dir.path(),
            "market_data/20230916/9/l2Book/SOL.lz4",
            &fixture_bytes(SOL_FIXTURE),
        );
        let cursor_dir = tempfile::tempdir().unwrap();
        let cursor = cursor_dir.path().join("sol.cursor");
        let source: Arc<dyn ObjectSource> =
            Arc::new(aetelier_entrepot::LocalDirSource::new(dir.path()));

        let first = HyperliquidEntrepotAdapter::new(
            Arc::clone(&source),
            &window(&["SOL"], 2023, 9, 16),
        )
        .with_cursor(Some(cursor.clone()));
        let (events, exit, _) =
            run_adapter(&first, DeclaredSet::only(DeclaredDatatype::Orderbook)).await;
        assert!(matches!(exit, TaskExit::Exhausted));
        assert_eq!(events.len(), 300);
        assert_eq!(
            std::fs::read_to_string(&cursor).unwrap(),
            "market_data/20230916/23/l2Book/SOL.lz4"
        );

        let rerun = HyperliquidEntrepotAdapter::new(
            Arc::clone(&source),
            &window(&["SOL"], 2023, 9, 16),
        )
        .with_cursor(Some(cursor.clone()));
        let (events, exit, metrics) =
            run_adapter(&rerun, DeclaredSet::only(DeclaredDatatype::Orderbook)).await;
        assert!(matches!(exit, TaskExit::Exhausted));
        assert_eq!(events.len(), 0, "a finished window re-emits nothing");
        assert_eq!(metrics.snapshot().msgs, 0);

        std::fs::write(&cursor, "market_data/20230916/3/l2Book/SOL.lz4").unwrap();
        let resumed = HyperliquidEntrepotAdapter::new(
            Arc::clone(&source),
            &window(&["SOL"], 2023, 9, 16),
        )
        .with_cursor(Some(cursor.clone()));
        let (events, exit, metrics) =
            run_adapter(&resumed, DeclaredSet::only(DeclaredDatatype::Orderbook))
                .await;
        assert!(matches!(exit, TaskExit::Exhausted));
        assert_eq!(events.len(), 150, "only keys after the cursor replay");
        assert_eq!(metrics.snapshot().gaps, 19);
    }

    #[tokio::test]
    async fn asset_ctxs_rows_map_to_funding_and_open_interest() {
        let dir = tempfile::tempdir().unwrap();
        stage_bytes(
            dir.path(),
            "asset_ctxs/20230916.csv.lz4",
            &fixture_bytes(CTX_FIXTURE),
        );
        let source: Arc<dyn ObjectSource> =
            Arc::new(aetelier_entrepot::LocalDirSource::new(dir.path()));
        let adapter = HyperliquidEntrepotAdapter::new(
            Arc::clone(&source),
            &window(&["AAVE"], 2023, 9, 16),
        );
        let (events, exit, metrics) = run_adapter(&adapter, DeclaredSet::all()).await;

        assert!(matches!(exit, TaskExit::Exhausted));
        assert_eq!(events.len(), 12, "six minutes, one funding and one oi each");
        match &events[0] {
            DomainEvent::FundingRate(fr) => {
                assert_eq!(fr.funding_rate_ts_us, 1_694_822_400_000_000);
                assert_eq!(fr.local_funding_ts_us, 1_694_822_400_000_000);
                assert_eq!(fr.funding_rate.to_string(), "-0.00001822");
                assert_eq!(
                    fr.premium.as_ref().map(|p| p.to_string()).as_deref(),
                    Some("-0.00044573")
                );
                assert_eq!(fr.interval_hours, 1);
                assert_eq!(fr.exchange, "hyperliquid");
                assert_eq!(fr.pair.base(), "AAVE");
                assert_eq!(fr.pair.quote(), "USDC");
                assert_eq!(fr.recv_seq, 1);
            }
            other => panic!("expected a funding rate first, got {other:?}"),
        }
        match &events[1] {
            DomainEvent::OpenInterest(oi) => {
                assert_eq!(oi.open_interest_ts_us, 1_694_822_400_000_000);
                assert_eq!(oi.open_interest.to_string(), "479.75");
                assert_eq!(
                    oi.mark_px.as_ref().map(|p| p.to_string()).as_deref(),
                    Some("55.837")
                );
                assert_eq!(oi.open_interest_value, None);
                assert_eq!(oi.recv_seq, 2);
            }
            other => panic!("expected open interest second, got {other:?}"),
        }
        let seqs: Vec<u64> = events
            .iter()
            .map(|ev| match ev {
                DomainEvent::FundingRate(fr) => fr.recv_seq,
                DomainEvent::OpenInterest(oi) => oi.recv_seq,
                other => panic!("unexpected event {other:?}"),
            })
            .collect();
        assert!(seqs.windows(2).all(|w| w[1] == w[0] + 1));
        let m = metrics.snapshot();
        assert_eq!(m.msgs, 12);
        assert_eq!(m.decode_err, 0);
        assert_eq!(m.ver_rejected, 0);
    }

    #[tokio::test]
    async fn orderbook_only_declaration_never_enumerates_ctx_objects() {
        let dir = tempfile::tempdir().unwrap();
        stage_bytes(
            dir.path(),
            "asset_ctxs/20230916.csv.lz4",
            &fixture_bytes(CTX_FIXTURE),
        );
        let source: Arc<dyn ObjectSource> =
            Arc::new(aetelier_entrepot::LocalDirSource::new(dir.path()));
        let adapter = HyperliquidEntrepotAdapter::new(
            Arc::clone(&source),
            &window(&["AAVE"], 2023, 9, 16),
        );
        let (events, exit, _) =
            run_adapter(&adapter, DeclaredSet::only(DeclaredDatatype::Orderbook))
                .await;
        assert!(matches!(exit, TaskExit::Exhausted));
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn ctx_header_drift_refuses_the_file_loudly() {
        let dir = tempfile::tempdir().unwrap();
        let drifted = lz4_lines(&[
            "time,coin,funding,open_interest,new_mystery_column".to_string(),
            "2023-09-16T00:00:00Z,AAVE,-0.00001822,479.75,1".to_string(),
        ]);
        stage_bytes(dir.path(), "asset_ctxs/20230916.csv.lz4", &drifted);
        let source: Arc<dyn ObjectSource> =
            Arc::new(aetelier_entrepot::LocalDirSource::new(dir.path()));
        let adapter = HyperliquidEntrepotAdapter::new(
            Arc::clone(&source),
            &window(&["AAVE"], 2023, 9, 16),
        );
        let (events, exit, metrics) = run_adapter(&adapter, DeclaredSet::all()).await;
        assert!(matches!(exit, TaskExit::Exhausted));
        assert!(events.is_empty());
        let m = metrics.snapshot();
        assert_eq!(m.ver_rejected, 1);
        assert_eq!(m.decode_err, 0);
    }

    #[tokio::test]
    async fn version_drift_counts_distinctly_and_keeps_replaying() {
        let lines = sol_fixture_lines();
        let mut v2: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
        v2["ver_num"] = serde_json::json!(2);
        let staged = lz4_lines(&[
            lines[0].clone(),
            v2.to_string(),
            v2.to_string(),
            lines[1].clone(),
        ]);

        let dir = tempfile::tempdir().unwrap();
        stage_bytes(dir.path(), "market_data/20230916/9/l2Book/SOL.lz4", &staged);
        let source: Arc<dyn ObjectSource> =
            Arc::new(aetelier_entrepot::LocalDirSource::new(dir.path()));
        let adapter = HyperliquidEntrepotAdapter::new(
            Arc::clone(&source),
            &window(&["SOL"], 2023, 9, 16),
        );
        let (events, exit, metrics) =
            run_adapter(&adapter, DeclaredSet::only(DeclaredDatatype::Orderbook))
                .await;

        assert!(matches!(exit, TaskExit::Exhausted));
        assert_eq!(events.len(), 2, "v1 lines around the drift still replay");
        let m = metrics.snapshot();
        assert_eq!(m.ver_rejected, 2);
        assert_eq!(m.decode_err, 0, "drift is not decode noise");
    }

    struct FatalSource;

    #[async_trait]
    impl ObjectSource for FatalSource {
        async fn list(
            &self,
            _prefix: &str,
        ) -> Result<Vec<aetelier_entrepot::ObjectMeta>, EntrepotError> {
            Ok(Vec::new())
        }

        async fn get(&self, key: &str) -> Result<Vec<u8>, EntrepotError> {
            Err(EntrepotError::Status {
                status: 403,
                key: key.to_string(),
                body: "AccessDenied".to_string(),
            })
        }
    }

    struct ExhaustedSource;

    #[async_trait]
    impl ObjectSource for ExhaustedSource {
        async fn list(
            &self,
            _prefix: &str,
        ) -> Result<Vec<aetelier_entrepot::ObjectMeta>, EntrepotError> {
            Ok(Vec::new())
        }

        async fn get(&self, key: &str) -> Result<Vec<u8>, EntrepotError> {
            Err(EntrepotError::Exhausted {
                attempts: 9,
                key: key.to_string(),
                last: "http 503".to_string(),
            })
        }
    }

    async fn exit_reason(adapter: &HyperliquidEntrepotAdapter) -> String {
        let (events, exit, _) =
            run_adapter(adapter, DeclaredSet::only(DeclaredDatatype::Orderbook)).await;
        assert!(events.is_empty());
        match exit {
            TaskExit::Failed(reason) => format!("{reason:?}"),
            other => panic!("expected a failed exit, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fatal_statuses_fail_the_task_with_body_context() {
        let adapter = HyperliquidEntrepotAdapter::new(
            Arc::new(FatalSource),
            &window(&["SOL"], 2023, 9, 16),
        );
        let reason = exit_reason(&adapter).await;
        assert!(reason.contains("403"));
        assert!(reason.contains("AccessDenied"));
    }

    #[tokio::test]
    async fn retry_exhaustion_fails_the_task_loudly() {
        let adapter = HyperliquidEntrepotAdapter::new(
            Arc::new(ExhaustedSource),
            &window(&["SOL"], 2023, 9, 16),
        );
        let reason = exit_reason(&adapter).await;
        assert!(reason.contains("exhausted"));
        assert!(reason.contains("http 503"));
    }

    struct RepublishedSource {
        key: String,
        bytes: Vec<u8>,
    }

    #[async_trait]
    impl ObjectSource for RepublishedSource {
        async fn list(
            &self,
            prefix: &str,
        ) -> Result<Vec<aetelier_entrepot::ObjectMeta>, EntrepotError> {
            if self.key.starts_with(prefix) {
                Ok(vec![aetelier_entrepot::ObjectMeta {
                    key: self.key.clone(),
                    size: self.bytes.len() as u64,
                    etag: Some("etag-at-listing".to_string()),
                    last_modified: None,
                }])
            } else {
                Ok(Vec::new())
            }
        }

        async fn get(&self, _key: &str) -> Result<Vec<u8>, EntrepotError> {
            Ok(self.bytes.clone())
        }

        async fn get_object(
            &self,
            _key: &str,
        ) -> Result<aetelier_entrepot::FetchedObject, EntrepotError> {
            Ok(aetelier_entrepot::FetchedObject {
                bytes: self.bytes.clone(),
                etag: Some("etag-after-republication".to_string()),
                request_charged: false,
            })
        }
    }

    #[tokio::test]
    async fn republished_objects_surface_via_etag_drift() {
        let adapter = HyperliquidEntrepotAdapter::new(
            Arc::new(RepublishedSource {
                key: "market_data/20230916/9/l2Book/SOL.lz4".to_string(),
                bytes: fixture_bytes(SOL_FIXTURE),
            }),
            &window(&[], 2023, 9, 16),
        );
        let (events, exit, metrics) =
            run_adapter(&adapter, DeclaredSet::only(DeclaredDatatype::Orderbook))
                .await;
        assert!(matches!(exit, TaskExit::Exhausted));
        assert_eq!(events.len(), 150);
        assert_eq!(metrics.snapshot().republished, 1);
    }

    #[test]
    fn profile_pins_full_refresh_reconstruction() {
        let dir = tempfile::tempdir().unwrap();
        let source: Arc<dyn ObjectSource> =
            Arc::new(aetelier_entrepot::LocalDirSource::new(dir.path()));
        let adapter =
            HyperliquidEntrepotAdapter::new(source, &window(&["BTC"], 2026, 8, 5));
        assert!(matches!(
            adapter.book_model("l2Book"),
            ReconstructionModel::FullRefresh
        ));
        let profile = adapter.profile();
        assert_eq!(profile.schema_version, 1);
        assert_eq!(profile.protocol_revision, "hyperliquid-entrepot-v0");
        assert_eq!(adapter.max_declared_depth(), Some(20));
    }

    #[test]
    fn factory_knows_hyperliquid_and_nothing_else() {
        let dir = tempfile::tempdir().unwrap();
        let source: Arc<dyn ObjectSource> =
            Arc::new(aetelier_entrepot::LocalDirSource::new(dir.path()));
        let opts = EntrepotOptions {
            fetch_concurrency: 4,
            cursor: None,
        };
        assert!(
            build_entrepot_adapter(
                "hyperliquid",
                Arc::clone(&source),
                &window(&["BTC"], 2026, 8, 5),
                &opts
            )
            .is_some()
        );
        assert!(
            build_entrepot_adapter(
                "binance",
                source,
                &window(&["BTC"], 2026, 8, 5),
                &opts
            )
            .is_none()
        );
    }
}
