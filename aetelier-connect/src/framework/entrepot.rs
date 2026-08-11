use std::sync::Arc;
use std::sync::LazyLock;

use chrono::NaiveDate;
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
use aetelier_types::config::markets::market_config::DeclaredSet;

pub trait LineDecoder: Send + Sync + 'static {
    fn decode_line(&self, line: &str) -> Result<Vec<DomainEvent>, Box<ExchangeError>>;
}

pub struct HyperliquidEnvelopeLines;

impl LineDecoder for HyperliquidEnvelopeLines {
    fn decode_line(&self, line: &str) -> Result<Vec<DomainEvent>, Box<ExchangeError>> {
        super::adapters::hyperliquid::HYPERLIQUID.replay_frame(line)
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

fn object_absent(err: &EntrepotError) -> bool {
    match err {
        EntrepotError::Status { status, .. } => *status == 404,
        EntrepotError::Io { source, .. } => source.kind() == std::io::ErrorKind::NotFound,
        _ => false,
    }
}

fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

static ENTREPOT_HYPERLIQUID_PROFILE: LazyLock<ExchangeProfile> =
    LazyLock::new(|| ExchangeProfile {
        id: "hyperliquid",
        symbol_codec: SymbolCodec::BareCoin { quote: "USDC" },
        budget: ConnectionBudget::default(),
        schema_version: 1,
        protocol_revision: "hyperliquid-entrepot-v0",
    });

pub struct HyperliquidEntrepotAdapter {
    source: Arc<dyn ObjectSource>,
    keys: Vec<String>,
    decoder: Arc<dyn LineDecoder>,
}

impl HyperliquidEntrepotAdapter {
    pub fn new(source: Arc<dyn ObjectSource>, window: &EntrepotWindow) -> Self {
        Self {
            source,
            keys: hyperliquid_l2book_keys(window),
            decoder: Arc::new(HyperliquidEnvelopeLines),
        }
    }

    pub fn with_decoder(mut self, decoder: Arc<dyn LineDecoder>) -> Self {
        self.decoder = decoder;
        self
    }
}

pub fn build_entrepot_adapter(
    venue: &str,
    source: Arc<dyn ObjectSource>,
    window: &EntrepotWindow,
) -> Option<Box<dyn ExchangeAdapter>> {
    match venue {
        "hyperliquid" => Some(Box::new(HyperliquidEntrepotAdapter::new(source, window))),
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
        None
    }

    fn spawn(
        &self,
        _symbols: Vec<String>,
        _declared: DeclaredSet,
        tx: mpsc::Sender<DomainEvent>,
        shutdown: watch::Receiver<bool>,
        metrics: SourceMetrics,
    ) -> JoinHandle<TaskExit> {
        let source = Arc::clone(&self.source);
        let decoder = Arc::clone(&self.decoder);
        let keys = self.keys.clone();
        tokio::spawn(async move {
            let mut recv_seq: u64 = 0;
            for key in keys {
                if *shutdown.borrow() {
                    return TaskExit::Completed;
                }
                let bytes = match source.get(&key).await {
                    Ok(b) => b,
                    Err(e) if object_absent(&e) => {
                        tracing::debug!(key = key.as_str(), "entrepot.object_absent");
                        metrics.bump_gaps();
                        continue;
                    }
                    Err(e) => {
                        tracing::error!(key = key.as_str(), error = %e, "entrepot.fetch_failed");
                        return TaskExit::Failed(
                            crate::clients::disconnect::DisconnectReason::TransportError {
                                source: e.to_string().into(),
                            },
                        );
                    }
                };
                let lines = match codec::decode_lz4(&key, &bytes)
                    .and_then(|d| codec::utf8_lines(&key, &d))
                {
                    Ok(lines) => lines,
                    Err(e) => {
                        tracing::error!(key = key.as_str(), error = %e, "entrepot.decode_failed");
                        metrics.bump_decode_err();
                        continue;
                    }
                };
                for line in lines {
                    match decoder.decode_line(&line) {
                        Ok(events) => {
                            for mut event in events {
                                recv_seq += 1;
                                event.stamp_local(now_us(), 0, recv_seq);
                                metrics.bump_msgs();
                                if tx.send(event).await.is_err() {
                                    return TaskExit::Completed;
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(key = key.as_str(), error = %e, "entrepot.line_undecodable");
                            metrics.bump_decode_err();
                        }
                    }
                }
            }
            TaskExit::Exhausted
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window(coins: &[&str]) -> EntrepotWindow {
        EntrepotWindow {
            start: NaiveDate::from_ymd_opt(2026, 8, 5).unwrap(),
            end: NaiveDate::from_ymd_opt(2026, 8, 6).unwrap(),
            coins: coins.iter().map(|c| c.to_string()).collect(),
        }
    }

    #[test]
    fn key_grammar_matches_the_documented_layout() {
        let keys = hyperliquid_l2book_keys(&window(&["BTC"]));
        assert_eq!(keys.len(), 48);
        assert_eq!(keys[0], "market_data/20260805/0/l2Book/BTC.lz4");
        assert_eq!(keys[9], "market_data/20260805/9/l2Book/BTC.lz4");
        assert_eq!(keys[47], "market_data/20260806/23/l2Book/BTC.lz4");
        assert!(keys.iter().all(|k| !k.contains("/09/")));
    }

    fn fixture_lines(limit: usize) -> Vec<String> {
        let path = format!(
            "{}/datasets/hyperliquid/btc_book_trade.jsonl",
            env!("CARGO_MANIFEST_DIR")
        );
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .take(limit)
            .map(str::to_string)
            .collect()
    }

    fn stage_object(root: &std::path::Path, key: &str, lines: &[String]) {
        let path = root.join(key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let payload = lines.join("\n");
        std::fs::write(
            path,
            aetelier_entrepot::codec::encode_lz4(payload.as_bytes()),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn replays_a_staged_hour_and_exhausts() {
        let dir = tempfile::tempdir().unwrap();
        let lines = fixture_lines(200);
        stage_object(dir.path(), "market_data/20260805/12/l2Book/BTC.lz4", &lines);

        let source = Arc::new(aetelier_entrepot::LocalDirSource::new(dir.path()));
        let one_hour = EntrepotWindow {
            start: NaiveDate::from_ymd_opt(2026, 8, 5).unwrap(),
            end: NaiveDate::from_ymd_opt(2026, 8, 5).unwrap(),
            coins: vec!["BTC".to_string()],
        };
        let adapter = HyperliquidEntrepotAdapter::new(source, &one_hour);

        let (tx, mut rx) = mpsc::channel(4096);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let metrics = SourceMetrics::default();
        let handle = adapter.spawn(
            vec!["BTC".to_string()],
            DeclaredSet::all(),
            tx,
            shutdown_rx,
            metrics.clone(),
        );

        let mut events = Vec::new();
        while let Some(ev) = rx.recv().await {
            events.push(ev);
        }
        let exit = handle.await.unwrap();

        assert!(matches!(exit, TaskExit::Exhausted));
        assert!(!events.is_empty());
        let books = events
            .iter()
            .filter(|e| matches!(e, DomainEvent::Book(_)))
            .count();
        assert!(books > 0, "the fixture window carries l2Book frames");
        for ev in &events {
            if let DomainEvent::Book(d) = ev {
                assert!(d.local_orderbook_ts_us > 0, "local stamp applied");
                assert!(d.is_snapshot, "hyperliquid books are full refreshes");
            }
        }
        let m = metrics.snapshot();
        assert!(m.msgs > 0);
        assert_eq!(
            m.gaps, 23,
            "the 23 unstaged hours of the day count as gaps, never fail the run"
        );
    }

    #[tokio::test]
    async fn missing_hours_skip_and_corrupt_objects_count() {
        let dir = tempfile::tempdir().unwrap();
        let lines = fixture_lines(40);
        stage_object(dir.path(), "market_data/20260805/3/l2Book/BTC.lz4", &lines);
        let corrupt = dir.path().join("market_data/20260805/4/l2Book/BTC.lz4");
        std::fs::create_dir_all(corrupt.parent().unwrap()).unwrap();
        std::fs::write(&corrupt, b"not an lz4 frame").unwrap();

        let source = Arc::new(aetelier_entrepot::LocalDirSource::new(dir.path()));
        let one_day = EntrepotWindow {
            start: NaiveDate::from_ymd_opt(2026, 8, 5).unwrap(),
            end: NaiveDate::from_ymd_opt(2026, 8, 5).unwrap(),
            coins: vec!["BTC".to_string()],
        };
        let adapter = HyperliquidEntrepotAdapter::new(source, &one_day);

        let (tx, mut rx) = mpsc::channel(4096);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let metrics = SourceMetrics::default();
        let handle = adapter.spawn(
            vec!["BTC".to_string()],
            DeclaredSet::all(),
            tx,
            shutdown_rx,
            metrics.clone(),
        );
        let mut events = 0usize;
        while rx.recv().await.is_some() {
            events += 1;
        }
        let exit = handle.await.unwrap();

        assert!(matches!(exit, TaskExit::Exhausted));
        assert!(events > 0);
        let m = metrics.snapshot();
        assert_eq!(m.gaps, 22, "absent hours skip");
        assert_eq!(m.decode_err, 1, "the corrupt hour counts loudly");
    }

    #[tokio::test]
    async fn shutdown_interrupts_with_completed_not_exhausted() {
        let dir = tempfile::tempdir().unwrap();
        let source = Arc::new(aetelier_entrepot::LocalDirSource::new(dir.path()));
        let adapter = HyperliquidEntrepotAdapter::new(source, &window(&["BTC"]));

        let (tx, _rx) = mpsc::channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(true);
        let handle = adapter.spawn(
            vec!["BTC".to_string()],
            DeclaredSet::all(),
            tx,
            shutdown_rx,
            SourceMetrics::default(),
        );
        let exit = handle.await.unwrap();
        drop(shutdown_tx);
        assert!(matches!(exit, TaskExit::Completed));
    }

    #[test]
    fn factory_knows_hyperliquid_and_nothing_else() {
        let dir = tempfile::tempdir().unwrap();
        let source: Arc<dyn ObjectSource> =
            Arc::new(aetelier_entrepot::LocalDirSource::new(dir.path()));
        assert!(
            build_entrepot_adapter("hyperliquid", Arc::clone(&source), &window(&["BTC"]))
                .is_some()
        );
        assert!(build_entrepot_adapter("binance", source, &window(&["BTC"])).is_none());
    }
}
