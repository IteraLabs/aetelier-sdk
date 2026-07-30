//! HTX (Huobi) spot public market-data adapter: subscribes to the incremental
//! MBP depth channel (`market.<sym>.mbp.150`) and the trade-detail channel,
//! and normalizes them into the framework's `DomainEvent` stream.
//!
//! Transport specifics:
//! - Inbound frames are gzip binary; the transport inflates before this code
//!   sees text ([`FrameCodec::Gzip`]).
//! - Keep-alive is server-driven: HTX sends `{"ping":<ts>}` and the client
//!   echoes `{"pong":<ts>}` from `on_inbound_control`; there is no client
//!   heartbeat timer (`heartbeat()` is `None`).
//! - The order book is seeded by an in-band REQ
//!   (`{"req":"market.<sym>.mbp.150",…}`) appended after the subscribe frames,
//!   so `HtxHooks` holds the symbol set and `prepare` builds one REQ per symbol.
//! - MBP deltas chain by `prevSeqNum`; a gap re-issues the in-band REQ.

use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::protocol::Message;

use aetelier_types::orderbooks::{NormalizedDelta, f64_to_decimal};
use aetelier_types::trades::{Trade, TradeSide};

use crate::errors::ExchangeError;
use crate::framework::budget::{ConnectionBudget, SourceMetrics};
use crate::framework::driver::{DEFAULT_RAW_BUFFER, drive};
use crate::framework::model::{
    DomainEvent, Normalizer, ReconstructionModel, SeqPredicate, SnapshotSource,
    epoch_to_us,
};
use crate::framework::protocol::{
    ControlAction, FrameCodec, Heartbeat, Prepared, ProtocolHooks,
};
use crate::framework::registry::{ExchangeAdapter, ExchangeProfile, TaskExit};
use crate::framework::symbol::SymbolCodec;
use crate::sources::htx::decoder::HtxDecoder;
use crate::sources::htx::events::HtxWssEvent;
use crate::sources::htx::responses::orderbooks::{HtxLevel, HtxMbpTick};

const HTX_WSS_URL: &str = "wss://api.huobi.pro/ws";

/// Market-by-price depth: 150 levels, incremental (carries seqNum/prevSeqNum).
const MBP_LEVELS: u32 = 150;

/// Wire symbol codec — lowercase concat, e.g. `btcusdt`.
const HTX_CODEC: SymbolCodec = SymbolCodec::Concat { upper: false };

/// MBP channel name for a wire symbol, e.g. `market.btcusdt.mbp.150`.
fn mbp_channel(symbol: &str) -> String {
    format!("market.{symbol}.mbp.{MBP_LEVELS}")
}

/// Trade-detail channel name, e.g. `market.btcusdt.trade.detail`.
fn trade_channel(symbol: &str) -> String {
    format!("market.{symbol}.trade.detail")
}

/// `market.btcusdt.mbp.150` → `btcusdt`.
fn symbol_from_channel(ch: &str) -> Option<&str> {
    ch.split('.').nth(1)
}

// ─────────────────────────────────────────────────────────────────────────
// ProtocolHooks
// ─────────────────────────────────────────────────────────────────────────

pub struct HtxHooks {
    symbols: Vec<String>,
}

impl HtxHooks {
    /// Construct hooks over `symbols` (the in-band REQ seed is built per
    /// symbol in `prepare`). Public so capture tooling can drive the same
    /// protocol the runtime uses.
    pub fn new(symbols: Vec<String>) -> Self {
        Self { symbols }
    }
}

#[async_trait::async_trait]
impl ProtocolHooks for HtxHooks {
    fn endpoint(&self) -> String {
        HTX_WSS_URL.to_string()
    }

    /// One `sub` frame per `(symbol × {mbp, trade})` topic.
    fn subscribe_frames(&self, symbols: &[String]) -> Vec<Message> {
        let mut frames = Vec::with_capacity(symbols.len() * 2);
        for s in symbols {
            frames.push(Message::Text(
                serde_json::json!({ "sub": mbp_channel(s), "id": format!("mbp-{s}") })
                    .to_string()
                    .into(),
            ));
            frames.push(Message::Text(
                serde_json::json!({ "sub": trade_channel(s), "id": format!("trd-{s}") })
                    .to_string()
                    .into(),
            ));
        }
        frames
    }

    fn frame_codec(&self) -> FrameCodec {
        FrameCodec::Gzip
    }

    /// Server-driven keep-alive: echo `{"ping":N}` as `{"pong":N}`.
    fn on_inbound_control(&self, text: &str) -> ControlAction {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(text)
            && let Some(ping) = v.get("ping")
        {
            let pong = serde_json::json!({ "pong": ping }).to_string();
            return ControlAction::Reply(Message::Text(pong.into()));
        }
        ControlAction::Ignore
    }

    fn heartbeat(&self) -> Heartbeat {
        Heartbeat::None // server pings; we only reply (on_inbound_control)
    }

    /// In-band seed: one REQ per symbol, appended after the subscribe frames.
    async fn prepare(&self) -> Result<Prepared, ExchangeError> {
        let extra_frames = self
            .symbols
            .iter()
            .map(|s| {
                Message::Text(
                    serde_json::json!({ "req": mbp_channel(s), "id": format!("req-{s}") })
                        .to_string()
                        .into(),
                )
            })
            .collect();
        Ok(Prepared {
            extra_frames,
            ..Default::default()
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Normalizer
// ─────────────────────────────────────────────────────────────────────────

/// Maps a decoded `HtxWssEvent` to `DomainEvent`s. Holds the worker's shared
/// [`SourceMetrics`] so every dropped event is counted, not just logged.
#[derive(Default)]
pub struct HtxNormalizer {
    pub metrics: SourceMetrics,
}

impl HtxNormalizer {
    fn to_delta(
        symbol: &str,
        tick: &HtxMbpTick,
        ts: u64,
        is_snapshot: bool,
    ) -> NormalizedDelta {
        let levels = |side: &[HtxLevel]| {
            side.iter()
                .map(|l| (l.0.to_string(), l.1.to_string()))
                .collect::<Vec<_>>()
        };
        NormalizedDelta {
            symbol: symbol.to_string(),
            bids: levels(&tick.bids),
            asks: levels(&tick.asks),
            update_id: tick.seq_num,
            // prev-id pointer drives ExactPrev continuity (0 on the seed).
            sequence: tick.prev_seq_num.unwrap_or(0),
            source_orderbook_ts_us: epoch_to_us(ts),
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            checksum: None,
            orders: Vec::new(),
            is_snapshot,
        }
    }
}

impl Normalizer for HtxNormalizer {
    type Event = HtxWssEvent;

    fn normalize(&self, event: HtxWssEvent) -> Vec<DomainEvent> {
        match event {
            HtxWssEvent::MbpSnapshot(s) => match symbol_from_channel(&s.rep) {
                Some(sym) => {
                    vec![DomainEvent::Book(Self::to_delta(sym, &s.data, s.ts, true))]
                }
                None => Vec::new(),
            },
            HtxWssEvent::MbpUpdate(u) => match symbol_from_channel(&u.ch) {
                Some(sym) => {
                    vec![DomainEvent::Book(Self::to_delta(sym, &u.tick, u.ts, false))]
                }
                None => Vec::new(),
            },
            HtxWssEvent::Trade(frame) => {
                let Some(sym) = symbol_from_channel(&frame.ch) else {
                    tracing::warn!(channel = %frame.ch, "htx.trade.bad_symbol");
                    self.metrics.add_dropped_frames(1);
                    return Vec::new();
                };
                let Some(pair) = HTX_CODEC.decode(sym) else {
                    tracing::warn!(symbol = %sym, "htx.trade.bad_symbol");
                    self.metrics.add_dropped_frames(1);
                    return Vec::new();
                };
                // The tick batches trades in DESCENDING tradeId order (newest
                // first — verified on live btcusdt captures, cycle #7):
                // reverse so the emitted stream is ascending, and arm the
                // sequence — tradeId is a per-symbol counter, verified
                // strictly monotonic AND dense (+1) across the capture, so
                // `trades_lost = seq - last - 1` counts real losses.
                frame
                    .tick
                    .data
                    .into_iter()
                    .rev()
                    .filter_map(|d| {
                        // Drop (never fabricate as Buy) an unknown taker side so
                        // it can't pollute the side distribution.
                        let Some(side) = TradeSide::from_str_loose(&d.direction) else {
                            tracing::warn!(
                                side = %d.direction,
                                id = d.trade_id,
                                "htx.trade.unknown_side"
                            );
                            self.metrics.add_dropped_frames(1);
                            return None;
                        };
                        Some(DomainEvent::Trade {
                            trade: Trade {
                                source_trade_ts_us: epoch_to_us(d.ts),
                                local_trade_ts_us: 0,
                                source_trade_rtt_us: 0,
                                pair: pair.clone(),
                                side,
                                amount: f64_to_decimal(d.amount),
                                price: f64_to_decimal(d.price),
                                exchange: "htx".to_string(),
                                id: d.trade_id.to_string(),
                                origin: Default::default(),
                            },
                            sequence: Some(d.trade_id),
                        })
                    })
                    .collect()
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// ExchangeAdapter
// ─────────────────────────────────────────────────────────────────────────

static HTX_PROFILE: ExchangeProfile = ExchangeProfile {
    id: "htx",
    symbol_codec: HTX_CODEC,
    budget: ConnectionBudget {
        // HTX public WSS caps a single connection at 100 topic subscriptions;
        // the concurrent-connection count is undocumented (uncapped).
        max_connections: None,
        max_streams_per_socket: Some(100),
        subscribe_rate: Vec::new(),
        connect_attempt_rate: None,
        connection_lifetime: None,
    },
    schema_version: 1,
    protocol_revision: "htx-v2",
};

pub struct HtxAdapter;

/// The single compiled-in HTX instance (referenced by `register_all`).
pub static HTX: HtxAdapter = HtxAdapter;

impl ExchangeAdapter for HtxAdapter {
    fn id(&self) -> &'static str {
        "htx"
    }

    fn profile(&self) -> &ExchangeProfile {
        &HTX_PROFILE
    }

    fn book_model(&self, _channel: &str) -> ReconstructionModel {
        // MBP deltas chain by prevSeqNum; seeded + recovered by in-band REQ.
        ReconstructionModel::SeqDelta {
            predicate: SeqPredicate::ExactPrev,
            source: SnapshotSource::ReqOnSocket,
        }
    }

    fn spawn(
        &self,
        symbols: Vec<String>,
        tx: mpsc::Sender<DomainEvent>,
        shutdown: watch::Receiver<bool>,
        metrics: SourceMetrics,
    ) -> JoinHandle<TaskExit> {
        let hooks = Arc::new(HtxHooks {
            symbols: symbols.clone(),
        });
        tokio::spawn(drive::<HtxHooks, HtxDecoder, HtxNormalizer>(
            hooks,
            symbols,
            HtxNormalizer {
                metrics: metrics.clone(),
            },
            tx,
            shutdown,
            DEFAULT_RAW_BUFFER,
            metrics,
        ))
    }

    fn replay_frame(
        &self,
        raw: &str,
    ) -> Result<Vec<DomainEvent>, Box<crate::errors::ExchangeError>> {
        use crate::clients::wss::WssDecoder;
        let normalizer = HtxNormalizer {
            metrics: SourceMetrics::default(),
        };
        match HtxDecoder::decode(raw)? {
            Some(event) => Ok(normalizer.normalize(event)),
            None => Ok(Vec::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::wss::WssDecoder;
    use crate::sources::htx::responses::orderbooks::HtxMbpUpdate;

    #[test]
    fn normalizes_mbp_update_with_prev_pointer() {
        let u = HtxMbpUpdate {
            ch: "market.btcusdt.mbp.150".into(),
            ts: 1_700_000_000_000,
            tick: HtxMbpTick {
                seq_num: 101,
                prev_seq_num: Some(100),
                bids: vec![HtxLevel(100.0, 1.5)],
                asks: vec![HtxLevel(101.0, 2.0)],
            },
        };
        let evs = HtxNormalizer::default().normalize(HtxWssEvent::MbpUpdate(u));
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            DomainEvent::Book(nd) => {
                assert_eq!(nd.symbol, "btcusdt");
                assert_eq!(nd.source_orderbook_ts_us, 1_700_000_000_000_000);
                assert_eq!(nd.update_id, 101);
                assert_eq!(nd.sequence, 100); // prevSeqNum → ExactPrev pointer
                assert!(!nd.is_snapshot);
            }
            other => panic!("expected Book, got {other:?}"),
        }
    }

    #[test]
    fn on_inbound_control_echoes_ping_as_pong() {
        let hooks = HtxHooks { symbols: vec![] };
        match hooks.on_inbound_control(r#"{"ping":1700000000000}"#) {
            ControlAction::Reply(Message::Text(body)) => {
                let v: serde_json::Value = serde_json::from_str(&body).unwrap();
                assert_eq!(v["pong"], 1_700_000_000_000_u64);
            }
            _ => panic!("ping must be answered with a pong reply"),
        }
        // A data frame is not control.
        assert!(matches!(
            hooks.on_inbound_control(r#"{"ch":"market.btcusdt.mbp.150"}"#),
            ControlAction::Ignore
        ));
    }

    #[tokio::test]
    async fn prepare_emits_one_req_seed_per_symbol() {
        let hooks = HtxHooks {
            symbols: vec!["btcusdt".into(), "ethusdt".into()],
        };
        let prepared = hooks.prepare().await.unwrap();
        assert_eq!(prepared.extra_frames.len(), 2);
        let Message::Text(first) = &prepared.extra_frames[0] else {
            panic!("expected text REQ frame");
        };
        let v: serde_json::Value = serde_json::from_str(first).unwrap();
        assert_eq!(v["req"], "market.btcusdt.mbp.150");
    }

    #[test]
    fn frame_codec_is_gzip() {
        assert_eq!(HtxHooks { symbols: vec![] }.frame_codec(), FrameCodec::Gzip);
    }

    #[test]
    fn decoder_routes_snapshot_update_and_trade() {
        let snap = r#"{"rep":"market.btcusdt.mbp.150","data":{"seqNum":100,"bids":[],"asks":[]}}"#;
        assert!(matches!(
            HtxDecoder::decode(snap).unwrap(),
            Some(HtxWssEvent::MbpSnapshot(_))
        ));
        let upd = r#"{"ch":"market.btcusdt.mbp.150","tick":{"seqNum":101,"prevSeqNum":100,"bids":[],"asks":[]}}"#;
        assert!(matches!(
            HtxDecoder::decode(upd).unwrap(),
            Some(HtxWssEvent::MbpUpdate(_))
        ));
        let trd = r#"{"ch":"market.btcusdt.trade.detail","tick":{"data":[]}}"#;
        assert!(matches!(
            HtxDecoder::decode(trd).unwrap(),
            Some(HtxWssEvent::Trade(_))
        ));
        // Subscribe ack → no event.
        assert!(
            HtxDecoder::decode(r#"{"status":"ok","subbed":"x","id":"y"}"#)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn trade_batch_reverses_descending_tick_and_arms_sequences() {
        // A live tick batches trades newest-first ([994, 993] on the wire —
        // cycle-#7 capture); the normalizer must emit ascending with the
        // tradeId armed as the sequence (verified dense on live data, the
        // premise of `trades_lost` accounting).
        let raw = r#"{"ch":"market.btcusdt.trade.detail","tick":{"data":[
            {"ts":1784133636000,"tradeId":994,"price":64000.0,"amount":0.1,"direction":"buy"},
            {"ts":1784133635000,"tradeId":993,"price":63999.0,"amount":0.2,"direction":"sell"}
        ]}}"#;
        let Some(ev) = HtxDecoder::decode(raw).unwrap() else {
            panic!("trade frame decodes");
        };
        let evs = HtxNormalizer {
            metrics: SourceMetrics::default(),
        }
        .normalize(ev);
        let seqs: Vec<Option<u64>> = evs
            .iter()
            .map(|e| match e {
                DomainEvent::Trade { sequence, .. } => *sequence,
                other => panic!("expected Trade, got {other:?}"),
            })
            .collect();
        assert_eq!(seqs, [Some(993), Some(994)], "ascending + armed");
    }

    /// The density guarantee behind the armed sequence, frozen against REAL
    /// data: replaying every `trade.detail` frame of a live 600s btcusdt
    /// window must yield strictly ascending, DENSE (+1) sequences. Dense is
    /// the load-bearing property — `trades_lost = seq - last - 1` fabricates
    /// losses if ids can legitimately skip.
    #[test]
    fn real_capture_trade_sequences_are_dense_after_batch_reverse() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/datasets/htx/btcusdt_trades_density.jsonl"
        );
        let raw = std::fs::read_to_string(path).expect("fixture present");
        let adapter = HtxAdapter;
        let mut seqs: Vec<u64> = Vec::new();
        for line in raw.lines().filter(|l| !l.is_empty()) {
            for ev in adapter.replay_frame(line).expect("frame decodes") {
                if let DomainEvent::Trade { sequence, .. } = ev {
                    seqs.push(sequence.expect("htx trades are armed"));
                }
            }
        }
        assert!(seqs.len() >= 40, "the window carries a real trade sample");
        for w in seqs.windows(2) {
            assert_eq!(
                w[1],
                w[0] + 1,
                "tradeId must be dense (+1) — the premise of loss accounting"
            );
        }
    }

    #[test]
    fn profile_declares_exact_prev_over_req_on_socket() {
        let a = HtxAdapter;
        assert_eq!(a.id(), "htx");
        assert!(matches!(
            a.book_model("orders"),
            ReconstructionModel::SeqDelta {
                predicate: SeqPredicate::ExactPrev,
                source: SnapshotSource::ReqOnSocket,
            }
        ));
    }
}
