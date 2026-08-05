//! Bitso public market-data adapter: subscribes to `diff-orders` + `trades`
//! streams via `{action,book,type}` framing and normalizes them into the
//! framework's `DomainEvent` stream.
//!
//! Order books use [`ReconstructionModel::L3`]: `diff-orders` streams per-order
//! changes keyed by order id (`o`), seeded by a REST L3 snapshot. The normalizer
//! projects each per-order change to a price-level `DomainEvent::Book` (a removed
//! order → size `0`). Codec is lowercase `Underscore` (`btc_mxn`).

use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::protocol::Message;

use aetelier_types::orderbooks::{L3Order, NormalizedDelta};
use aetelier_types::trades::{Trade, TradeSide};

use crate::framework::budget::{ConnectionBudget, SourceMetrics};
use crate::framework::driver::{DEFAULT_RAW_BUFFER, drive};
use crate::framework::model::{
    DomainEvent, Normalizer, ReconstructionModel, SnapshotSource, epoch_to_us,
};
use crate::framework::protocol::DeclaredSet;
use crate::framework::protocol::{Heartbeat, ProtocolHooks};
use crate::framework::registry::{ExchangeAdapter, ExchangeProfile, TaskExit};
use crate::framework::symbol::SymbolCodec;
use crate::sources::bitso::decoder::BitsoDecoder;
use crate::sources::bitso::events::BitsoWssEvent;
use crate::sources::bitso::rest::parse_snapshot;

const BITSO_WSS_URL: &str = "wss://ws.bitso.com";

/// Wire symbol codec — lowercase `btc_mxn`.
const BITSO_CODEC: SymbolCodec = SymbolCodec::Underscore { upper: false };

pub struct BitsoHooks;

impl ProtocolHooks for BitsoHooks {
    fn endpoint(&self) -> String {
        BITSO_WSS_URL.to_string()
    }

    /// One `{action,book,type}` object per `(book × {diff-orders, trades})`.
    fn subscribe_frames(
        &self,
        symbols: &[String],
        _declared: &DeclaredSet,
    ) -> Vec<Message> {
        let mut frames = Vec::with_capacity(symbols.len() * 2);
        for s in symbols {
            frames.push(Message::Text(
                serde_json::json!({ "action": "subscribe", "book": s, "type": "diff-orders" })
                    .to_string()
                    .into(),
            ));
            frames.push(Message::Text(
                serde_json::json!({ "action": "subscribe", "book": s, "type": "trades" })
                    .to_string()
                    .into(),
            ));
        }
        frames
    }

    fn heartbeat(&self) -> Heartbeat {
        Heartbeat::None
    }
}

/// Maps a decoded `BitsoWssEvent` to `DomainEvent`s. Holds the worker's
/// shared [`SourceMetrics`] so every dropped event is counted, not just
/// logged.
///
/// Also runs the per-book `diff-orders` sequence sentinel: the envelope
/// `sequence` is dense per book (+1 per message), so a hole is a PROVEN
/// dropped message on this socket — unlike Coinbase's connection-wide counter
/// there is nothing to attribute a hole to (no heartbeat channel shares the
/// numbering), so confirmation is immediate, no debounce. The trades channel
/// carries no envelope sequence (verified live), so a diff-orders hole is
/// also the trade-loss suspicion signal for this socket.
#[derive(Default)]
pub struct BitsoNormalizer {
    pub metrics: SourceMetrics,
    /// Last seen envelope sequence per book. Per-connection state (a fresh
    /// normalizer is built per spawn), so the first frame of a connection
    /// seeds the counter without a check. `Mutex` because `normalize` takes
    /// `&self`; the driver is the sole caller, so it is uncontended.
    last_seq: std::sync::Mutex<std::collections::HashMap<String, u64>>,
}

impl BitsoNormalizer {
    /// Observe one diff-orders envelope sequence for `book`. Returns the
    /// number of provably-dropped messages when the dense counter skips.
    fn observe_seq(&self, book: &str, seq: u64) -> Option<u64> {
        let mut map = self.last_seq.lock().ok()?;
        let dropped = match map.get(book) {
            Some(prev) => {
                let jump = seq.saturating_sub(prev.saturating_add(1));
                (jump > 0).then_some(jump)
            }
            None => None,
        };
        map.insert(book.to_string(), seq);
        dropped
    }
}

impl Normalizer for BitsoNormalizer {
    type Event = BitsoWssEvent;

    fn normalize(&self, event: BitsoWssEvent) -> Vec<DomainEvent> {
        match event {
            BitsoWssEvent::DiffOrders(m) => {
                let mut evs = Vec::with_capacity(2);

                if let Some(seq) = m.sequence
                    && let Some(dropped) = self.observe_seq(&m.book, seq)
                {
                    tracing::warn!(
                        book = %m.book,
                        dropped,
                        "bitso.diff_sequence_gap_resyncing"
                    );
                    evs.push(DomainEvent::ConnectionGap { dropped });
                }

                let mut orders = Vec::with_capacity(m.payload.len());
                let mut update_id = 0u64;
                for o in &m.payload {
                    update_id = update_id.max(o.d);
                    let removed = matches!(o.s.as_str(), "cancelled" | "completed");
                    if !removed
                        && !matches!(
                            o.s.as_str(),
                            "open" | "partially filled" | "partial-fill" | "queued"
                        )
                    {
                        tracing::warn!(
                            status = %o.s,
                            order_id = %o.o,
                            "bitso: unmapped diff-order status treated as open"
                        );
                    }
                    orders.push(L3Order {
                        order_id: o.o.clone(),
                        is_ask: o.t != 0,
                        price: o.r.clone().unwrap_or_default(),
                        size: o.a.clone().unwrap_or_default(),
                        removed,
                    });
                }
                evs.push(DomainEvent::Book(NormalizedDelta {
                    symbol: m.book,
                    bids: Vec::new(),
                    asks: Vec::new(),
                    update_id,
                    sequence: m.sequence.unwrap_or(0),

                    source_orderbook_ts_us: epoch_to_us(update_id),
                    local_orderbook_ts_us: 0,
                    source_orderbook_rtt_us: 0,
                    checksum: None,
                    orders,
                    is_snapshot: false,
                }));
                evs
            }
            BitsoWssEvent::Trades(m) => {
                let Some(pair) = BITSO_CODEC.decode(&m.book) else {
                    tracing::warn!(symbol = %m.book, "bitso.trade.bad_symbol");
                    self.metrics.add_dropped_frames(1);
                    return Vec::new();
                };
                m.payload
                    .into_iter()
                    .filter_map(|t| {
                        let (Ok(amount), Ok(price)) = (
                            t.a.parse::<rust_decimal::Decimal>(),
                            t.r.parse::<rust_decimal::Decimal>(),
                        ) else {
                            self.metrics.add_dropped_frames(1);
                            return None;
                        };
                        let side = match t.t {
                            0 => TradeSide::Buy,
                            1 => TradeSide::Sell,
                            other => {
                                tracing::warn!(t = other, "bitso.trade.unknown_side");
                                self.metrics.add_dropped_frames(1);
                                return None;
                            }
                        };
                        Some(DomainEvent::Trade {
                            trade: Trade {
                                source_trade_ts_us: epoch_to_us(t.x),
                                local_trade_ts_us: 0,
                                source_trade_rtt_us: 0,
                                pair: pair.clone(),
                                side,
                                amount,
                                price,
                                exchange: "bitso".to_string(),
                                id: t.i.to_string(),
                                origin: Default::default(),
                            },

                            sequence: None,
                        })
                    })
                    .collect()
            }
        }
    }
}

static BITSO_PROFILE: ExchangeProfile = ExchangeProfile {
    id: "bitso",
    symbol_codec: BITSO_CODEC,
    budget: ConnectionBudget {
        max_connections: None,
        max_streams_per_socket: None,
        subscribe_rate: Vec::new(),
        connect_attempt_rate: None,
        connection_lifetime: None,
    },
    schema_version: 1,
    protocol_revision: "bitso-v3",
};

pub struct BitsoAdapter;

/// The single compiled-in Bitso instance (referenced by `register_all`).
pub static BITSO: BitsoAdapter = BitsoAdapter;

impl ExchangeAdapter for BitsoAdapter {
    fn id(&self) -> &'static str {
        "bitso"
    }

    fn profile(&self) -> &ExchangeProfile {
        &BITSO_PROFILE
    }

    fn rest_seeder(
        &self,
    ) -> Option<std::sync::Arc<dyn crate::framework::rest::RestSnapshot>> {
        Some(std::sync::Arc::new(BitsoRestSnapshot::new()))
    }

    fn book_model(&self, _channel: &str) -> ReconstructionModel {
        ReconstructionModel::L3 {
            source: SnapshotSource::RestSnapshot,
        }
    }

    fn spawn(
        &self,
        symbols: Vec<String>,
        declared: DeclaredSet,
        tx: mpsc::Sender<DomainEvent>,
        shutdown: watch::Receiver<bool>,
        metrics: SourceMetrics,
    ) -> JoinHandle<TaskExit> {
        tokio::spawn(drive::<BitsoHooks, BitsoDecoder, BitsoNormalizer>(
            Arc::new(BitsoHooks),
            symbols,
            declared,
            BitsoNormalizer {
                metrics: metrics.clone(),
                ..Default::default()
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
        let normalizer = BitsoNormalizer {
            metrics: SourceMetrics::default(),
            ..Default::default()
        };
        match BitsoDecoder::decode(raw)? {
            Some(event) => Ok(normalizer.normalize(event)),
            None => Ok(Vec::new()),
        }
    }

    fn replay_seed(
        &self,
        raw: &str,
        wire_symbol: &str,
    ) -> Result<
        Option<aetelier_types::orderbooks::NormalizedDelta>,
        Box<crate::errors::ExchangeError>,
    > {
        parse_snapshot(raw, wire_symbol).map(Some).map_err(Box::new)
    }
}

pub use crate::sources::bitso::rest::BitsoRestSnapshot;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::wss::WssDecoder;
    use crate::sources::bitso::responses::{
        BitsoDiffMessage, BitsoDiffOrder, BitsoTrade, BitsoTradeMessage,
    };

    #[test]
    fn diff_orders_project_to_l3_orders_with_removal() {
        let m = BitsoDiffMessage {
            book: "btc_mxn".into(),
            sequence: None,
            payload: vec![
                BitsoDiffOrder {
                    d: 100,
                    r: Some("1000000".into()),
                    a: Some("0.5".into()),
                    t: 0,
                    o: "o1".into(),
                    s: "open".into(),
                },
                BitsoDiffOrder {
                    d: 101,
                    r: None,
                    a: None,
                    t: 1,
                    o: "o2".into(),
                    s: "cancelled".into(),
                },
            ],
        };
        let evs = BitsoNormalizer::default().normalize(BitsoWssEvent::DiffOrders(m));
        match &evs[0] {
            DomainEvent::Book(nd) => {
                assert_eq!(nd.symbol, "btc_mxn");
                assert_eq!(nd.update_id, 101);
                assert!(nd.bids.is_empty() && nd.asks.is_empty());
                assert_eq!(nd.orders.len(), 2);

                assert_eq!(nd.orders[0].order_id, "o1");
                assert!(!nd.orders[0].is_ask);
                assert_eq!(nd.orders[0].price, "1000000");
                assert_eq!(nd.orders[0].size, "0.5");
                assert!(!nd.orders[0].removed);

                assert_eq!(nd.orders[1].order_id, "o2");
                assert!(nd.orders[1].is_ask);
                assert!(nd.orders[1].removed);
            }
            other => panic!("expected Book, got {other:?}"),
        }
    }

    #[test]
    fn normalizes_trade_taker_side() {
        let m = BitsoTradeMessage {
            book: "btc_mxn".into(),
            payload: vec![BitsoTrade {
                i: 42,
                a: "0.1".into(),
                r: "1000000".into(),
                t: 1,
                x: 1_700_000_000_000,
            }],
        };
        let evs = BitsoNormalizer::default().normalize(BitsoWssEvent::Trades(m));
        match &evs[0] {
            DomainEvent::Trade { trade, .. } => {
                assert_eq!(trade.exchange, "bitso");
                assert_eq!(trade.id, "42");
                assert_eq!(trade.side, TradeSide::Sell);
                assert_eq!(trade.pair.base(), "BTC");
                assert_eq!(trade.pair.quote(), "MXN");

                assert_eq!(trade.source_trade_ts_us, 1_700_000_000_000_000);
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn subscribe_frame_uses_action_book_type() {
        let frames =
            BitsoHooks.subscribe_frames(&["btc_mxn".to_string()], &DeclaredSet::all());
        assert_eq!(frames.len(), 2);
        let Message::Text(body) = &frames[0] else {
            panic!("expected a text frame");
        };
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(v["action"], "subscribe");
        assert_eq!(v["book"], "btc_mxn");
        assert_eq!(v["type"], "diff-orders");
    }

    #[test]
    fn decoder_skips_subscription_ack_without_payload() {
        let ack = r#"{"action":"subscribe","response":"ok","type":"diff-orders","book":"btc_mxn"}"#;
        assert!(BitsoDecoder::decode(ack).unwrap().is_none());

        assert!(BitsoDecoder::decode(r#"{"type":"ka"}"#).unwrap().is_none());
    }

    #[test]
    fn profile_declares_l3() {
        let a = BitsoAdapter;
        assert_eq!(a.id(), "bitso");
        assert!(matches!(
            a.profile().symbol_codec,
            SymbolCodec::Underscore { upper: false }
        ));
        assert!(matches!(
            a.book_model("orders"),
            ReconstructionModel::L3 {
                source: SnapshotSource::RestSnapshot
            }
        ));
    }

    fn diff_frame(book: &str, seq: u64) -> String {
        format!(
            r#"{{"type":"diff-orders","book":"{book}","sequence":{seq},"payload":[{{"d":100,"r":"1000000","a":"0.1","t":0,"o":"oid-{seq}","s":"open"}}]}}"#
        )
    }

    fn events_for(n: &BitsoNormalizer, raw: &str) -> Vec<DomainEvent> {
        match BitsoDecoder::decode(raw).unwrap() {
            Some(ev) => n.normalize(ev),
            None => Vec::new(),
        }
    }

    #[test]
    fn sequence_sentinel_silent_on_contiguous_and_fires_exactly_on_a_hole() {
        let n = BitsoNormalizer::default();

        for seq in 10..15u64 {
            let evs = events_for(&n, &diff_frame("btc_mxn", seq));
            assert_eq!(evs.len(), 1, "book delta only at seq {seq}");
            assert!(matches!(evs[0], DomainEvent::Book(_)));
        }

        let evs = events_for(&n, &diff_frame("btc_mxn", 17));
        assert_eq!(evs.len(), 2);
        match &evs[0] {
            DomainEvent::ConnectionGap { dropped } => assert_eq!(*dropped, 2),
            other => panic!("expected ConnectionGap first, got {other:?}"),
        }
        assert!(matches!(evs[1], DomainEvent::Book(_)));

        let evs = events_for(&n, &diff_frame("btc_mxn", 18));
        assert_eq!(evs.len(), 1);
    }

    #[test]
    fn sequence_sentinel_tracks_books_independently() {
        let n = BitsoNormalizer::default();
        assert_eq!(events_for(&n, &diff_frame("btc_mxn", 100)).len(), 1);
        assert_eq!(events_for(&n, &diff_frame("eth_mxn", 500)).len(), 1);

        let evs = events_for(&n, &diff_frame("btc_mxn", 102));
        assert!(matches!(evs[0], DomainEvent::ConnectionGap { dropped: 1 }));
        assert_eq!(events_for(&n, &diff_frame("eth_mxn", 501)).len(), 1);
    }

    /// Stateful replay of the REAL committed capture (live btc_mxn, cycle #5;
    /// 899 diff-orders frames with contiguous envelope sequences): a clean
    /// capture must produce ZERO ConnectionGap events — the sentinel's
    /// false-positive guard. Dropping one mid-stream diff frame from the same
    /// capture must produce EXACTLY ONE, with dropped == 1.
    #[test]
    fn real_capture_replays_clean_and_detects_an_injected_drop() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/datasets/bitso/btcmxn_book_trade.jsonl"
        );
        let raw = std::fs::read_to_string(path).expect("fixture present");
        let frames: Vec<&str> = raw.lines().filter(|l| !l.is_empty()).collect();
        assert!(frames.len() > 500, "fixture should carry a real window");

        let count_gaps = |skip: Option<usize>| -> u64 {
            let n = BitsoNormalizer::default();
            let mut gaps = 0u64;
            for (i, f) in frames.iter().enumerate() {
                if Some(i) == skip {
                    continue;
                }
                for ev in events_for(&n, f) {
                    if let DomainEvent::ConnectionGap { dropped } = ev {
                        gaps += dropped;
                    }
                }
            }
            gaps
        };

        assert_eq!(
            count_gaps(None),
            0,
            "a clean live capture must never confirm a gap"
        );

        let mid_diff = frames
            .iter()
            .enumerate()
            .filter(|(_, f)| f.contains(r#""type":"diff-orders""#))
            .map(|(i, _)| i)
            .nth(400)
            .expect("a mid-stream diff frame");
        assert_eq!(
            count_gaps(Some(mid_diff)),
            1,
            "exactly one one-message gap confirmed"
        );
    }
}
