//! Kraken v2 spot public market-data adapter: subscribes to the `book` and
//! `trade` channels and normalizes them into the framework's `DomainEvent`
//! stream. Kraken pushes its own server heartbeats (`Heartbeat::None`), and the
//! incremental `book` channel uses `ChecksumDelta`/`KrakenTop10` reconstruction.

use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::protocol::Message;

use aetelier_types::orderbooks::{NormalizedDelta, f64_to_decimal};
use aetelier_types::trades::{Trade, TradeSide};

use crate::framework::budget::{ConnectionBudget, SourceMetrics};
use crate::framework::driver::{DEFAULT_RAW_BUFFER, drive};
use crate::framework::model::{
    ChecksumFmt, DomainEvent, Normalizer, ReconstructionModel,
};
use crate::framework::protocol::DeclaredSet;
use crate::framework::protocol::{Heartbeat, ProtocolHooks};
use crate::framework::registry::{ExchangeAdapter, ExchangeProfile, TaskExit};
use crate::framework::symbol::SymbolCodec;
use crate::sources::kraken::decoder::KrakenDecoder;
use crate::sources::kraken::events::KrakenWssEvent;
use crate::sources::kraken::responses::orderbooks::KrakenBookResponse;
use crate::sources::kraken::responses::trades::KrakenTradeData;

/// Kraken v2 public spot endpoint (no auth for `book` / `trade`).
const KRAKEN_WSS_URL: &str = "wss://ws.kraken.com/v2";

/// Wire symbol codec — `BTC/USD`.
const KRAKEN_CODEC: SymbolCodec = SymbolCodec::Slash;

// ─────────────────────────────────────────────────────────────────────────
// ProtocolHooks
// ─────────────────────────────────────────────────────────────────────────

/// Kraken v2 WSS protocol behaviour.
pub struct KrakenHooks;

impl ProtocolHooks for KrakenHooks {
    fn endpoint(&self) -> String {
        KRAKEN_WSS_URL.to_string()
    }

    /// Two subscribe frames — one per channel — each carrying the full symbol
    /// array under `params`. Kraken v2 keys the subscription by `channel` +
    /// `symbol: [...]` (a single frame can fan out over many symbols, but each
    /// channel is its own frame). `symbols` are venue wire symbols (`"BTC/USD"`,
    /// Slash codec).
    fn subscribe_frames(
        &self,
        symbols: &[String],
        _declared: &DeclaredSet,
    ) -> Vec<Message> {
        let book = serde_json::json!({
            // `depth: 10` pins Kraken's returned book to the top-10 window that
            // `ReconstructionModel::ChecksumDelta { KrakenTop10 }` validates via
            // CRC32 — without it Kraken defaults to a wider book and the checksum
            // never matches. Mirrors the legacy `KrakenWssClient` depth param.
            "method": "subscribe",
            "params": { "channel": "book", "symbol": symbols, "depth": 10 },
        });
        let trade = serde_json::json!({
            "method": "subscribe",
            "params": { "channel": "trade", "symbol": symbols },
        });
        vec![
            Message::Text(book.to_string().into()),
            Message::Text(trade.to_string().into()),
        ]
    }

    /// Kraken server-heartbeats (`{"channel":"heartbeat"}` ~1/s, swallowed by
    /// the decoder); no client ping.
    fn heartbeat(&self) -> Heartbeat {
        Heartbeat::None
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Normalizer — decoded KrakenWssEvent → DomainEvent
// ─────────────────────────────────────────────────────────────────────────

/// Maps a decoded [`KrakenWssEvent`] to `DomainEvent`s. Derives the canonical
/// pair from the event's venue symbol (the socket is multi-symbol). Holds the
/// worker's shared [`SourceMetrics`] so every dropped event is counted, not
/// just logged.
#[derive(Default)]
pub struct KrakenNormalizer {
    pub metrics: SourceMetrics,
}

impl KrakenNormalizer {
    /// Build a `NormalizedDelta` from one Kraken `book` payload. Kraken's book
    /// is CRC32-validated rather than sequence-numbered, so the venue exposes no
    /// monotonic update id; `to_normalized` rides the per-frame `checksum` on
    /// both `update_id` and `sequence` (the `ChecksumDelta` model validates via
    /// CRC32, not the id). The first `data` entry per symbol is the book payload
    /// (Kraken sends one entry per symbol per frame).
    fn to_delta(resp: &KrakenBookResponse) -> Option<NormalizedDelta> {
        resp.to_normalized()
    }
}

impl Normalizer for KrakenNormalizer {
    type Event = KrakenWssEvent;

    fn normalize(&self, event: KrakenWssEvent) -> Vec<DomainEvent> {
        match event {
            KrakenWssEvent::OrderbookData(resp) => Self::to_delta(&resp)
                .map(DomainEvent::Book)
                .into_iter()
                .collect(),
            // A `trade` frame batches multiple trades; emit one Trade per trade
            // rather than only the first (no head-drop).
            KrakenWssEvent::TradeData(trades) => trades
                .into_iter()
                .filter_map(|t| normalize_trade(t, &self.metrics))
                .collect(),
        }
    }
}

/// Map one decoded Kraken trade print to a `DomainEvent::Trade`. Pair is decoded
/// via the venue `SymbolCodec` (`"BTC/USD"` → `BTC`/`USD`); price/qty arrive as
/// JSON floats already. Every dropped print bumps `dropped_frames`.
fn normalize_trade(t: KrakenTradeData, metrics: &SourceMetrics) -> Option<DomainEvent> {
    let Some(pair) = KRAKEN_CODEC.decode(&t.symbol) else {
        tracing::warn!(symbol = %t.symbol, "kraken.trade.bad_symbol");
        metrics.add_dropped_frames(1);
        return None;
    };
    let Some(side) = TradeSide::from_str_loose(&t.side) else {
        tracing::warn!(side = %t.side, id = %t.trade_id, "kraken.trade.unknown_side");
        metrics.add_dropped_frames(1);
        return None;
    };
    let trade = Trade {
        source_trade_ts_us: t.timestamp_us(),
        local_trade_ts_us: 0,
        source_trade_rtt_us: 0,
        pair,
        side,
        amount: f64_to_decimal(t.qty),
        price: f64_to_decimal(t.price),
        exchange: "kraken".to_string(),
        id: t.trade_id.to_string(),
        origin: Default::default(),
    };
    // Kraken's `trade_id` is a per-pair monotonic counter (u64, verified
    // ascending +1 on live BTC/USD capture, cycle #2 trade completion);
    // arming it feeds the SourcedTradebook loss accounting. Trades never
    // gap/resync (no re-seed).
    Some(DomainEvent::Trade {
        trade,
        sequence: Some(t.trade_id),
    })
}

// ─────────────────────────────────────────────────────────────────────────
// ExchangeAdapter — registry entry
// ─────────────────────────────────────────────────────────────────────────

/// Static, data-only Kraken profile. Const-constructible so it lives in a
/// `static` (no init-time allocation: `Vec::new` is `const`).
static KRAKEN_PROFILE: ExchangeProfile = ExchangeProfile {
    id: "kraken",
    symbol_codec: KRAKEN_CODEC,
    budget: ConnectionBudget {
        // Kraken public WSS: no documented hard cap on symbols per socket.
        max_connections: None,
        max_streams_per_socket: None,
        subscribe_rate: Vec::new(),
        connect_attempt_rate: None,
        connection_lifetime: None,
    },
    schema_version: 1,
    protocol_revision: "kraken-v2",
};

/// The registry handle. Unit struct — all state is in the static profile.
pub struct KrakenAdapter;

/// The single compiled-in Kraken instance (referenced by `register_all`).
pub static KRAKEN: KrakenAdapter = KrakenAdapter;

impl ExchangeAdapter for KrakenAdapter {
    fn id(&self) -> &'static str {
        "kraken"
    }

    fn profile(&self) -> &ExchangeProfile {
        &KRAKEN_PROFILE
    }

    fn book_model(&self, _channel: &str) -> ReconstructionModel {
        // Incremental `book`: CRC32-validated deltas, Kraken top-10 recipe.
        ReconstructionModel::ChecksumDelta {
            fmt: ChecksumFmt::KrakenTop10,
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
        tokio::spawn(drive::<KrakenHooks, KrakenDecoder, KrakenNormalizer>(
            Arc::new(KrakenHooks),
            symbols,
            declared,
            KrakenNormalizer {
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
        let normalizer = KrakenNormalizer {
            metrics: SourceMetrics::default(),
        };
        match KrakenDecoder::decode(raw)? {
            Some(event) => Ok(normalizer.normalize(event)),
            None => Ok(Vec::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::wss::WssDecoder;
    use crate::sources::kraken::responses::orderbooks::{
        KrakenBookData, KrakenPriceLevel,
    };
    use aetelier_types::orderbooks::decimal_to_f64;
    use aetelier_types::trading_pair::TradingPair;

    fn level(price: f64, qty: f64) -> KrakenPriceLevel {
        KrakenPriceLevel {
            price: price.to_string(),
            qty: qty.to_string(),
        }
    }

    fn book_resp(ty: &str) -> KrakenBookResponse {
        KrakenBookResponse {
            channel: "book".into(),
            ty: ty.into(),
            data: vec![KrakenBookData {
                symbol: "BTC/USD".into(),
                bids: vec![level(21921.73, 0.063)],
                asks: vec![level(21922.00, 0.500)],
                checksum: 2439117997,
                timestamp: "2023-09-26T16:49:20.962586Z".into(),
            }],
        }
    }

    #[test]
    fn normalizes_book_snapshot_to_book_event() {
        let evs = KrakenNormalizer::default()
            .normalize(KrakenWssEvent::OrderbookData(book_resp("snapshot")));
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            DomainEvent::Book(nd) => {
                assert_eq!(nd.symbol, "BTC/USD");
                // Kraken is checksum-validated: update_id/sequence carry the CRC32.
                assert_eq!(nd.update_id, 2439117997);
                assert_eq!(nd.sequence, 2439117997);
                assert!(nd.is_snapshot);
                assert_eq!(nd.bids.len(), 1);
                assert_eq!(nd.asks.len(), 1);
            }
            other => panic!("expected Book, got {other:?}"),
        }
    }

    #[test]
    fn book_update_is_not_a_snapshot() {
        let evs = KrakenNormalizer::default()
            .normalize(KrakenWssEvent::OrderbookData(book_resp("update")));
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            DomainEvent::Book(nd) => assert!(!nd.is_snapshot),
            other => panic!("expected Book, got {other:?}"),
        }
    }

    #[test]
    fn normalizes_trade_to_domain_event() {
        let t = KrakenTradeData {
            symbol: "BTC/USD".into(),
            side: "sell".into(),
            price: 23536.30,
            qty: 0.001,
            ord_type: "limit".into(),
            trade_id: 12345,
            timestamp: "2023-02-09T20:19:35.396Z".into(),
        };
        let evs =
            KrakenNormalizer::default().normalize(KrakenWssEvent::TradeData(vec![t]));
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            DomainEvent::Trade { trade, sequence } => {
                assert_eq!(trade.exchange, "kraken");
                assert_eq!(trade.id, "12345");
                assert_eq!(trade.side, TradeSide::Sell);
                assert!((decimal_to_f64(trade.price) - 23536.30).abs() < 1e-6);
                assert!((decimal_to_f64(trade.amount) - 0.001).abs() < 1e-9);
                assert_eq!(trade.pair, TradingPair::new("BTC", "USD"));
                // The per-pair trade_id arms the sequence (cycle #2 completion).
                assert_eq!(*sequence, Some(12345));
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    fn trade_data(id: u64, side: &str, price: f64) -> KrakenTradeData {
        KrakenTradeData {
            symbol: "BTC/USD".into(),
            side: side.into(),
            price,
            qty: 0.001,
            ord_type: "limit".into(),
            trade_id: id,
            timestamp: "2023-02-09T20:19:35.396Z".into(),
        }
    }

    #[test]
    fn multi_trade_frame_yields_one_domain_event_per_trade_no_head_drop() {
        // Kraken v2 batches trades per frame; N (>=3) trades must yield N Trades.
        let trades = vec![
            trade_data(1, "buy", 100.0),
            trade_data(2, "sell", 101.0),
            trade_data(3, "buy", 102.0),
        ];
        let evs =
            KrakenNormalizer::default().normalize(KrakenWssEvent::TradeData(trades));
        assert_eq!(evs.len(), 3, "all trades must survive (no head-drop)");
        let ids: Vec<&str> = evs
            .iter()
            .map(|e| match e {
                DomainEvent::Trade { trade, .. } => trade.id.as_str(),
                other => panic!("expected Trade, got {other:?}"),
            })
            .collect();
        assert_eq!(ids, ["1", "2", "3"]);
    }

    #[test]
    fn decoder_preserves_all_trades_in_a_multi_trade_frame() {
        // End-to-end: a raw v2 trade frame with 3 trades decodes to 3.
        let raw = r#"{"channel":"trade","type":"update","data":[
            {"symbol":"BTC/USD","side":"buy","price":100.0,"qty":0.001,"ord_type":"limit","trade_id":1,"timestamp":"2023-02-09T20:19:35.396Z"},
            {"symbol":"BTC/USD","side":"sell","price":101.0,"qty":0.001,"ord_type":"limit","trade_id":2,"timestamp":"2023-02-09T20:19:35.396Z"},
            {"symbol":"BTC/USD","side":"buy","price":102.0,"qty":0.001,"ord_type":"limit","trade_id":3,"timestamp":"2023-02-09T20:19:35.396Z"}
        ]}"#;
        let KrakenWssEvent::TradeData(trades) =
            KrakenDecoder::decode(raw).unwrap().unwrap()
        else {
            panic!("expected TradeData");
        };
        assert_eq!(trades.len(), 3);
        let evs =
            KrakenNormalizer::default().normalize(KrakenWssEvent::TradeData(trades));
        assert_eq!(evs.len(), 3);
    }

    #[test]
    fn subscribe_frames_carry_book_and_trade_channels() {
        let frames =
            KrakenHooks.subscribe_frames(&["BTC/USD".to_string()], &DeclaredSet::all());
        assert_eq!(frames.len(), 2);
        let bodies: Vec<serde_json::Value> = frames
            .iter()
            .map(|f| match f {
                Message::Text(b) => serde_json::from_str(b).unwrap(),
                _ => panic!("expected a text frame"),
            })
            .collect();
        assert_eq!(bodies[0]["method"], "subscribe");
        assert_eq!(bodies[0]["params"]["channel"], "book");
        assert_eq!(bodies[0]["params"]["symbol"][0], "BTC/USD");
        // Depth pinned to the top-10 checksum window (KrakenTop10).
        assert_eq!(bodies[0]["params"]["depth"], 10);
        assert_eq!(bodies[1]["params"]["channel"], "trade");
        assert_eq!(bodies[1]["params"]["symbol"][0], "BTC/USD");
    }

    #[test]
    fn decoder_dispatches_trade_frame_through_normalizer() {
        // End-to-end the decoder → normalizer path on a raw v2 trade frame.
        let raw = r#"{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"buy","price":23536.30,"qty":0.001,"ord_type":"limit","trade_id":99,"timestamp":"2023-02-09T20:19:35.396Z"}]}"#;
        let ev = KrakenDecoder::decode(raw)
            .expect("decode ok")
            .expect("a trade event");
        let evs = KrakenNormalizer::default().normalize(ev);
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            DomainEvent::Trade { trade, .. } => {
                assert_eq!(trade.id, "99");
                assert_eq!(trade.side, TradeSide::Buy);
            }
            other => panic!("expected Trade, got {other:?}"),
        }
    }

    #[test]
    fn profile_and_checksum_model_match_the_kraken_row() {
        let a = KrakenAdapter;
        assert_eq!(a.id(), "kraken");
        assert_eq!(a.profile().id, "kraken");
        assert_eq!(a.profile().protocol_revision, "kraken-v2");
        assert!(matches!(a.profile().symbol_codec, SymbolCodec::Slash));
        assert!(matches!(
            a.book_model("book"),
            ReconstructionModel::ChecksumDelta {
                fmt: ChecksumFmt::KrakenTop10,
            }
        ));
    }
}
