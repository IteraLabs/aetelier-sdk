//! Integration tests for the `data_worker` module.
//!
//! These tests verify the correctness of the data worker's internal
//! components **without** requiring a live exchange connection.  They
//! exercise:
//!
//! - [`ConnectionManager`] state machine transitions and backoff
//! - [`TopicRegistry`] creation, publishing, and subscription
//! - [`GapDetector`] silence detection
//! - [`classify_event`] routing for all exchange variants
//! - Config → registry → publish → subscribe round-trip

#![allow(deprecated)] // exercises the deprecated legacy raw-ingestion path (still live for derivatives/BYO)

use std::time::Duration;

use aetelier_connect::clients::connection_manager::{
    ConnectionManager, ConnectionManagerConfig,
};
use aetelier_connect::clients::connection_state::ConnectionState;
use aetelier_connect::clients::disconnect::DisconnectReason;
use aetelier_connect::clients::reconnect::ReconnectAction;
use aetelier_connect::sources::ExchangeEvent;
use aetelier_connect::sources::bybit::events::BybitWssEvent;
use aetelier_connect::sources::bybit::responses::BybitTradeData;
use aetelier_connect::workers::data_worker::DataWorker;
use aetelier_connect::workers::gap_detector::{GapDetector, GapDetectorSet};
use aetelier_connect::workers::topic_publisher::{TopicMessage, TopicRegistry};
use aetelier_types::config::markets::market_config::{
    DataTypesSection, ExchangeSection, FeedToggle, LogsSection, MarketSnapshotConfig,
    OrderbookConfig, OutputSection, PipelineSection, SymbolSection, SyncMode, TimeUnit,
    UpdateFrequency,
};

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn sample_config() -> MarketSnapshotConfig {
    MarketSnapshotConfig {
        exchange: ExchangeSection {
            name: "bybit".into(),
        },
        symbol: SymbolSection {
            name: "BTCUSDT".into(),
            sync_mode: SyncMode::OnTrade,
        },
        update_frequency: UpdateFrequency {
            value: 100,
            unit: TimeUnit::Millis,
        },
        pipeline: PipelineSection { flush_threshold: 0 },
        datatypes: DataTypesSection {
            orderbook: OrderbookConfig {
                enabled: true,
                depth: 50,
            },
            trades: FeedToggle { enabled: true },
            liquidations: FeedToggle { enabled: true },
            funding_rates: FeedToggle { enabled: false },
            open_interest: FeedToggle { enabled: false },
        },
        output: OutputSection {
            dir: "/tmp/test".into(),
        },
        logs: LogsSection {
            n_orderbooks: 0,
            n_trades: 0,
            n_liquidations: 0,
            n_fundings: 0,
            n_open_interests: 0,
        },
    }
}

fn bybit_trade() -> ExchangeEvent {
    ExchangeEvent::Bybit(BybitWssEvent::TradeData(vec![BybitTradeData {
        trade_ts: 1_700_000_000_000,
        symbol: "BTCUSDT".into(),
        side: "Buy".into(),
        amount: "0.001".into(),
        price: "42000.0".into(),
        direction: Some("PlusTick".into()),
        trade_id: "test-001".into(),
        block_trade: false,
        rpi_trade: false,
        sequence: 1,
    }]))
}

// ─────────────────────────────────────────────────────────────────────────────
// ConnectionManager tests
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn connection_manager_initial_state() {
    let mgr = ConnectionManager::with_defaults("test:BTCUSDT");
    assert_eq!(mgr.state(), ConnectionState::Disconnected);
    assert_eq!(mgr.consecutive_failures(), 0);
    assert!(mgr.transitions().is_empty());
}

#[test]
fn connection_manager_full_lifecycle() {
    let mut mgr = ConnectionManager::with_defaults("test:BTCUSDT");

    mgr.transition(ConnectionState::Connecting, "initial");
    assert_eq!(mgr.state(), ConnectionState::Connecting);

    mgr.transition(ConnectionState::Subscribing, "ws opened");
    assert_eq!(mgr.state(), ConnectionState::Subscribing);

    mgr.transition(ConnectionState::Streaming, "first event");
    mgr.on_connected();
    assert_eq!(mgr.state(), ConnectionState::Streaming);
    assert_eq!(mgr.consecutive_failures(), 0);

    // Simulate disconnect
    let reason = DisconnectReason::TransportError {
        source: "tcp reset".into(),
    };
    let action = mgr.on_disconnect(&reason);
    assert!(matches!(
        mgr.state(),
        ConnectionState::Reconnecting { attempt: 1 }
    ));
    assert!(matches!(action, ReconnectAction::RetryAfter(_)));

    assert_eq!(mgr.transitions().len(), 4); // connect, subscribe, stream, reconnect
}

#[test]
fn connection_manager_spec_backoff_config() {
    let cfg = ConnectionManagerConfig::default();
    assert_eq!(cfg.initial_delay, Duration::from_millis(100));
    assert_eq!(cfg.max_delay, Duration::from_secs(10));
    assert!(cfg.max_attempts.is_none());
}

#[test]
fn connection_manager_non_retryable_gives_up() {
    let mut mgr = ConnectionManager::with_defaults("test:BTCUSDT");
    let reason = DisconnectReason::ReceiverDropped;
    let action = mgr.on_disconnect(&reason);
    assert!(matches!(action, ReconnectAction::GiveUp { .. }));
}

// ─────────────────────────────────────────────────────────────────────────────
// TopicRegistry tests
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn registry_from_config_creates_expected_topics() {
    let cfg = sample_config();
    let registry = TopicRegistry::from_config(
        &cfg.exchange.name,
        &cfg.symbol.name,
        &cfg.datatypes,
        64,
    );

    assert_eq!(registry.len(), 3);
    assert!(registry.get("orderbook.50.BTCUSDT").is_some());
    assert!(registry.get("trade.all.BTCUSDT").is_some());
    assert!(registry.get("liquidation.all.BTCUSDT").is_some());
    // disabled in config
    assert!(registry.get("funding.all.BTCUSDT").is_none());
    assert!(registry.get("open_interest.all.BTCUSDT").is_none());
}

#[test]
fn registry_subscribe_all_returns_all_topics() {
    let cfg = sample_config();
    let registry = TopicRegistry::from_config(
        &cfg.exchange.name,
        &cfg.symbol.name,
        &cfg.datatypes,
        64,
    );

    let subs = registry.subscribe_all();
    assert_eq!(subs.len(), 3);

    let topic_names: Vec<&str> = subs.iter().map(|(n, _)| n.as_str()).collect();
    assert!(topic_names.contains(&"orderbook.50.BTCUSDT"));
    assert!(topic_names.contains(&"trade.all.BTCUSDT"));
    assert!(topic_names.contains(&"liquidation.all.BTCUSDT"));
}

#[test]
fn publish_subscribe_roundtrip() {
    let cfg = sample_config();
    let registry = TopicRegistry::from_config(
        &cfg.exchange.name,
        &cfg.symbol.name,
        &cfg.datatypes,
        64,
    );

    let mut rx = registry.subscribe("trade.all.BTCUSDT").unwrap();

    let msg = TopicMessage {
        topic: "trade.all.BTCUSDT".into(),
        received_at_us: 42,
        exchange: "bybit".into(),
        payload: bybit_trade(),
    };

    let n = registry.publish("trade.all.BTCUSDT", msg).unwrap();
    assert_eq!(n, 1);

    let received = rx.try_recv().unwrap();
    assert_eq!(received.topic, "trade.all.BTCUSDT");
    assert_eq!(received.received_at_us, 42);
    assert_eq!(received.exchange, "bybit");

    // Verify the payload is the raw event, unmodified
    match &received.payload {
        ExchangeEvent::Bybit(BybitWssEvent::TradeData(td)) => {
            assert_eq!(td[0].price, "42000.0");
            assert_eq!(td[0].symbol, "BTCUSDT");
        }
        _ => panic!("expected Bybit trade event"),
    }
}

#[test]
fn publish_no_receivers_succeeds() {
    let cfg = sample_config();
    let registry = TopicRegistry::from_config(
        &cfg.exchange.name,
        &cfg.symbol.name,
        &cfg.datatypes,
        64,
    );

    // No subscriber — should succeed with 0 receivers
    let msg = TopicMessage {
        topic: "trade.all.BTCUSDT".into(),
        received_at_us: 0,
        exchange: "bybit".into(),
        payload: bybit_trade(),
    };

    let n = registry.publish("trade.all.BTCUSDT", msg).unwrap();
    assert_eq!(n, 0);
}

#[test]
fn publish_unknown_topic_is_error() {
    let cfg = sample_config();
    let registry = TopicRegistry::from_config(
        &cfg.exchange.name,
        &cfg.symbol.name,
        &cfg.datatypes,
        64,
    );

    let msg = TopicMessage {
        topic: "bad.topic".into(),
        received_at_us: 0,
        exchange: "bybit".into(),
        payload: bybit_trade(),
    };

    assert!(registry.publish("bad.topic", msg).is_err());
}

#[test]
fn multiple_subscribers_receive_same_message() {
    let cfg = sample_config();
    let registry = TopicRegistry::from_config(
        &cfg.exchange.name,
        &cfg.symbol.name,
        &cfg.datatypes,
        64,
    );

    let mut rx1 = registry.subscribe("trade.all.BTCUSDT").unwrap();
    let mut rx2 = registry.subscribe("trade.all.BTCUSDT").unwrap();

    let msg = TopicMessage {
        topic: "trade.all.BTCUSDT".into(),
        received_at_us: 99,
        exchange: "bybit".into(),
        payload: bybit_trade(),
    };

    let n = registry.publish("trade.all.BTCUSDT", msg).unwrap();
    assert_eq!(n, 2);

    assert_eq!(rx1.try_recv().unwrap().received_at_us, 99);
    assert_eq!(rx2.try_recv().unwrap().received_at_us, 99);
}

// ─────────────────────────────────────────────────────────────────────────────
// GapDetector tests
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn gap_detector_first_event_no_gap() {
    let mut det = GapDetector::with_defaults("test");
    assert!(det.record_event().is_none());
    assert_eq!(det.gap_count(), 0);
    assert_eq!(det.total_gap_ms(), 0);
}

#[test]
fn gap_detector_rapid_events_no_gap() {
    let mut det = GapDetector::new("test", Duration::from_secs(1));
    det.record_event();
    assert!(det.record_event().is_none());
    assert_eq!(det.gap_count(), 0);
}

#[tokio::test]
async fn gap_detector_detects_silence() {
    let threshold = Duration::from_millis(50);
    let mut det = GapDetector::new("test", threshold);

    det.record_event();
    tokio::time::sleep(Duration::from_millis(80)).await;

    let gap = det.record_event();
    assert!(gap.is_some());
    assert!(gap.unwrap() >= 50);
    assert_eq!(det.gap_count(), 1);
}

#[test]
fn gap_detector_set_routes_by_topic() {
    let topics = &["topic.a", "topic.b"];
    let mut set = GapDetectorSet::new(topics, Duration::from_secs(1));

    // First events — never gaps
    assert!(set.record_event("topic.a").is_none());
    assert!(set.record_event("topic.b").is_none());

    // Unknown topic — None (no panic)
    assert!(set.record_event("topic.unknown").is_none());

    let stats = set.stats();
    assert_eq!(stats.len(), 2);
    assert!(stats.iter().all(|s| s.gap_count == 0));
}

// ─────────────────────────────────────────────────────────────────────────────
// DataWorker construction tests
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn data_worker_build_registry_matches_config() {
    let cfg = sample_config();
    let worker = DataWorker::new(cfg);
    let registry = worker.build_registry();

    assert_eq!(registry.len(), 3);
    assert!(registry.get("orderbook.50.BTCUSDT").is_some());
    assert!(registry.get("trade.all.BTCUSDT").is_some());
    assert!(registry.get("liquidation.all.BTCUSDT").is_some());
}

#[test]
fn data_worker_all_datatypes_registry() {
    let mut cfg = sample_config();
    cfg.datatypes.funding_rates.enabled = true;
    cfg.datatypes.open_interest.enabled = true;

    let worker = DataWorker::new(cfg);
    let registry = worker.build_registry();

    assert_eq!(registry.len(), 5);
    assert!(registry.get("orderbook.50.BTCUSDT").is_some());
    assert!(registry.get("trade.all.BTCUSDT").is_some());
    assert!(registry.get("liquidation.all.BTCUSDT").is_some());
    assert!(registry.get("funding.all.BTCUSDT").is_some());
    assert!(registry.get("open_interest.all.BTCUSDT").is_some());
}

#[test]
fn data_worker_builder_methods() {
    // Builder methods (with_channel_capacity, with_gap_threshold, etc.) have
    // been replaced by CommonWorkerFields config.  This test now verifies that
    // the legacy constructor + build_registry path still works.
    let cfg = sample_config();
    let worker = DataWorker::new(cfg);

    let registry = worker.build_registry();
    assert!(!registry.is_empty());
}

/// Verify the DataWorker exits cleanly when shutdown is signalled
/// immediately (no network connection attempted because shutdown
/// is already `true` at the start of the reconnect loop).
#[tokio::test]
async fn data_worker_immediate_shutdown() {
    let cfg = sample_config();
    let worker = DataWorker::new(cfg);
    let registry = worker.build_registry();

    let (_tx, rx) = tokio::sync::watch::channel(true); // already shut down

    let report = worker.run_legacy(rx, registry).await.unwrap();

    assert_eq!(report.exchange, "bybit");
    assert_eq!(report.symbol, "BTCUSDT");
    assert_eq!(report.total_events, 0);
    assert_eq!(report.reconnect_count, 0);
    assert!(report.errors.is_empty());
}

/// Verify the DataWorker shuts down promptly when the shutdown signal
/// fires shortly after start (before a real connection can establish).
#[tokio::test]
async fn data_worker_delayed_shutdown() {
    let cfg = sample_config();
    let worker = DataWorker::new(cfg);
    let registry = worker.build_registry();

    let (tx, rx) = tokio::sync::watch::channel(false);

    // Signal shutdown after a brief delay
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = tx.send(true);
    });

    let report =
        tokio::time::timeout(Duration::from_secs(5), worker.run_legacy(rx, registry))
            .await
            .expect("DataWorker should exit within 5 s")
            .unwrap();

    assert_eq!(report.exchange, "bybit");
    assert_eq!(report.symbol, "BTCUSDT");
    // May or may not have attempted a connection in 50 ms
    assert!(report.elapsed_secs < 6.0);
}

// ─────────────────────────────────────────────────────────────────────────────
// Coinbase topic registry tests
// ─────────────────────────────────────────────────────────────────────────────

fn coinbase_config() -> MarketSnapshotConfig {
    MarketSnapshotConfig {
        exchange: ExchangeSection {
            name: "coinbase".into(),
        },
        symbol: SymbolSection {
            name: "BTC-USD".into(),
            sync_mode: SyncMode::OnTrade,
        },
        update_frequency: UpdateFrequency {
            value: 100,
            unit: TimeUnit::Millis,
        },
        pipeline: PipelineSection { flush_threshold: 0 },
        datatypes: DataTypesSection {
            orderbook: OrderbookConfig {
                enabled: true,
                depth: 50,
            },
            trades: FeedToggle { enabled: true },
            liquidations: FeedToggle { enabled: false },
            funding_rates: FeedToggle { enabled: false },
            open_interest: FeedToggle { enabled: false },
        },
        output: OutputSection {
            dir: "/tmp/test".into(),
        },
        logs: LogsSection {
            n_orderbooks: 0,
            n_trades: 0,
            n_liquidations: 0,
            n_fundings: 0,
            n_open_interests: 0,
        },
    }
}

#[test]
fn coinbase_registry_creates_expected_topics() {
    let cfg = coinbase_config();
    let registry = TopicRegistry::from_config(
        &cfg.exchange.name,
        &cfg.symbol.name,
        &cfg.datatypes,
        64,
    );

    // Coinbase spot: only orderbook + trade
    assert_eq!(registry.len(), 2);
    assert!(registry.get("orderbook.50.BTC-USD").is_some());
    assert!(registry.get("trade.all.BTC-USD").is_some());
}

#[test]
fn coinbase_publish_trade_roundtrip() {
    let cfg = coinbase_config();
    let registry = TopicRegistry::from_config(
        &cfg.exchange.name,
        &cfg.symbol.name,
        &cfg.datatypes,
        64,
    );

    let mut rx = registry.subscribe("trade.all.BTC-USD").unwrap();

    let event = ExchangeEvent::Coinbase(
        aetelier_connect::sources::coinbase::events::CoinbaseWssEvent::TradeData(vec![
            aetelier_connect::sources::coinbase::responses::CoinbaseTradeData {
                trade_id: "cb-001".into(),
                product_id: "BTC-USD".into(),
                price: "42000.00".into(),
                size: "0.001".into(),
                side: "BUY".into(),
                time: "2024-01-15T12:00:00.000Z".into(),
            },
        ]),
    );

    let msg = TopicMessage {
        topic: "trade.all.BTC-USD".into(),
        received_at_us: 555,
        exchange: "coinbase".into(),
        payload: event,
    };

    registry.publish("trade.all.BTC-USD", msg).unwrap();
    let received = rx.try_recv().unwrap();

    assert_eq!(received.exchange, "coinbase");
    match &received.payload {
        ExchangeEvent::Coinbase(
            aetelier_connect::sources::coinbase::events::CoinbaseWssEvent::TradeData(td),
        ) => {
            assert_eq!(td.len(), 1);
            assert_eq!(td[0].price, "42000.00");
            assert_eq!(td[0].product_id, "BTC-USD");
        }
        _ => panic!("expected Coinbase trade event"),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Kraken topic registry tests
// ─────────────────────────────────────────────────────────────────────────────

fn kraken_config() -> MarketSnapshotConfig {
    MarketSnapshotConfig {
        exchange: ExchangeSection {
            name: "kraken".into(),
        },
        symbol: SymbolSection {
            name: "BTC/USD".into(),
            sync_mode: SyncMode::OnTrade,
        },
        update_frequency: UpdateFrequency {
            value: 100,
            unit: TimeUnit::Millis,
        },
        pipeline: PipelineSection { flush_threshold: 0 },
        datatypes: DataTypesSection {
            orderbook: OrderbookConfig {
                enabled: true,
                depth: 25,
            },
            trades: FeedToggle { enabled: true },
            liquidations: FeedToggle { enabled: false },
            funding_rates: FeedToggle { enabled: false },
            open_interest: FeedToggle { enabled: false },
        },
        output: OutputSection {
            dir: "/tmp/test".into(),
        },
        logs: LogsSection {
            n_orderbooks: 0,
            n_trades: 0,
            n_liquidations: 0,
            n_fundings: 0,
            n_open_interests: 0,
        },
    }
}

#[test]
fn kraken_registry_creates_expected_topics() {
    let cfg = kraken_config();
    let registry = TopicRegistry::from_config(
        &cfg.exchange.name,
        &cfg.symbol.name,
        &cfg.datatypes,
        64,
    );

    assert_eq!(registry.len(), 2);
    assert!(registry.get("orderbook.25.BTC/USD").is_some());
    assert!(registry.get("trade.all.BTC/USD").is_some());
}

#[test]
fn kraken_publish_trade_roundtrip() {
    let cfg = kraken_config();
    let registry = TopicRegistry::from_config(
        &cfg.exchange.name,
        &cfg.symbol.name,
        &cfg.datatypes,
        64,
    );

    let mut rx = registry.subscribe("trade.all.BTC/USD").unwrap();

    let event = ExchangeEvent::Kraken(
        aetelier_connect::sources::kraken::events::KrakenWssEvent::TradeData(vec![
            aetelier_connect::sources::kraken::responses::KrakenTradeData {
                symbol: "BTC/USD".into(),
                side: "buy".into(),
                price: 42000.0,
                qty: 0.001,
                ord_type: "market".into(),
                trade_id: 12345,
                timestamp: "2024-01-15T12:00:00.000000Z".into(),
            },
        ]),
    );

    let msg = TopicMessage {
        topic: "trade.all.BTC/USD".into(),
        received_at_us: 777,
        exchange: "kraken".into(),
        payload: event,
    };

    registry.publish("trade.all.BTC/USD", msg).unwrap();
    let received = rx.try_recv().unwrap();

    assert_eq!(received.exchange, "kraken");
    match &received.payload {
        ExchangeEvent::Kraken(
            aetelier_connect::sources::kraken::events::KrakenWssEvent::TradeData(td),
        ) => {
            assert_eq!(td.len(), 1);
            assert_eq!(td[0].price, 42000.0);
            assert_eq!(td[0].symbol, "BTC/USD");
        }
        _ => panic!("expected Kraken trade event"),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Topic naming convention tests
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn topic_naming_convention_orderbook() {
    let cfg = sample_config();
    let registry = TopicRegistry::from_config(
        &cfg.exchange.name,
        &cfg.symbol.name,
        &cfg.datatypes,
        64,
    );
    // orderbook.{depth}.{symbol}
    assert!(registry.get("orderbook.50.BTCUSDT").is_some());
}

#[test]
fn topic_naming_with_different_depth() {
    let mut dt = sample_config().datatypes;
    dt.orderbook.depth = 200;

    let registry = TopicRegistry::from_config("bybit", "BTCUSDT", &dt, 64);
    assert!(registry.get("orderbook.200.BTCUSDT").is_some());
    assert!(registry.get("orderbook.50.BTCUSDT").is_none());
}
