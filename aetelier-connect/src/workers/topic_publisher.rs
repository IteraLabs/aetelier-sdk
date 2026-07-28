//! Topic-keyed broadcast channels for raw event publishing.
//!
//! The `TopicPublisher` is the output interface of the
//! `DataWorker` (see `super::data_worker::DataWorker`).  Each exchange
//! subscription maps to exactly one topic, and the `DataWorker`
//! publishes raw (un-normalised) events through the matching publisher.
//!
//! # Topic naming convention
//!
//! ```text
//! {datatype}.{qualifier}.{symbol}
//! ```
//!
//! Examples:
//! - `orderbook.50.BTCUSDT`
//! - `trade.all.BTCUSDT`
//! - `liquidation.all.BTCUSDT`
//! - `funding.all.BTCUSDT`
//! - `open_interest.all.BTCUSDT`
//!
//! # Back-pressure
//!
//! The underlying `tokio::sync::broadcast` channel has a bounded
//! capacity.  If **all** receivers lag behind, the oldest messages are
//! silently dropped — this is acceptable because the `data_worker`'s
//! contract is *best-effort ingestion*, not guaranteed delivery.
//! Guaranteed delivery is the downstream consumer's responsibility.

use crate::framework::model::DomainEvent;
use crate::sources::ExchangeEvent;
use aetelier_types::config::markets::market_config::DataTypesSection;
use aetelier_types::snapshots::MarketSnapshot;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Default broadcast channel capacity per topic.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 8192;

// ─────────────────────────────────────────────────────────────────────────────
// TopicMessage
// ─────────────────────────────────────────────────────────────────────────────

/// Envelope wrapping a raw exchange event with ingestion metadata.
///
/// Published into broadcast channels by the `DataWorker`.  Downstream
/// consumers receive clones of this struct.
#[derive(Debug, Clone)]
pub struct TopicMessage {
    /// Canonical topic name (e.g. `"orderbook.50.BTCUSDT"`).
    pub topic: String,
    /// Wall-clock microsecond timestamp when the frame was received
    /// by the `DataWorker` (before any processing).
    pub received_at_us: u64,
    /// Exchange that produced this event.
    pub exchange: String,
    /// The raw decoded event — no normalisation, no delta application.
    pub payload: ExchangeEvent,
}

// ─────────────────────────────────────────────────────────────────────────────
// TopicPublisher
// ─────────────────────────────────────────────────────────────────────────────

/// A single named broadcast channel.
///
/// Holds the `broadcast::Sender` for one topic.  Subscribers obtain a
/// `broadcast::Receiver` by calling [`subscribe()`](Self::subscribe).
pub struct TopicPublisher {
    topic: String,
    tx: broadcast::Sender<TopicMessage>,
    /// The bounded capacity passed to `broadcast::channel()`.
    cap: usize,
}

impl TopicPublisher {
    /// Create a new publisher for the given topic with the specified
    /// channel capacity.
    pub fn new(topic: impl Into<String>, capacity: usize) -> Self {
        let topic = topic.into();
        let (tx, _) = broadcast::channel(capacity);
        Self {
            topic,
            tx,
            cap: capacity,
        }
    }

    /// The topic name this publisher writes to.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Publish a message.
    ///
    /// Returns `Ok(n)` where `n` is the number of active receivers, or
    /// `Err` if there are zero receivers (the message is still dropped
    /// in that case, which is fine for best-effort ingestion).
    pub fn send(
        &self,
        msg: TopicMessage,
    ) -> Result<usize, Box<tokio::sync::broadcast::error::SendError<TopicMessage>>> {
        Ok(self.tx.send(msg)?)
    }

    /// Create a new receiver subscribed to this topic.
    ///
    /// The receiver will see all messages published **after** this call.
    pub fn subscribe(&self) -> broadcast::Receiver<TopicMessage> {
        self.tx.subscribe()
    }

    /// Number of currently active receivers.
    pub fn receiver_count(&self) -> usize {
        self.tx.receiver_count()
    }

    /// Number of messages sitting in the channel (sent but not yet
    /// received by all receivers).
    pub fn len(&self) -> usize {
        self.tx.len()
    }

    /// Returns `true` when no unsent messages remain in the channel.
    pub fn is_empty(&self) -> bool {
        self.tx.len() == 0
    }

    /// The bounded capacity this channel was created with.
    pub fn capacity(&self) -> usize {
        self.cap
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SnapshotChannel
// ─────────────────────────────────────────────────────────────────────────────

/// Default capacity for a worker's [`SnapshotChannel`].
///
/// Derivation: snapshots are grid-paced (typically 250ms-1s per period), so
/// 1024 buffered snapshots buy a subscriber roughly 4-17 minutes of stall
/// before it starts lagging — generous next to the raw channels' venue-paced
/// 8192, at a fraction of the memory (Arc-shared payloads).
pub const DEFAULT_SNAPSHOT_CHANNEL_CAPACITY: usize = 1024;

/// The live broadcast channel for grid-aligned [`MarketSnapshot`]s — the
/// in-process subscription point for the synchronized stream (the channel
/// analog of what `ParquetSink` persists).
///
/// Contract: publishing never blocks the collector; payloads are
/// `Arc`-shared (one clone per emit, zero per subscriber); a slow subscriber
/// observes `RecvError::Lagged(n)` with the exact number of snapshots it
/// missed — quantified loss, never silent.
#[derive(Clone)]
pub struct SnapshotChannel {
    tx: broadcast::Sender<Arc<MarketSnapshot>>,
    cap: usize,
}

impl SnapshotChannel {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx, cap: capacity }
    }

    /// Publish one snapshot. Returns the number of active subscribers;
    /// `Err` means zero subscribers (the snapshot is dropped, which is the
    /// correct no-listener behavior, not a failure).
    pub fn publish(
        &self,
        snapshot: Arc<MarketSnapshot>,
    ) -> Result<usize, Box<broadcast::error::SendError<Arc<MarketSnapshot>>>> {
        Ok(self.tx.send(snapshot)?)
    }

    /// Subscribe to snapshots published **after** this call. A subscriber
    /// that falls more than the channel capacity behind receives
    /// `RecvError::Lagged(n)` carrying the exact count of missed snapshots.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<MarketSnapshot>> {
        self.tx.subscribe()
    }

    pub fn receiver_count(&self) -> usize {
        self.tx.receiver_count()
    }

    /// Snapshots queued and not yet seen by every subscriber.
    pub fn len(&self) -> usize {
        self.tx.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tx.len() == 0
    }

    pub fn capacity(&self) -> usize {
        self.cap
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TopicRegistry
// ─────────────────────────────────────────────────────────────────────────────

/// Collection of [`TopicPublisher`]s keyed by topic name.
///
/// Built at `DataWorker` startup from the configured exchange, symbol,
/// and enabled data types.
pub struct TopicRegistry {
    publishers: HashMap<String, TopicPublisher>,
}

impl TopicRegistry {
    /// Build a registry for the given exchange configuration.
    ///
    /// Creates one [`TopicPublisher`] per enabled data type.
    pub fn from_config(
        exchange: &str,
        symbol: &str,
        datatypes: &DataTypesSection,
        capacity: usize,
    ) -> Self {
        let mut publishers = HashMap::new();

        if datatypes.orderbook.enabled {
            let topic = format!("orderbook.{}.{}", datatypes.orderbook.depth, symbol);
            publishers.insert(topic.clone(), TopicPublisher::new(topic, capacity));
        }

        if datatypes.trades.enabled {
            let topic = format!("trade.all.{}", symbol);
            publishers.insert(topic.clone(), TopicPublisher::new(topic, capacity));
        }

        if datatypes.liquidations.enabled {
            let topic = format!("liquidation.all.{}", symbol);
            publishers.insert(topic.clone(), TopicPublisher::new(topic, capacity));
        }

        if datatypes.funding_rates.enabled {
            let topic = format!("funding.all.{}", symbol);
            publishers.insert(topic.clone(), TopicPublisher::new(topic, capacity));
        }

        if datatypes.open_interest.enabled {
            let topic = format!("open_interest.all.{}", symbol);
            publishers.insert(topic.clone(), TopicPublisher::new(topic, capacity));
        }

        tracing::info!(
            exchange = exchange,
            symbol = symbol,
            topics = ?publishers.keys().collect::<Vec<_>>(),
            "topic_registry.created"
        );

        Self { publishers }
    }

    /// Look up a publisher by topic name.
    pub fn get(&self, topic: &str) -> Option<&TopicPublisher> {
        self.publishers.get(topic)
    }

    /// All registered topic names.
    pub fn topics(&self) -> Vec<&str> {
        self.publishers.keys().map(|s| s.as_str()).collect()
    }

    /// Number of registered topics.
    pub fn len(&self) -> usize {
        self.publishers.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.publishers.is_empty()
    }

    /// Build a registry from explicit topic names (tests and ad-hoc wiring).
    pub fn with_topics(topics: &[&str], capacity: usize) -> Self {
        let mut publishers = HashMap::new();
        for topic in topics {
            publishers.insert(topic.to_string(), TopicPublisher::new(*topic, capacity));
        }
        Self { publishers }
    }

    /// Subscribe to a specific topic.
    ///
    /// Returns `None` if the topic doesn't exist.
    pub fn subscribe(&self, topic: &str) -> Option<broadcast::Receiver<TopicMessage>> {
        self.publishers.get(topic).map(|p| p.subscribe())
    }

    /// Subscribe to all topics.
    ///
    /// Returns a `Vec` of `(topic_name, Receiver)` pairs.
    pub fn subscribe_all(&self) -> Vec<(String, broadcast::Receiver<TopicMessage>)> {
        self.publishers
            .iter()
            .map(|(name, pub_)| (name.clone(), pub_.subscribe()))
            .collect()
    }

    /// Publish a message to the named topic.
    ///
    /// Returns `Ok(n_receivers)` on success.  Returns `Err` if the
    /// topic doesn't exist (programming error — the caller should only
    /// publish to topics that were registered at startup).
    pub fn publish(&self, topic: &str, msg: TopicMessage) -> Result<usize, PublishError> {
        let publisher = self
            .publishers
            .get(topic)
            .ok_or_else(|| PublishError::UnknownTopic(topic.to_string()))?;

        // broadcast::send fails only if there are zero receivers.
        // That's fine — we log it and move on.
        match publisher.send(msg) {
            Ok(n) => Ok(n),
            Err(_) => {
                tracing::trace!(topic = topic, "topic_registry.no_receivers");
                Ok(0)
            }
        }
    }

    /// Worst single-topic fill ratio (`len/capacity`), `None` when the
    /// registry has no topics. Backpressure detection keys on this, not the
    /// aggregate: one saturated hot topic must not hide in the average.
    pub fn max_fill_ratio(&self) -> Option<f64> {
        self.publishers
            .values()
            .filter(|p| p.capacity() > 0)
            .map(|p| p.len() as f64 / p.capacity() as f64)
            .fold(None, |acc, r| Some(acc.map_or(r, |a: f64| a.max(r))))
    }

    /// Aggregate queue depth and capacity across all registered topics.
    ///
    /// Returns `(total_messages_pending, total_capacity)`.
    pub fn queue_stats(&self) -> (u64, u64) {
        let mut depth: u64 = 0;
        let mut capacity: u64 = 0;
        for pub_ in self.publishers.values() {
            depth += pub_.len() as u64;
            capacity += pub_.capacity() as u64;
        }
        (depth, capacity)
    }
}

/// Error from [`TopicRegistry::publish()`].
#[derive(Debug, Clone)]
pub enum PublishError {
    /// Attempted to publish to a topic that was never registered.
    UnknownTopic(String),
}

impl std::fmt::Display for PublishError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownTopic(t) => write!(f, "unknown topic: {}", t),
        }
    }
}

impl std::error::Error for PublishError {}

// ─────────────────────────────────────────────────────────────────────────────
// Domain (normalized) topic family — framework DataWorker path
// ─────────────────────────────────────────────────────────────────────────────

/// Envelope wrapping a NORMALIZED [`DomainEvent`] with ingestion metadata.
///
/// Parallel to [`TopicMessage`] (which carries the raw, venue-typed
/// `ExchangeEvent`). The framework `DataWorker` path publishes these on its own
/// broadcast family so existing raw subscribers — and `TopicMessage` itself —
/// stay byte-for-byte untouched. A normalized `DomainEvent` cannot be losslessly
/// downcast to a venue-native `ExchangeEvent`, so the two streams are distinct.
#[derive(Debug, Clone)]
pub struct DomainTopicMessage {
    /// Canonical topic name (e.g. `"orderbook.50.BTCUSDT"`) — same naming as the
    /// raw family, so dashboards/parsers need no new patterns.
    pub topic: String,
    /// Wall-clock microsecond timestamp when the worker received the frame.
    pub received_at_us: u64,
    /// Exchange that produced this event (stamped by the worker — `DomainEvent`
    /// carries no venue tag).
    pub exchange: String,
    /// The normalized event (a `Book` delta or a `Trade`).
    pub payload: DomainEvent,
}

/// Broadcast topics carrying [`DomainTopicMessage`]s for the framework path.
///
/// Only `orderbook.{depth}.{symbol}` and `trade.all.{symbol}` exist, because
/// `DomainEvent` models only `Book | Trade`. `broadcast::Sender` is `Clone`, so
/// this registry is `Clone` and the publishing sink and downstream subscribers
/// can share the same channels.
#[derive(Clone)]
pub struct DomainTopicRegistry {
    publishers: HashMap<String, broadcast::Sender<DomainTopicMessage>>,
}

impl DomainTopicRegistry {
    /// Build the domain topics for the enabled book/trade datatypes. Derivatives
    /// datatypes are intentionally absent (not representable in `DomainEvent`).
    pub fn from_config(
        symbol: &str,
        datatypes: &DataTypesSection,
        capacity: usize,
    ) -> Self {
        let mut publishers = HashMap::new();
        if datatypes.orderbook.enabled {
            let topic = format!("orderbook.{}.{}", datatypes.orderbook.depth, symbol);
            publishers.insert(topic, broadcast::channel(capacity).0);
        }
        if datatypes.trades.enabled {
            let topic = format!("trade.all.{}", symbol);
            publishers.insert(topic, broadcast::channel(capacity).0);
        }
        Self { publishers }
    }

    /// Subscribe to a domain topic (`None` if the topic doesn't exist).
    pub fn subscribe(
        &self,
        topic: &str,
    ) -> Option<broadcast::Receiver<DomainTopicMessage>> {
        self.publishers.get(topic).map(|s| s.subscribe())
    }

    /// All registered domain topic names.
    pub fn topics(&self) -> Vec<&str> {
        self.publishers.keys().map(|s| s.as_str()).collect()
    }

    /// Number of registered domain topics.
    pub fn len(&self) -> usize {
        self.publishers.len()
    }

    /// Whether the registry has no topics.
    pub fn is_empty(&self) -> bool {
        self.publishers.is_empty()
    }

    /// Publish to a domain topic. Returns `Ok(0)` when there are no receivers
    /// (best-effort, same contract as the raw family).
    pub fn publish(
        &self,
        topic: &str,
        msg: DomainTopicMessage,
    ) -> Result<usize, PublishError> {
        let publisher = self
            .publishers
            .get(topic)
            .ok_or_else(|| PublishError::UnknownTopic(topic.to_string()))?;
        match publisher.send(msg) {
            Ok(n) => Ok(n),
            Err(_) => {
                tracing::trace!(topic = topic, "domain_topic_registry.no_receivers");
                Ok(0)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::bybit::events::BybitWssEvent;
    use crate::sources::bybit::responses::BybitTradeData;
    use aetelier_types::config::markets::market_config::{
        DataTypesSection, FeedToggle, OrderbookConfig,
    };

    fn sample_datatypes() -> DataTypesSection {
        DataTypesSection {
            orderbook: OrderbookConfig {
                enabled: true,
                depth: 50,
            },
            trades: FeedToggle { enabled: true },
            liquidations: FeedToggle { enabled: true },
            funding_rates: FeedToggle { enabled: false },
            open_interest: FeedToggle { enabled: false },
        }
    }

    /// Construct a minimal `ExchangeEvent` for testing.
    fn dummy_event() -> ExchangeEvent {
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

    #[test]
    fn registry_creates_topics_for_enabled_datatypes() {
        let registry =
            TopicRegistry::from_config("bybit", "BTCUSDT", &sample_datatypes(), 64);

        assert_eq!(registry.len(), 3);
        assert!(registry.get("orderbook.50.BTCUSDT").is_some());
        assert!(registry.get("trade.all.BTCUSDT").is_some());
        assert!(registry.get("liquidation.all.BTCUSDT").is_some());
        // disabled
        assert!(registry.get("funding.all.BTCUSDT").is_none());
        assert!(registry.get("open_interest.all.BTCUSDT").is_none());
    }

    #[test]
    fn subscribe_and_publish() {
        let registry =
            TopicRegistry::from_config("bybit", "BTCUSDT", &sample_datatypes(), 64);

        let mut rx = registry.subscribe("trade.all.BTCUSDT").unwrap();

        let msg = TopicMessage {
            topic: "trade.all.BTCUSDT".into(),
            received_at_us: 123_456_789,
            exchange: "bybit".into(),
            payload: dummy_event(),
        };

        let n = registry.publish("trade.all.BTCUSDT", msg).unwrap();
        assert_eq!(n, 1);

        let received = rx.try_recv().unwrap();
        assert_eq!(received.topic, "trade.all.BTCUSDT");
        assert_eq!(received.received_at_us, 123_456_789);
    }

    #[test]
    fn publish_to_unknown_topic_returns_error() {
        let registry =
            TopicRegistry::from_config("bybit", "BTCUSDT", &sample_datatypes(), 64);

        let msg = TopicMessage {
            topic: "nonexistent".into(),
            received_at_us: 0,
            exchange: "bybit".into(),
            payload: dummy_event(),
        };

        let result = registry.publish("nonexistent", msg);
        assert!(result.is_err());
    }

    #[test]
    fn domain_registry_carries_only_book_and_trade_topics_with_raw_names() {
        let dt = sample_datatypes(); // orderbook(50) + trades + liquidations
        let raw = TopicRegistry::from_config("bybit", "BTCUSDT", &dt, 64);
        let dom = DomainTopicRegistry::from_config("BTCUSDT", &dt, 64);
        // DomainEvent models only Book|Trade — liquidations are NOT a domain topic.
        assert_eq!(dom.len(), 2);
        assert!(dom.subscribe("orderbook.50.BTCUSDT").is_some());
        assert!(dom.subscribe("trade.all.BTCUSDT").is_some());
        assert!(dom.subscribe("liquidation.all.BTCUSDT").is_none());
        // Every domain topic name is byte-identical to a raw topic name.
        for t in dom.topics() {
            assert!(
                raw.get(t).is_some(),
                "domain topic {t} not present in raw registry"
            );
        }
    }

    #[test]
    fn domain_registry_publish_subscribe() {
        let dt = sample_datatypes();
        let reg = DomainTopicRegistry::from_config("BTCUSDT", &dt, 64);
        let mut rx = reg.subscribe("trade.all.BTCUSDT").unwrap();
        let msg = DomainTopicMessage {
            topic: "trade.all.BTCUSDT".into(),
            received_at_us: 42,
            exchange: "bybit".into(),
            payload: DomainEvent::Trade {
                trade: aetelier_types::trades::Trade::random(),
                sequence: None,
            },
        };
        let n = reg.publish("trade.all.BTCUSDT", msg).unwrap();
        assert_eq!(n, 1);
        let got = rx.try_recv().unwrap();
        assert_eq!(got.topic, "trade.all.BTCUSDT");
        assert_eq!(got.exchange, "bybit");
        assert!(matches!(got.payload, DomainEvent::Trade { .. }));
    }
}
