//! Shared OpenTelemetry attribute keys for the aetelier-sdk engine.
//!
//! These constants define the dimensional labels attached to every metric,
//! span, and log record.  Using shared constants ensures consistency across
//! all instrumentation points and prevents typo-induced cardinality explosions.
//!
//! # Cardinality Guidelines
//!
//! - **Low cardinality** (safe on metrics): `EXCHANGE`, `SYMBOL`, `MARKET_TYPE`,
//!   `WORKER_ID`, `TOPIC`, `SINK_NAME`.
//! - **High cardinality** (spans/logs only, never on metrics): trade IDs,
//!   update IDs, order timestamps, raw prices.

use opentelemetry::KeyValue;

// ─────────────────────────────────────────────────────────────────────────────
// Attribute key constants
// ─────────────────────────────────────────────────────────────────────────────

/// Exchange identifier (e.g. `"bybit"`, `"kraken"`).
pub const EXCHANGE: &str = "exchange";

/// Trading pair / instrument symbol (e.g. `"BTCUSDT"`).
pub const SYMBOL: &str = "symbol";

/// Market type: `"spot"`, `"perpetual"`, or `"inverse"`.
pub const MARKET_TYPE: &str = "market_type";

/// Unique worker identifier (e.g. `"bybit:BTCUSDT:0"`).
pub const WORKER_ID: &str = "worker_id";

/// Canonical topic name (e.g. `"orderbook.50.BTCUSDT"`, `"trade.all.BTCUSDT"`).
pub const TOPIC: &str = "topic";

/// Output sink name (e.g. `"channel"`, `"terminal"`, `"buffered"`).
pub const SINK_NAME: &str = "sink_name";

// ─────────────────────────────────────────────────────────────────────────────
// Topic category helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Topic category for dashboard metric filtering.
///
/// Derived from the canonical topic name:
/// - `"orderbook.*"` → `"orderbook"`
/// - `"trade.*"` / `"publicTrade.*"` → `"trade"`
/// - `"liquidation.*"` → `"liquidation"`
/// - `"funding.*"` → `"funding"`
/// - `"open_interest.*"` → `"open_interest"`
pub const TOPIC_CATEGORY: &str = "topic_category";

/// Extract the topic category from a canonical topic name.
///
/// ```
/// use aetelier_telemetry::attributes::topic_category;
/// assert_eq!(topic_category("orderbook.50.BTCUSDT"), "orderbook");
/// assert_eq!(topic_category("trade.all.BTCUSDT"), "trade");
/// assert_eq!(topic_category("funding.all.ETHUSDT"), "funding");
/// ```
pub fn topic_category(topic: &str) -> &str {
    if topic.starts_with("orderbook") {
        "orderbook"
    } else if topic.starts_with("trade") || topic.starts_with("publicTrade") {
        "trade"
    } else if topic.starts_with("liquidation") {
        "liquidation"
    } else if topic.starts_with("funding") {
        "funding"
    } else if topic.starts_with("open_interest") {
        "open_interest"
    } else {
        "unknown"
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// KeyValue builders
// ─────────────────────────────────────────────────────────────────────────────

/// Build the common per-event attribute set used on every counter/histogram
/// observation.
///
/// This produces a small, fixed-cardinality attribute set suitable for
/// metric dimensions.
pub fn event_attributes(
    exchange: &str,
    symbol: &str,
    worker_id: &str,
    topic: &str,
) -> Vec<KeyValue> {
    vec![
        KeyValue::new(EXCHANGE, exchange.to_string()),
        KeyValue::new(SYMBOL, symbol.to_string()),
        KeyValue::new(WORKER_ID, worker_id.to_string()),
        KeyValue::new(TOPIC, topic.to_string()),
        KeyValue::new(TOPIC_CATEGORY, topic_category(topic).to_string()),
    ]
}

/// Build the per-worker attribute set used on gauges and worker-level metrics.
pub fn worker_attributes(
    exchange: &str,
    symbol: &str,
    market_type: &str,
    worker_id: &str,
) -> Vec<KeyValue> {
    vec![
        KeyValue::new(EXCHANGE, exchange.to_string()),
        KeyValue::new(SYMBOL, symbol.to_string()),
        KeyValue::new(MARKET_TYPE, market_type.to_string()),
        KeyValue::new(WORKER_ID, worker_id.to_string()),
    ]
}
