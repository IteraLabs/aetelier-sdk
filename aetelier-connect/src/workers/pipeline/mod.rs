//! Composable event pipeline between [`IngestionCore`] and workers.
//!
//! The pipeline transforms or filters [`TopicMessage`]s flowing from
//! the ingestion layer to the worker event loop.  For most exchanges
//! the pipeline is [`PassthroughPipeline`] (identity).  For exchanges
//! that require multi-source coordination (e.g. Binance REST snapshot
//! + WSS deltas) a specialised stage is interposed.
//!
//! # Architecture
//!
//! ```text
//! IngestionCore ──mpsc──▶ EventPipeline ──mpsc──▶ Worker
//! ```

pub mod book_initializer;

use tokio::sync::{mpsc, watch};

use crate::sources::binance::rest::BinanceRestClient;
use aetelier_types::config::markets::market_config::DataTypesSection;
use aetelier_types::exchanges::Exchange;

use super::topic_publisher::TopicMessage;
use book_initializer::BookInitializer;

// ─────────────────────────────────────────────────────────────────────────────
// EventPipeline trait
// ─────────────────────────────────────────────────────────────────────────────

/// A composable transform stage in the event pipeline.
///
/// Implementations consume [`TopicMessage`]s from `input` and produce
/// (possibly transformed) messages to `output`.
#[async_trait::async_trait]
pub trait EventPipeline: Send + 'static {
    /// Run the pipeline until the input channel closes or shutdown fires.
    async fn run(
        self: Box<Self>,
        input: mpsc::Receiver<TopicMessage>,
        output: mpsc::Sender<TopicMessage>,
        shutdown: watch::Receiver<bool>,
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// PassthroughPipeline
// ─────────────────────────────────────────────────────────────────────────────

/// Identity pipeline — forwards all events unchanged.
///
/// Used for Bybit, Coinbase, Kraken, and any exchange that does not
/// require multi-source coordination.
pub struct PassthroughPipeline;

#[async_trait::async_trait]
impl EventPipeline for PassthroughPipeline {
    async fn run(
        self: Box<Self>,
        mut input: mpsc::Receiver<TopicMessage>,
        output: mpsc::Sender<TopicMessage>,
        _shutdown: watch::Receiver<bool>,
    ) {
        while let Some(msg) = input.recv().await {
            if output.send(msg).await.is_err() {
                break;
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Factory
// ─────────────────────────────────────────────────────────────────────────────

/// Construct the appropriate pipeline for the given exchange.
///
/// Returns a `PassthroughPipeline` for all exchanges except Binance
/// (when orderbooks are enabled), which gets a `BookInitializer`.
pub fn build_pipeline(
    exchange: Exchange,
    symbol: &str,
    datatypes: &DataTypesSection,
) -> Box<dyn EventPipeline> {
    match exchange {
        Exchange::Binance if datatypes.orderbook.enabled => {
            let rest = BinanceRestClient::new("https://api.binance.com");
            let ob_topic = format!("orderbook.{}.{}", datatypes.orderbook.depth, symbol);
            Box::new(BookInitializer::new(rest, symbol.to_string(), ob_topic))
        }
        _ => Box::new(PassthroughPipeline),
    }
}
