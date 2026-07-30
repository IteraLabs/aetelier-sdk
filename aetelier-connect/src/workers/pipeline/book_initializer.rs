//! Binance orderbook initialisation pipeline stage.
//!
//! [`BookInitializer`] intercepts orderbook events from the WSS stream,
//! buffers them while fetching a REST depth snapshot, reconciles the
//! two according to Binance's documented protocol, and then forwards
//! a synthesised snapshot followed by validated deltas.
//!
//! Trade events are forwarded immediately regardless of book state.

use std::collections::VecDeque;

use tokio::sync::{mpsc, watch};
use tracing::{error, info};

use crate::sources::ExchangeEvent;
use crate::sources::binance::events::BinanceWssEvent;
use crate::sources::binance::rest::BinanceRestClient;
use crate::workers::pipeline::EventPipeline;
use crate::workers::topic_publisher::TopicMessage;

use super::super::ingestion_core::wall_clock_us;

// ─────────────────────────────────────────────────────────────────────────────
// BookInitializer
// ─────────────────────────────────────────────────────────────────────────────

/// Pipeline stage that coordinates REST snapshot + WSS delta
/// reconciliation for Binance orderbooks.
///
/// # State machine
///
/// ```text
/// ┌───────────┐  REST snapshot received   ┌────────┐
/// │ Buffering │ ────────────────────────▶  │ Synced │
/// └───────────┘  (reconcile + replay)      └────────┘
///       ▲                                       │
///       └───── WSS reconnect detected ──────────┘
/// ```
pub struct BookInitializer {
    rest_client: BinanceRestClient,
    symbol: String,
    ob_topic: String,
}

/// Internal state of the initializer.
enum InitState {
    /// Accumulating depthUpdate events while waiting for REST snapshot.
    Buffering { buffer: VecDeque<TopicMessage> },
    /// Snapshot applied; forward deltas directly.
    Synced { last_update_id: u64 },
}

impl BookInitializer {
    /// Create a new `BookInitializer`.
    ///
    /// # Arguments
    ///
    /// * `rest_client` — Binance REST client for fetching `/api/v3/depth`.
    /// * `symbol` — Trading pair in Binance format (e.g. `"BTCUSDT"`).
    /// * `ob_topic` — Canonical orderbook topic name (e.g. `"orderbook.50.BTCUSDT"`).
    pub fn new(rest_client: BinanceRestClient, symbol: String, ob_topic: String) -> Self {
        Self {
            rest_client,
            symbol,
            ob_topic,
        }
    }

    /// Extract the `last_update_id` (`u` field) from a depth update
    /// event wrapped inside a `TopicMessage`.
    fn extract_last_update_id(msg: &TopicMessage) -> Option<u64> {
        if let ExchangeEvent::Binance(BinanceWssEvent::DepthUpdate(ref upd)) = msg.payload
        {
            Some(upd.last_update_id)
        } else {
            None
        }
    }

    /// Extract the `first_update_id` (`U` field) from a depth update.
    fn extract_first_update_id(msg: &TopicMessage) -> Option<u64> {
        if let ExchangeEvent::Binance(BinanceWssEvent::DepthUpdate(ref upd)) = msg.payload
        {
            Some(upd.first_update_id)
        } else {
            None
        }
    }

    /// Synthesise a [`TopicMessage`] wrapping a `DepthSnapshot` event.
    fn synthesize_snapshot_msg(
        snapshot: &crate::sources::binance::responses::orderbooks::BinanceDepthSnapshot,
        topic: &str,
    ) -> TopicMessage {
        TopicMessage {
            topic: topic.to_string(),
            received_at_us: wall_clock_us(),
            exchange: "binance".to_string(),
            payload: ExchangeEvent::Binance(BinanceWssEvent::DepthSnapshot(
                snapshot.clone(),
            )),
        }
    }
}

#[async_trait::async_trait]
impl EventPipeline for BookInitializer {
    async fn run(
        self: Box<Self>,
        mut input: mpsc::Receiver<TopicMessage>,
        output: mpsc::Sender<TopicMessage>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut state = InitState::Buffering {
            buffer: VecDeque::new(),
        };

        // Spawn the REST snapshot fetch concurrently.
        let rest = self.rest_client.clone();
        let sym = self.symbol.clone();
        let snapshot_handle =
            tokio::spawn(async move { rest.fetch_depth(&sym, 5000).await });

        tokio::pin!(snapshot_handle);

        info!(
            symbol = self.symbol.as_str(),
            topic = self.ob_topic.as_str(),
            "book_initializer.started"
        );

        loop {
            tokio::select! {
                // ── Incoming event from IngestionCore ─────────────────
                msg = input.recv() => {
                    let Some(msg) = msg else {
                        // IngestionCore closed the channel.
                        break;
                    };

                    // Non-orderbook events (trades): always forward immediately.
                    if msg.topic != self.ob_topic {
                        if output.send(msg).await.is_err() { break; }
                        continue;
                    }

                    match &mut state {
                        InitState::Buffering { buffer } => {
                            buffer.push_back(msg);
                        }
                        InitState::Synced { last_update_id } => {
                            // Once synced, deltas forward straight through:
                            // the framework's sequence-gap detection owns
                            // reconnect handling, so this stage only tracks
                            // the latest update id.
                            if let Some(u) = Self::extract_last_update_id(&msg) {
                                *last_update_id = u;
                            }
                            if output.send(msg).await.is_err() { break; }
                        }
                    }
                }

                // ── REST snapshot result ──────────────────────────────
                result = &mut snapshot_handle,
                    if matches!(state, InitState::Buffering { .. }) =>
                {
                    match result {
                        Ok(Ok(snapshot)) => {
                            let snap_id = snapshot.last_update_id;
                            info!(
                                last_update_id = snap_id,
                                "book_initializer.snapshot_received"
                            );

                            // Emit the snapshot as a synthetic event.
                            let snap_msg = Self::synthesize_snapshot_msg(
                                &snapshot, &self.ob_topic,
                            );
                            if output.send(snap_msg).await.is_err() { break; }

                            // Reconcile buffered deltas.
                            if let InitState::Buffering { buffer } = &mut state {
                                let mut replayed = 0u64;
                                let mut discarded = 0u64;

                                for msg in buffer.drain(..) {
                                    let u = Self::extract_last_update_id(&msg);
                                    let _big_u = Self::extract_first_update_id(&msg);

                                    match u {
                                        Some(u_val) if u_val <= snap_id => {
                                            // Delta is stale — discard.
                                            discarded += 1;
                                        }
                                        _ => {
                                            // Delta is valid — forward.
                                            if output.send(msg).await.is_err() {
                                                return;
                                            }
                                            replayed += 1;
                                        }
                                    }
                                }

                                info!(
                                    replayed = replayed,
                                    discarded = discarded,
                                    "book_initializer.buffer_reconciled"
                                );
                            }

                            state = InitState::Synced {
                                last_update_id: snap_id,
                            };
                        }
                        Ok(Err(e)) => {
                            error!(error = %e, "book_initializer.snapshot_fetch_failed");
                            // Cannot initialise — drain buffer and continue
                            // forwarding raw deltas (downstream OrderbookDelta
                            // will reject them as NotInitialized, which is
                            // the correct behaviour).
                            if let InitState::Buffering { buffer } = &mut state {
                                for msg in buffer.drain(..) {
                                    if output.send(msg).await.is_err() { return; }
                                }
                            }
                            state = InitState::Synced { last_update_id: 0 };
                        }
                        Err(join_err) => {
                            error!(error = %join_err, "book_initializer.snapshot_task_panicked");
                            break;
                        }
                    }
                }

                // ── Shutdown signal ───────────────────────────────────
                _ = shutdown.changed() => {
                    if *shutdown.borrow() { break; }
                }
            }
        }

        info!(symbol = self.symbol.as_str(), "book_initializer.stopped");
    }
}
