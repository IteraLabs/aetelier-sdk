//! The generic Ingest→Sync drive loop shared by every adapter's `spawn`.
//!
//! One connection's lifecycle: the [`WssTransport`] decodes `D::Event` → the
//! venue [`Normalizer`] maps each to `DomainEvent` → the caller's `tx`. Graceful
//! shutdown aborts the socket and best-effort flushes what is buffered; the
//! transport's [`WssExitReason`] is mapped onto a [`TaskExit`] terminal.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{mpsc, watch};
use tokio::task::JoinError;

use super::budget::SourceMetrics;
use super::model::{DomainEvent, Normalizer};
use super::protocol::ProtocolHooks;
use super::registry::TaskExit;
use super::transport::WssTransport;
use crate::clients::disconnect::{DisconnectReason, WssExitReason};
use crate::clients::wss::WssDecoder;

/// Default in-process Ingest→Sync buffer depth for one connection.
pub const DEFAULT_RAW_BUFFER: usize = 4096;

/// Bound on the graceful-shutdown drain flush.
///
/// Derivation: the legitimate worst case is CPU-bound (a full
/// [`DEFAULT_RAW_BUFFER`] of already-decoded events through normalize+stamp,
/// well under 50 ms); the only slow path is a stuck-but-alive downstream
/// consumer, which is exactly the pathology this bound exists to cut. 2 s is
/// ~40x the legitimate worst case and leaves the rest of the process kill
/// grace (docker default 10 s) for the final sink flush and teardown.
pub const STOP_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Drive one venue connection end-to-end. `N::Event` must equal `D::Event`
/// (the normalizer consumes exactly what the decoder yields). `metrics` is the
/// worker's shared counter handle — the transport bumps msgs/decode/drop
/// counters through its clone.
pub async fn drive<H, D, N>(
    hooks: Arc<H>,
    symbols: Vec<String>,
    normalizer: N,
    tx: mpsc::Sender<DomainEvent>,
    mut shutdown: watch::Receiver<bool>,
    buffer: usize,
    metrics: SourceMetrics,
) -> TaskExit
where
    H: ProtocolHooks,
    D: WssDecoder,
    N: Normalizer<Event = D::Event>,
{
    // Internal Ingest buffer: transport decodes D::Event → here; this loop
    // normalizes → DomainEvent → the caller's `tx`.
    let (raw_tx, mut raw_rx) = mpsc::channel::<(D::Event, u64)>(buffer);
    let rtt_us = Arc::new(AtomicU64::new(0));
    let transport = WssTransport::<H, D>::new(hooks, symbols);
    let transport_task = tokio::spawn(transport.run(raw_tx, rtt_us.clone(), metrics));

    loop {
        tokio::select! {
            // Graceful shutdown: abort the socket, then flush what is already
            // buffered, bounded by STOP_DRAIN_TIMEOUT — a drain that exceeds
            // the bound (stuck downstream consumer) exits DrainTimedOut and
            // the outstanding artifacts are the caller's `partial` outcome.
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    transport_task.abort();
                    let flush = drain_buffered(&mut raw_rx, &normalizer, &tx, &rtt_us);
                    return match tokio::time::timeout(STOP_DRAIN_TIMEOUT, flush).await {
                        Ok(()) => TaskExit::Completed,
                        Err(_) => TaskExit::DrainTimedOut,
                    };
                }
            }
            maybe = raw_rx.recv() => match maybe {
                Some((ev, receipt_us)) => {
                    let rtt = rtt_us.load(Ordering::Relaxed);
                    for mut de in normalizer.normalize(ev) {
                        de.stamp_local(receipt_us, rtt);
                        if tx.send(de).await.is_err() {
                            // Sync end dropped — intentional shutdown.
                            transport_task.abort();
                            return TaskExit::Completed;
                        }
                    }
                }
                // Transport closed its sender: await the exit reason and map it.
                None => return exit_to_task(transport_task.await),
            },
        }
    }
}

/// Flush every already-buffered event through the normalizer to the caller.
///
/// Non-blocking on the receive side (`try_recv` over what is buffered); the
/// only await is the downstream `tx.send`, which is why the caller bounds
/// this with [`STOP_DRAIN_TIMEOUT`].
async fn drain_buffered<E, N>(
    raw_rx: &mut mpsc::Receiver<(E, u64)>,
    normalizer: &N,
    tx: &mpsc::Sender<DomainEvent>,
    rtt_us: &AtomicU64,
) where
    E: Send + 'static,
    N: Normalizer<Event = E>,
{
    while let Ok((ev, receipt_us)) = raw_rx.try_recv() {
        let rtt = rtt_us.load(Ordering::Relaxed);
        for mut de in normalizer.normalize(ev) {
            de.stamp_local(receipt_us, rtt);
            if tx.send(de).await.is_err() {
                return;
            }
        }
    }
}

/// Map a finished transport run onto the terminal outcome.
/// A clean server close / caller-driven shutdown is a natural completion;
/// everything else surfaces for the worker's reconnect policy.
pub fn exit_to_task(joined: Result<WssExitReason, JoinError>) -> TaskExit {
    match joined {
        Ok(reason) => match DisconnectReason::from(reason) {
            DisconnectReason::CleanClose | DisconnectReason::ReceiverDropped => {
                TaskExit::Completed
            }
            other => TaskExit::Failed(other),
        },
        Err(_) => TaskExit::Failed(DisconnectReason::TransportError {
            source: "transport task panicked".into(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aetelier_types::TradeSide;
    use aetelier_types::orderbooks::f64_to_decimal;
    use aetelier_types::trades::Trade;
    use aetelier_types::trading_pair::TradingPair;

    struct StubNormalizer;

    impl Normalizer for StubNormalizer {
        type Event = u64;

        fn normalize(&self, event: Self::Event) -> Vec<DomainEvent> {
            vec![DomainEvent::Trade {
                trade: Trade {
                    source_trade_ts_us: event,
                    local_trade_ts_us: 0,
                    source_trade_rtt_us: 0,
                    pair: TradingPair::new("BTC", "USDT"),
                    side: TradeSide::Buy,
                    amount: f64_to_decimal(1.0),
                    price: f64_to_decimal(1.0),
                    exchange: "stub".to_string(),
                    id: event.to_string(),
                    origin: Default::default(),
                },
                sequence: Some(event),
            }]
        }
    }

    #[tokio::test]
    async fn drain_flushes_buffered_events_and_completes() {
        let (raw_tx, mut raw_rx) = mpsc::channel::<(u64, u64)>(16);
        let (tx, mut rx) = mpsc::channel::<DomainEvent>(16);
        let rtt = AtomicU64::new(0);
        for i in 0..5u64 {
            raw_tx.try_send((i, i)).unwrap();
        }

        let flush = drain_buffered(&mut raw_rx, &StubNormalizer, &tx, &rtt);
        tokio::time::timeout(STOP_DRAIN_TIMEOUT, flush)
            .await
            .expect("drain of a small buffer finishes well inside the bound");

        let mut got = 0;
        while rx.try_recv().is_ok() {
            got += 1;
        }
        assert_eq!(got, 5);
    }

    #[tokio::test(start_paused = true)]
    async fn stuck_consumer_hits_the_drain_bound() {
        let (raw_tx, mut raw_rx) = mpsc::channel::<(u64, u64)>(16);
        // Downstream capacity 1, pre-filled, receiver alive but never
        // consuming — the send inside the drain parks forever.
        let (tx, _rx) = mpsc::channel::<DomainEvent>(1);
        let rtt = AtomicU64::new(0);
        tx.try_send(StubNormalizer.normalize(99).pop().unwrap())
            .unwrap();
        raw_tx.try_send((1, 1)).unwrap();

        let flush = drain_buffered(&mut raw_rx, &StubNormalizer, &tx, &rtt);
        let out = tokio::time::timeout(STOP_DRAIN_TIMEOUT, flush).await;
        assert!(out.is_err(), "stuck consumer must trip the drain bound");
    }
}
