//! Operational-envelope data types: per-venue connection/rate budgets,
//! buffer overflow policy, and per-source metrics handles carried by
//! `ExchangeAdapter::spawn` and the venue profile.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// One rate-limit window (e.g. 5 requests per 1s).
#[derive(Debug, Clone, Copy)]
pub struct RateWindow {
    pub max: u32,
    pub per: Duration,
}

/// Per-venue connection / rate envelope. Caps are `Option` — `None` means
/// unknown or uncapped (Upbit/HTX), so a sharding planner must not divide by
/// a missing cap.
#[derive(Debug, Clone, Default)]
pub struct ConnectionBudget {
    pub max_connections: Option<u32>,
    pub max_streams_per_socket: Option<u32>,
    /// Multi-window subscribe rate (Upbit is `[{5,1s},{100,60s}]`).
    pub subscribe_rate: Vec<RateWindow>,
    /// New-connection rate, distinct from concurrent-connection cap
    /// (Bitfinex `20/min` is the binding constraint).
    pub connect_attempt_rate: Option<RateWindow>,
    /// Mandatory reconnect interval (KuCoin token lives 24h).
    pub connection_lifetime: Option<Duration>,
}

/// Bounded-buffer overflow policy. Correctness-aware: book deltas must
/// resync (never silently drop), trades may drop-and-count.
#[derive(Debug, Clone, Copy)]
pub enum BufferOverflow {
    /// Trades: acceptable, counted into metrics.
    DropOldestCount,
    /// Book deltas: overflow breaks continuity → trigger a resync.
    ResyncBook,
    /// Last resort; per-source isolated so it can't stall other venues.
    Block,
}

/// Bounded sizing for the in-process Ingest→Sync stream buffer.
///
/// This is a tokio mpsc queue.
#[derive(Debug, Clone, Copy)]
pub struct StreamBudget {
    pub capacity: usize,
    pub on_overflow: BufferOverflow,
}

/// Per-source observability handle, carried by `spawn`. Cheap to clone — every
/// clone shares the same atomic counters, so the worker holds one handle and the
/// transport, runtime, and normalizer tasks each increment through their clone.
/// Read back with [`SourceMetrics::snapshot`].
#[derive(Debug, Clone, Default)]
pub struct SourceMetrics {
    inner: Arc<SourceMetricsInner>,
}

#[derive(Debug, Default)]
struct SourceMetricsInner {
    conn_epoch_us: AtomicU64,
    msgs: AtomicU64,
    decode_err: AtomicU64,
    dropped_frames: AtomicU64,
    reconnects: AtomicU64,
    gaps: AtomicU64,
    resyncs: AtomicU64,
    checksum_fail: AtomicU64,
    flush_failures: AtomicU64,
    trade_gaps: AtomicU64,
    trades_lost: AtomicU64,
    trade_gap_incidents: AtomicU64,
    trade_gap_window_us: AtomicU64,
    possible_dropped_trades: AtomicU64,
    trade_loss_confidence: AtomicU64,
    book_gap_incidents: AtomicU64,
    book_gap_window_us: AtomicU64,
    deltas_missed_exact: AtomicU64,
    trades_recovered: AtomicU64,
    reconcile_fetches: AtomicU64,
    reconcile_failures: AtomicU64,
    reseed_exhausted: AtomicU64,
    undeclared_dropped: AtomicU64,
    seeds_abandoned: AtomicU64,
    ack_timeouts: AtomicU64,
    replay_duplicates: AtomicU64,
    out_of_order_frames: AtomicU64,
    ingest_backpressure: AtomicU64,
    source_exhausted: AtomicU64,
    gaps_beyond_edge: AtomicU64,
    ver_rejected: AtomicU64,
    integrity_fail: AtomicU64,
    republished: AtomicU64,
}

/// Confidence class of the trade-loss accounting on this source, encoded as a
/// `u64` inside the metrics snapshot (wire-stable).
///
/// `Estimated = 0` is the DEFAULT (the atomic's zero state): the venue cannot
/// be counted from the WSS stream (global/snowflake/sequence-scale ids); loss
/// is signaled by the book-channel sentinel and `possible_dropped_trades` is a
/// labeled rate-model ESTIMATE, never presented as a count. `Exact = 1` — the
/// venue supplies a dense per-stream trade sequence, so `trades_lost` is a
/// real count; the runtime flips this on the first sequenced trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum TradeLossConfidence {
    Estimated = 0,
    Exact = 1,
}

/// Plain-`u64` view of [`SourceMetrics`] for reporting (e.g. into `WorkerStatus`).
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize,
)]
#[serde(default)]
pub struct SourceMetricsSnapshot {
    pub msgs: u64,
    pub decode_err: u64,
    pub dropped_frames: u64,
    pub reconnects: u64,
    pub gaps: u64,
    pub resyncs: u64,
    pub checksum_fail: u64,
    pub flush_failures: u64,
    /// Trade-continuity breaks (each may cover many lost prints).
    #[serde(default)]
    pub trade_gaps: u64,
    /// Prints permanently lost, summed from venue trade-sequence jumps.
    #[serde(default)]
    pub trades_lost: u64,
    /// Sentinel: suspicion incidents opened for possible trade loss
    /// (book-channel gap on the socket carrying trades).
    #[serde(default)]
    pub trade_gap_incidents: u64,
    /// Sentinel: cumulative µs spent inside trade-suspicion windows.
    #[serde(default)]
    pub trade_gap_window_us: u64,
    /// Sentinel ESTIMATE of prints possibly dropped during suspicion windows
    /// (trailing trade-rate × window). Never a count — see
    /// `trade_loss_confidence`.
    #[serde(default)]
    pub possible_dropped_trades: u64,
    /// [`TradeLossConfidence`] as u64: 0 = estimated (sentinel only — the
    /// default), 1 = exact (dense sequence armed, flipped on first
    /// sequenced trade).
    #[serde(default)]
    pub trade_loss_confidence: u64,
    /// Coverage ledger: book gap incidents (continuity break → resync-complete).
    #[serde(default)]
    pub book_gap_incidents: u64,
    /// Coverage ledger: cumulative µs the book spent gapped/reconnecting.
    #[serde(default)]
    pub book_gap_window_us: u64,
    /// Book deltas provably dropped, where the venue's sequencing makes the
    /// count exact (range/exact-prev arithmetic, connection-gap counts).
    #[serde(default)]
    pub deltas_missed_exact: u64,
    /// Live reconciliation: prints recovered from the venue REST endpoint and
    /// injected into the stream with `origin = rest`.
    #[serde(default)]
    pub trades_recovered: u64,
    /// Live reconciliation: REST fetch calls made (the metered unit).
    #[serde(default)]
    pub reconcile_fetches: u64,
    /// Live reconciliation: fetches that errored (collection unaffected).
    #[serde(default)]
    pub reconcile_failures: u64,
    /// Reseed budget (MAX_RESEEDS) exhaustions that escalated to a socket
    /// reconnect — the dedicated alarm distinguishing a persistently failing
    /// seed path from ordinary gap-recovery resyncs (which also bump
    /// `resyncs`). A steadily climbing value means the venue's seed source is
    /// unhealthy, not the stream.
    #[serde(default)]
    pub reseed_exhausted: u64,
    #[serde(default)]
    pub undeclared_dropped: u64,
    /// Seed fetches still in flight when the runtime exited on a
    /// shutdown/resync path — abandoned, never applied. Bounds the "seed was
    /// requested but no book appeared" ambiguity.
    #[serde(default)]
    pub seeds_abandoned: u64,
    #[serde(default)]
    pub ack_timeouts: u64,
    #[serde(default)]
    pub replay_duplicates: u64,
    #[serde(default)]
    pub out_of_order_frames: u64,
    #[serde(default)]
    pub ingest_backpressure: u64,
    /// 1 when a finite source reported `TaskExit::Exhausted`: everything it
    /// had was delivered and the worker ended as a terminal success.
    #[serde(default)]
    pub source_exhausted: u64,
    #[serde(default)]
    pub gaps_beyond_edge: u64,
    #[serde(default)]
    pub ver_rejected: u64,
    #[serde(default)]
    pub integrity_fail: u64,
    #[serde(default)]
    pub republished: u64,
}

impl SourceMetrics {
    /// Mint the identity of a socket that has just CONNECTED: Unix
    /// microseconds, the platform timestamp unit. Called by the transport at
    /// the moment the handshake completes, so the value is when the socket
    /// opened — not when a connect was attempted.
    ///
    /// Guarded monotonic per feed (`max(now, last + 1)`): at microsecond
    /// resolution two connects cannot realistically share an instant, so the
    /// guard is insurance against a clock stepping backwards (NTP), not a
    /// granularity patch. Held on the shared handle, so it outlives the
    /// per-connection task and stays monotonic for the feed's lifetime.
    pub fn next_conn_epoch_us(&self) -> u64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        let mut prev = self.inner.conn_epoch_us.load(Ordering::Relaxed);
        loop {
            let next = now.max(prev.saturating_add(1));
            match self.inner.conn_epoch_us.compare_exchange_weak(
                prev,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return next,
                Err(observed) => prev = observed,
            }
        }
    }

    /// The identity of the socket currently open on this source; `0` before
    /// the first connect completes.
    pub fn conn_epoch_us(&self) -> u64 {
        self.inner.conn_epoch_us.load(Ordering::Relaxed)
    }

    /// One decoded wire frame reached the normalizer.
    pub fn bump_msgs(&self) {
        self.inner.msgs.fetch_add(1, Ordering::Relaxed);
    }
    /// A frame failed to inflate/decode and was skipped.
    pub fn bump_decode_err(&self) {
        self.inner.decode_err.fetch_add(1, Ordering::Relaxed);
    }
    /// `n` events were dropped before emission (e.g. an unparseable trade side).
    pub fn add_dropped_frames(&self, n: u64) {
        self.inner.dropped_frames.fetch_add(n, Ordering::Relaxed);
    }
    pub fn bump_ack_timeouts(&self) {
        self.inner.ack_timeouts.fetch_add(1, Ordering::Relaxed);
    }
    pub fn bump_replay_duplicates(&self) {
        self.inner.replay_duplicates.fetch_add(1, Ordering::Relaxed);
    }
    pub fn bump_out_of_order_frames(&self) {
        self.inner
            .out_of_order_frames
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn bump_ingest_backpressure(&self) {
        self.inner
            .ingest_backpressure
            .fetch_add(1, Ordering::Relaxed);
    }
    /// The transport reconnected (one reconnect-loop iteration).
    pub fn bump_reconnects(&self) {
        self.inner.reconnects.fetch_add(1, Ordering::Relaxed);
    }
    /// A book continuity gap was detected.
    pub fn bump_gaps(&self) {
        self.inner.gaps.fetch_add(1, Ordering::Relaxed);
    }
    /// A resync (reseed / reconnect-to-reseed) was triggered.
    pub fn bump_resyncs(&self) {
        self.inner.resyncs.fetch_add(1, Ordering::Relaxed);
    }
    /// A CRC32 book checksum mismatched after applying a delta.
    pub fn bump_checksum_fail(&self) {
        self.inner.checksum_fail.fetch_add(1, Ordering::Relaxed);
    }
    /// A parquet flush batch failed (retained for retry).
    pub fn bump_flush_failures(&self) {
        self.inner.flush_failures.fetch_add(1, Ordering::Relaxed);
    }
    /// The source reported `TaskExit::Exhausted` — finite input fully
    /// delivered; latches to 1 and never resets.
    pub fn mark_source_exhausted(&self) {
        self.inner.source_exhausted.store(1, Ordering::Relaxed);
    }

    pub fn bump_gaps_beyond_edge(&self) {
        self.inner.gaps_beyond_edge.fetch_add(1, Ordering::Relaxed);
    }

    pub fn bump_ver_rejected(&self) {
        self.inner.ver_rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn bump_integrity_fail(&self) {
        self.inner.integrity_fail.fetch_add(1, Ordering::Relaxed);
    }

    pub fn bump_republished(&self) {
        self.inner.republished.fetch_add(1, Ordering::Relaxed);
    }

    /// One trade-continuity break was observed on a venue trade sequence.
    pub fn bump_trade_gaps(&self) {
        self.inner.trade_gaps.fetch_add(1, Ordering::Relaxed);
    }

    /// `missed` prints were permanently lost at a trade-continuity break.
    pub fn add_trades_lost(&self, missed: u64) {
        self.inner.trades_lost.fetch_add(missed, Ordering::Relaxed);
    }

    /// A trade-suspicion window closed: one sentinel incident spanning
    /// `window_us`, with `estimate` possibly-dropped prints (rate model).
    pub fn record_trade_suspicion(&self, window_us: u64, estimate: u64) {
        self.inner
            .trade_gap_incidents
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .trade_gap_window_us
            .fetch_add(window_us, Ordering::Relaxed);
        self.inner
            .possible_dropped_trades
            .fetch_add(estimate, Ordering::Relaxed);
    }

    /// Declare this source's trade-loss confidence class (set once at spawn
    /// from the venue's arming status).
    pub fn set_trade_loss_confidence(&self, c: TradeLossConfidence) {
        self.inner
            .trade_loss_confidence
            .store(c as u64, Ordering::Relaxed);
    }

    /// A book gap incident closed (continuity break → resync complete),
    /// spanning `window_us`. Recorded by the worker, which measures the
    /// gap-to-recovery wall clock across the reconnect.
    pub fn record_book_gap_incident(&self, window_us: u64) {
        self.inner
            .book_gap_incidents
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .book_gap_window_us
            .fetch_add(window_us, Ordering::Relaxed);
    }

    /// `n` book messages were provably dropped (exact — dense per-book or
    /// connection-counter arithmetic). Recorded at detection by the runtime.
    pub fn add_deltas_missed(&self, n: u64) {
        self.inner
            .deltas_missed_exact
            .fetch_add(n, Ordering::Relaxed);
    }

    /// Live reconciliation counters.
    pub fn add_trades_recovered(&self, n: u64) {
        self.inner.trades_recovered.fetch_add(n, Ordering::Relaxed);
    }
    pub fn bump_reconcile_fetches(&self) {
        self.inner.reconcile_fetches.fetch_add(1, Ordering::Relaxed);
    }
    pub fn bump_reconcile_failures(&self) {
        self.inner
            .reconcile_failures
            .fetch_add(1, Ordering::Relaxed);
    }
    /// A reseed budget exhausted and escalated to reconnect (also counted in
    /// `resyncs`; this is the dedicated exhaustion alarm).
    pub fn bump_undeclared_dropped(&self) {
        self.inner
            .undeclared_dropped
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn bump_reseed_exhausted(&self) {
        self.inner.reseed_exhausted.fetch_add(1, Ordering::Relaxed);
    }
    /// `n` in-flight seed fetches were abandoned by a runtime exit.
    pub fn add_seeds_abandoned(&self, n: u64) {
        self.inner.seeds_abandoned.fetch_add(n, Ordering::Relaxed);
    }

    /// Read all counters as a plain-`u64` snapshot.
    pub fn snapshot(&self) -> SourceMetricsSnapshot {
        let i = &self.inner;
        SourceMetricsSnapshot {
            msgs: i.msgs.load(Ordering::Relaxed),
            decode_err: i.decode_err.load(Ordering::Relaxed),
            dropped_frames: i.dropped_frames.load(Ordering::Relaxed),
            reconnects: i.reconnects.load(Ordering::Relaxed),
            gaps: i.gaps.load(Ordering::Relaxed),
            resyncs: i.resyncs.load(Ordering::Relaxed),
            checksum_fail: i.checksum_fail.load(Ordering::Relaxed),
            flush_failures: i.flush_failures.load(Ordering::Relaxed),
            trade_gaps: i.trade_gaps.load(Ordering::Relaxed),
            trades_lost: i.trades_lost.load(Ordering::Relaxed),
            trade_gap_incidents: i.trade_gap_incidents.load(Ordering::Relaxed),
            trade_gap_window_us: i.trade_gap_window_us.load(Ordering::Relaxed),
            possible_dropped_trades: i.possible_dropped_trades.load(Ordering::Relaxed),
            trade_loss_confidence: i.trade_loss_confidence.load(Ordering::Relaxed),
            book_gap_incidents: i.book_gap_incidents.load(Ordering::Relaxed),
            book_gap_window_us: i.book_gap_window_us.load(Ordering::Relaxed),
            deltas_missed_exact: i.deltas_missed_exact.load(Ordering::Relaxed),
            trades_recovered: i.trades_recovered.load(Ordering::Relaxed),
            reconcile_fetches: i.reconcile_fetches.load(Ordering::Relaxed),
            reconcile_failures: i.reconcile_failures.load(Ordering::Relaxed),
            reseed_exhausted: i.reseed_exhausted.load(Ordering::Relaxed),
            undeclared_dropped: i.undeclared_dropped.load(Ordering::Relaxed),
            seeds_abandoned: i.seeds_abandoned.load(Ordering::Relaxed),
            ack_timeouts: i.ack_timeouts.load(Ordering::Relaxed),
            replay_duplicates: i.replay_duplicates.load(Ordering::Relaxed),
            out_of_order_frames: i.out_of_order_frames.load(Ordering::Relaxed),
            ingest_backpressure: i.ingest_backpressure.load(Ordering::Relaxed),
            source_exhausted: i.source_exhausted.load(Ordering::Relaxed),
            gaps_beyond_edge: i.gaps_beyond_edge.load(Ordering::Relaxed),
            ver_rejected: i.ver_rejected.load(Ordering::Relaxed),
            integrity_fail: i.integrity_fail.load(Ordering::Relaxed),
            republished: i.republished.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn conn_epoch_us_advances_when_two_sockets_open_in_the_same_instant() {
        let m = SourceMetrics::default();
        let first = m.next_conn_epoch_us();
        let second = m.next_conn_epoch_us();
        let third = m.next_conn_epoch_us();
        assert!(second > first, "{second} must exceed {first}");
        assert!(third > second, "{third} must exceed {second}");
    }

    #[test]
    fn conn_epoch_us_is_unset_before_the_first_connect() {
        let m = SourceMetrics::default();
        assert_eq!(m.conn_epoch_us(), 0);
        let minted = m.next_conn_epoch_us();
        assert_eq!(m.conn_epoch_us(), minted);
    }

    #[test]
    fn conn_epoch_us_starts_from_wall_clock_micros() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let minted = SourceMetrics::default().next_conn_epoch_us();
        assert!(minted >= now, "{minted} must be at or after {now}");
    }

    #[test]
    fn conn_epoch_us_clones_share_one_identity() {
        let m = SourceMetrics::default();
        let clone = m.clone();
        let first = m.next_conn_epoch_us();
        let second = clone.next_conn_epoch_us();
        assert!(second > first);
        assert_eq!(m.conn_epoch_us(), second);
    }
    use super::*;

    #[test]
    fn clones_share_counters_and_snapshot_reads_them() {
        let a = SourceMetrics::default();
        let b = a.clone();
        a.bump_reconnects();
        b.bump_reconnects();
        b.add_dropped_frames(3);
        a.bump_flush_failures();
        let s = a.snapshot();
        assert_eq!(s.reconnects, 2, "clones must share the same atomics");
        assert_eq!(s.dropped_frames, 3);
        assert_eq!(s.flush_failures, 1);
        assert_eq!(s.msgs, 0);
        assert_eq!(a.snapshot(), b.snapshot());
    }
}
