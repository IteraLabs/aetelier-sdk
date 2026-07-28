//! Per-topic ingestion gap detection.
//!
//! [`GapDetector`] tracks the wall-clock time between consecutive events
//! on a single topic.  When the silence exceeds a configurable threshold,
//! the detector records it as an **ingestion gap metric** — *not* as
//! corrupted data.
//!
//! This distinction is critical: during exchange maintenance windows,
//! network blips, or naturally quiet markets, there is simply no data to
//! ingest.  Downstream consumers must be aware of gaps but should not
//! treat them as data-integrity failures.
//!
//! # Metric name
//!
//! `ingestion_gap_duration_ms` — emitted as a `tracing::warn!` span
//! whenever a gap exceeding the threshold is detected.

use std::time::Duration;

use tokio::time::Instant;

/// Default silence threshold: 5 seconds without events = gap.
pub const DEFAULT_SILENCE_THRESHOLD: Duration = Duration::from_secs(5);

/// Per-topic ingestion gap tracker.
///
/// Call [`record_event()`](Self::record_event) on every received event.
/// If the time since the previous event exceeds `silence_threshold`, the
/// call returns `Some(gap_ms)` and emits a tracing warning.
#[derive(Debug)]
pub struct GapDetector {
    /// Topic this detector is attached to.
    topic: String,
    /// Timestamp of the most recent event (or creation time).
    last_event_at: Option<Instant>,
    /// Minimum silence duration before we consider it a gap.
    silence_threshold: Duration,
    /// Cumulative gap duration in milliseconds.
    total_gap_ms: u64,
    /// Number of distinct gap episodes detected.
    gap_count: u64,
}

impl GapDetector {
    /// Create a new detector for the given topic.
    pub fn new(topic: impl Into<String>, silence_threshold: Duration) -> Self {
        Self {
            topic: topic.into(),
            last_event_at: None,
            silence_threshold,
            total_gap_ms: 0,
            gap_count: 0,
        }
    }

    /// Create a detector with the default 5-second threshold.
    pub fn with_defaults(topic: impl Into<String>) -> Self {
        Self::new(topic, DEFAULT_SILENCE_THRESHOLD)
    }

    /// Record that an event was received.
    ///
    /// Returns `Some(gap_ms)` if the time since the previous event
    /// exceeded the silence threshold, `None` otherwise.
    ///
    /// On the very first call (no previous event), always returns `None`.
    pub fn record_event(&mut self) -> Option<u64> {
        let now = Instant::now();

        let gap = self.last_event_at.and_then(|prev| {
            let elapsed = now.duration_since(prev);
            if elapsed > self.silence_threshold {
                let gap_ms = elapsed.as_millis() as u64;
                self.total_gap_ms += gap_ms;
                self.gap_count += 1;

                tracing::warn!(
                    topic = %self.topic,
                    ingestion_gap_duration_ms = gap_ms,
                    gap_count = self.gap_count,
                    total_gap_ms = self.total_gap_ms,
                    "ingestion.gap_detected"
                );

                Some(gap_ms)
            } else {
                None
            }
        });

        self.last_event_at = Some(now);
        gap
    }

    /// The topic this detector is attached to.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Cumulative gap duration across all detected episodes.
    pub fn total_gap_ms(&self) -> u64 {
        self.total_gap_ms
    }

    /// Number of distinct gap episodes detected.
    pub fn gap_count(&self) -> u64 {
        self.gap_count
    }

    /// Time since the last event (or since creation if no events yet).
    ///
    /// Returns `None` if no event has been recorded.
    pub fn time_since_last_event(&self) -> Option<Duration> {
        self.last_event_at.map(|t| t.elapsed())
    }

    /// The configured silence threshold.
    pub fn silence_threshold(&self) -> Duration {
        self.silence_threshold
    }
}

/// Collection of [`GapDetector`]s keyed by topic name.
///
/// Built alongside a [`TopicRegistry`](super::topic_publisher::TopicRegistry)
/// at `DataWorker` startup.
pub struct GapDetectorSet {
    detectors: std::collections::HashMap<String, GapDetector>,
}

impl GapDetectorSet {
    /// Create detectors for a list of topic names.
    pub fn new(topics: &[&str], silence_threshold: Duration) -> Self {
        let detectors = topics
            .iter()
            .map(|&t| (t.to_string(), GapDetector::new(t, silence_threshold)))
            .collect();
        Self { detectors }
    }

    /// Record an event for the named topic.
    ///
    /// Returns `Some(gap_ms)` if a gap was detected, `None` otherwise.
    /// Returns `None` for unknown topics (no panic).
    pub fn record_event(&mut self, topic: &str) -> Option<u64> {
        self.detectors.get_mut(topic).and_then(|d| d.record_event())
    }

    /// Snapshot of all detector stats.
    pub fn stats(&self) -> Vec<GapStats> {
        self.detectors
            .values()
            .map(|d| GapStats {
                topic: d.topic().to_string(),
                gap_count: d.gap_count(),
                total_gap_ms: d.total_gap_ms(),
            })
            .collect()
    }
}

/// Summary stats for one gap detector.
#[derive(Debug, Clone)]
pub struct GapStats {
    pub topic: String,
    pub gap_count: u64,
    pub total_gap_ms: u64,
}
