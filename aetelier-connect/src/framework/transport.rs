//! Generic WSS transport loop.
//!
//! `WssTransport<H, D>` is monomorphized per venue (static dispatch on
//! `D::decode`) and returns [`WssExitReason`], so it feeds
//! `ReconnectPolicy`/`ConnectionManager`.

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use tokio::sync::{Mutex, mpsc, watch};
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info, warn};
use url::Url;

use super::budget::SourceMetrics;
use super::protocol::{
    AckOutcome, ControlAction, FramePayload, Heartbeat, ProtocolHooks,
};
use crate::clients::disconnect::WssExitReason;
use crate::clients::reconnect::HealthMonitor;
use crate::clients::wss::WssDecoder;

/// Cadence of the WS-protocol RTT ping (independent of any venue keep-alive).
const RTT_PING_SECS: u64 = 10;

/// Bound on the initial TCP+TLS+WS handshake; a black-holed SYN or hung TLS
/// handshake surfaces as `ConnectionFailed` instead of parking the task.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// Receive-staleness deadline: if NO frame of any kind (data, ping, pong)
/// arrives within this window the socket is presumed half-open and the
/// transport exits `Stale` so the worker reconnects. Order books tick far
/// inside this bound on every venue, so a healthy socket never trips it.
const STALE_AFTER: Duration = Duration::from_secs(60);

/// Wall-clock now in UTC epoch microseconds (`0` on a clock error) — the
/// platform timestamp standard. The RTT ping payload
/// carries the same unit end-to-end.
fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// Decode the 8-byte little-endian send-time payload (UTC epoch µs) echoed
/// back in an RTT pong.
/// Returns `None` for any non-8-byte payload (a pong we did not originate, e.g.
/// a venue keep-alive), so callers ignore it rather than mis-measure the RTT.
/// Exit reason for a venue ack rejection: BEFORE the first data frame it is
/// a deterministic subscribe-reject (terminal `SubscriptionRejected`); AFTER
/// data it is a mid-session stream error (retryable contract-break).
fn ack_rejection_exit(streamed: bool, reason: String) -> WssExitReason {
    if streamed {
        WssExitReason::ConnectionFailed(format!("stream error after data: {reason}"))
    } else {
        WssExitReason::SubscriptionRejected(reason)
    }
}

pub(crate) fn decode_ping_send_us(payload: &[u8]) -> Option<u64> {
    <[u8; 8]>::try_from(payload).ok().map(u64::from_le_bytes)
}

/// Fold a fresh RTT `sample_us` into the running EWMA. The first sample
/// (`prev_us == 0`) seeds the average; thereafter the new value weighs 1/5 and
/// the prior 4/5, smoothing per-pong jitter while still tracking drift.
pub(crate) fn rtt_ewma_us(prev_us: u64, sample_us: u64) -> u64 {
    if prev_us == 0 {
        sample_us
    } else {
        (sample_us + 4 * prev_us) / 5
    }
}

/// Generic, exchange-agnostic WSS pump: connect → prepare → subscribe →
/// heartbeat → read(inflate → control → decode) → exit.
pub struct WssTransport<H: ProtocolHooks, D: WssDecoder> {
    hooks: Arc<H>,
    symbols: Vec<String>,
    declared: aetelier_types::config::markets::market_config::DeclaredSet,
    _decoder: PhantomData<fn() -> D>,
}

impl<H: ProtocolHooks, D: WssDecoder> WssTransport<H, D> {
    /// Build a transport for `symbols` using the venue's protocol `hooks`.
    pub fn new(
        hooks: Arc<H>,
        symbols: Vec<String>,
        declared: aetelier_types::config::markets::market_config::DeclaredSet,
    ) -> Self {
        Self {
            hooks,
            symbols,
            declared,
            _decoder: PhantomData,
        }
    }

    /// Run the message loop until the connection closes, goes stale, or `tx`'s
    /// receiver is dropped. Consumes `self`.
    pub async fn run(
        self,
        tx: mpsc::Sender<(D::Event, u64)>,
        rtt_us: Arc<AtomicU64>,
        metrics: SourceMetrics,
    ) -> WssExitReason {
        self.run_with_deadlines(tx, rtt_us, metrics, CONNECT_TIMEOUT, STALE_AFTER)
            .await
    }

    /// [`run`](Self::run) with explicit connect/staleness deadlines — the
    /// production consts in the public path, short values in tests.
    pub(crate) async fn run_with_deadlines(
        self,
        tx: mpsc::Sender<(D::Event, u64)>,
        rtt_us: Arc<AtomicU64>,
        metrics: SourceMetrics,
        connect_timeout: Duration,
        stale_after: Duration,
    ) -> WssExitReason {
        // 1. Bootstrap (token / dynamic URL / login). Default no-op.
        let prepared = match self.hooks.prepare().await {
            Ok(p) => p,
            Err(e) => return WssExitReason::ConnectionFailed(format!("prepare: {e}")),
        };

        // 2. Resolve endpoint (prepare may override it).
        let endpoint = prepared
            .endpoint_override
            .unwrap_or_else(|| self.hooks.endpoint());
        let url = match Url::parse(&endpoint) {
            Ok(u) => u,
            Err(e) => return WssExitReason::ConnectionFailed(format!("url parse: {e}")),
        };

        let (ws_stream, _) = match tokio::time::timeout(
            connect_timeout,
            connect_async(url.as_str()),
        )
        .await
        {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => return WssExitReason::Transport(e),
            Err(_) => {
                return WssExitReason::ConnectionFailed(format!(
                    "connect timed out after {connect_timeout:?}"
                ));
            }
        };
        info!(endpoint = %endpoint, "framework.wss.connected");

        let (writer_half, mut reader) = ws_stream.split();
        let writer = Arc::new(Mutex::new(writer_half));

        // 3. Subscribe (+ any extra bootstrap frames, e.g. HTX REQ seed).
        let mut frames = self.hooks.subscribe_frames(&self.symbols, &self.declared);
        let held_extras = match prepared.extra_frames_delay {
            Some(delay) if !prepared.extra_frames.is_empty() => {
                Some((prepared.extra_frames, delay))
            }
            _ => {
                frames.extend(prepared.extra_frames);
                None
            }
        };
        for frame in frames {
            if let Err(e) = writer.lock().await.send(frame).await {
                return WssExitReason::Transport(e);
            }
        }
        let extras_handle = held_extras.map(|(extras, delay)| {
            let w = writer.clone();
            tokio::spawn(async move {
                sleep(delay).await;
                for frame in extras {
                    if w.lock().await.send(frame).await.is_err() {
                        warn!("framework.wss.delayed_extra_frame_send_failed");
                        return;
                    }
                }
                info!(
                    delay_ms = delay.as_millis() as u64,
                    "framework.wss.extra_frames_sent"
                );
            })
        });

        // 4. Heartbeat task (prepare may override the strategy/cadence). A
        // write failure flips `hb_dead` so the read loop reconnects instead of
        // idling on a socket whose keep-alive silently stopped. The sender is
        // kept alive in this scope so the watch never reads as closed when the
        // venue runs no client heartbeat.
        let (hb_dead_tx, mut hb_dead_rx) = watch::channel(false);
        let heartbeat = prepared
            .heartbeat_override
            .unwrap_or_else(|| self.hooks.heartbeat());
        let hb_handle: Option<tokio::task::JoinHandle<()>> =
            if matches!(heartbeat, Heartbeat::None) {
                None
            } else {
                let w = writer.clone();
                let hb = heartbeat;
                let dead = hb_dead_tx.clone();
                Some(tokio::spawn(async move {
                    loop {
                        let every = match &hb {
                            Heartbeat::WsPong { every } => *every,
                            Heartbeat::Text { every, .. } => *every,
                            Heartbeat::Resubscribe { every, .. } => *every,
                            Heartbeat::None => break,
                        };
                        sleep(every).await;
                        let frame = match &hb {
                            Heartbeat::WsPong { .. } => Message::Pong(Vec::new().into()),
                            Heartbeat::Text { payload, .. } => {
                                Message::Text(payload().into())
                            }
                            Heartbeat::Resubscribe { frame, .. } => frame.clone(),
                            Heartbeat::None => break,
                        };
                        if w.lock().await.send(frame).await.is_err() {
                            let _ = dead.send(true);
                            break;
                        }
                    }
                }))
            };

        // 4b. RTT ping: a WS-protocol Ping carrying the send-ns. Per RFC 6455
        // the server echoes it in the Pong, so the read loop recovers the
        // round-trip with zero per-venue code; venues that ignore WS-ping just
        // leave the RTT at 0.
        let rtt_ping_handle = {
            let w = writer.clone();
            tokio::spawn(async move {
                loop {
                    sleep(Duration::from_secs(RTT_PING_SECS)).await;
                    let payload = now_us().to_le_bytes().to_vec();
                    if w.lock()
                        .await
                        .send(Message::Ping(payload.into()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            })
        };

        // 5. Read loop: staleness-guarded select over inflate → ack →
        // control → decode. `health` resets on EVERY frame (data or control),
        // so only a genuinely silent socket trips the deadline.
        let codec = self.hooks.frame_codec();
        let mut exit = WssExitReason::StreamEnded;
        // Set on the first decoded data frame: an ack rejection BEFORE data
        // is a deterministic subscribe-reject (terminal); one AFTER data is
        // a mid-session stream error (retryable contract-break).
        let mut streamed = false;
        let mut health = HealthMonitor::new(stale_after);

        loop {
            let msg = tokio::select! {
                maybe = reader.next() => match maybe {
                    Some(m) => m,
                    None => break, // StreamEnded
                },
                _ = tokio::time::sleep_until(health.deadline()) => {
                    warn!(silence = ?stale_after, "framework.wss.stale");
                    exit = WssExitReason::Stale { silence: stale_after };
                    break;
                }
                _ = hb_dead_rx.changed() => {
                    if *hb_dead_rx.borrow() {
                        warn!("framework.wss.heartbeat_task_died");
                        exit = WssExitReason::ConnectionFailed(
                            "heartbeat write failed — socket write half dead".into(),
                        );
                        break;
                    }
                    continue;
                }
            };
            let receipt_us = now_us();
            health.record_activity();
            let msg = match msg {
                Ok(m) => m,
                Err(e) => {
                    error!("framework.wss.read_error: {e}");
                    exit = WssExitReason::Transport(e);
                    break;
                }
            };

            // Extract an owned, inflated text frame; control frames are
            // handled inline (ping/pong/close).
            let text: String = match msg {
                Message::Text(t) => match codec.inflate(FramePayload::Text(&t)) {
                    Ok(s) => s,
                    Err(e) => {
                        warn!("framework.wss.inflate_error: {e}");
                        metrics.add_dropped_frames(1);
                        continue;
                    }
                },
                Message::Binary(b) => match codec.inflate(FramePayload::Binary(&b)) {
                    Ok(s) => s,
                    Err(e) => {
                        warn!("framework.wss.inflate_error: {e}");
                        metrics.add_dropped_frames(1);
                        continue;
                    }
                },
                Message::Ping(p) => {
                    let _ = writer.lock().await.send(Message::Pong(p)).await;
                    continue;
                }
                Message::Close(f) => {
                    exit = WssExitReason::ServerClose(f);
                    break;
                }
                // RTT pong: the server echoed our send-µs — recover the round trip.
                Message::Pong(p) => {
                    if let Some(send_us) = decode_ping_send_us(&p) {
                        let rtt = receipt_us.saturating_sub(send_us);
                        if rtt > 0 {
                            let sample_us = rtt.max(1);
                            let prev = rtt_us.load(Ordering::Relaxed);
                            let next = rtt_ewma_us(prev, sample_us);
                            rtt_us.store(next, Ordering::Relaxed);
                        }
                    }
                    continue;
                }
                _ => continue,
            };

            // Subscription acks: pre-data rejection is deterministic
            // (terminal Rejected); post-data rejection is a broken stream
            // contract — reconnect instead of idling data-less.
            match self.hooks.classify_ack(&text) {
                AckOutcome::Rejected(reason) => {
                    error!(%reason, streamed, "framework.wss.subscribe_rejected");
                    exit = ack_rejection_exit(streamed, reason);
                    break;
                }
                AckOutcome::Accepted => {
                    info!("framework.wss.subscribed");
                    continue;
                }
                AckOutcome::NotAck => {}
            }

            // Server-driven echo pings (HTX) reply here and skip decode.
            match self.hooks.on_inbound_control(&text) {
                ControlAction::Reply(frame) => {
                    if let Err(e) = writer.lock().await.send(frame).await {
                        exit = WssExitReason::HeartbeatWriteFailed(e);
                        break;
                    }
                    continue;
                }
                ControlAction::Ignore => {}
            }

            match D::decode(&text) {
                Ok(Some(event)) => {
                    metrics.bump_msgs();
                    streamed = true;
                    if tx.send((event, receipt_us)).await.is_err() {
                        exit = WssExitReason::ReceiverDropped;
                        break;
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    warn!("framework.wss.decode_error: {e}");
                    metrics.bump_decode_err();
                }
            }
        }

        if let Some(h) = hb_handle {
            h.abort();
        }
        if let Some(h) = extras_handle {
            h.abort();
        }
        rtt_ping_handle.abort();
        exit
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framework::protocol::DeclaredSet as Prepared_DS;
    use crate::framework::protocol::Prepared;

    #[test]
    fn pre_data_ack_rejection_is_terminal_post_data_is_retryable() {
        assert!(matches!(
            ack_rejection_exit(false, "bad symbol".into()),
            WssExitReason::SubscriptionRejected(r) if r == "bad symbol"
        ));
        let post = ack_rejection_exit(true, "rate limit".into());
        assert!(matches!(&post, WssExitReason::ConnectionFailed(_)));
        let mapped: crate::clients::disconnect::DisconnectReason = post.into();
        assert!(mapped.is_retryable());
    }

    #[test]
    fn decode_ping_send_us_round_trips_an_8_byte_le_payload() {
        let send_us = 1_700_000_000_123_456_789u64;
        let payload = send_us.to_le_bytes().to_vec();
        assert_eq!(decode_ping_send_us(&payload), Some(send_us));
        // The zero payload is still a valid 8-byte frame.
        assert_eq!(decode_ping_send_us(&0u64.to_le_bytes()), Some(0));
    }

    #[test]
    fn decode_ping_send_us_rejects_wrong_length_payloads() {
        assert_eq!(decode_ping_send_us(&[]), None);
        assert_eq!(decode_ping_send_us(&[1, 2, 3, 4]), None, "too short");
        assert_eq!(decode_ping_send_us(&[0u8; 7]), None, "one byte short");
        assert_eq!(decode_ping_send_us(&[0u8; 9]), None, "one byte long");
        assert_eq!(decode_ping_send_us(&[0u8; 16]), None, "too long");
    }

    #[test]
    fn rtt_ewma_us_seeds_then_smooths() {
        // First sample (prev == 0) seeds the average outright.
        assert_eq!(rtt_ewma_us(0, 200), 200);
        // prev=100, sample=200 -> (200 + 4*100) / 5 = 120.
        assert_eq!(rtt_ewma_us(100, 200), 120);
        // A steady sample equal to prev is a fixed point.
        assert_eq!(rtt_ewma_us(150, 150), 150);
    }

    struct SilentHooks(String);

    impl ProtocolHooks for SilentHooks {
        fn endpoint(&self) -> String {
            self.0.clone()
        }
        fn subscribe_frames(
            &self,
            _symbols: &[String],
            _declared: &Prepared_DS,
        ) -> Vec<Message> {
            Vec::new()
        }
    }

    struct NoopDecoder;

    impl WssDecoder for NoopDecoder {
        type Event = String;
        fn decode(
            text: &str,
        ) -> Result<Option<Self::Event>, Box<crate::errors::ExchangeError>> {
            Ok(Some(text.to_string()))
        }
    }

    struct DelayedExtrasHooks(String);

    #[async_trait::async_trait]
    impl ProtocolHooks for DelayedExtrasHooks {
        fn endpoint(&self) -> String {
            self.0.clone()
        }
        fn subscribe_frames(
            &self,
            _symbols: &[String],
            _declared: &Prepared_DS,
        ) -> Vec<Message> {
            vec![Message::Text("sub".into())]
        }
        async fn prepare(&self) -> Result<Prepared, crate::errors::ExchangeError> {
            Ok(Prepared {
                extra_frames: vec![Message::Text("req".into())],
                extra_frames_delay: Some(Duration::from_millis(300)),
                ..Default::default()
            })
        }
    }

    #[tokio::test]
    async fn delayed_extra_frames_trail_the_subscribe() {
        use futures_util::{SinkExt as _, StreamExt as _};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (seen_tx, mut seen_rx) = mpsc::channel::<(String, std::time::Instant)>(8);
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            while let Some(Ok(msg)) = ws.next().await {
                if let Message::Text(t) = msg {
                    let _ = seen_tx
                        .send((t.to_string(), std::time::Instant::now()))
                        .await;
                }
            }
            let _ = ws.close(None).await;
        });

        let transport = WssTransport::<DelayedExtrasHooks, NoopDecoder>::new(
            Arc::new(DelayedExtrasHooks(format!("ws://{addr}"))),
            vec!["TEST".to_string()],
            Prepared_DS::all(),
        );
        let (tx, _rx) = mpsc::channel(8);
        let run = tokio::spawn(async move {
            transport
                .run_with_deadlines(
                    tx,
                    Arc::new(AtomicU64::new(0)),
                    SourceMetrics::default(),
                    Duration::from_secs(5),
                    Duration::from_secs(5),
                )
                .await
        });

        let (first, t_first) =
            tokio::time::timeout(Duration::from_secs(3), seen_rx.recv())
                .await
                .unwrap()
                .unwrap();
        let (second, t_second) =
            tokio::time::timeout(Duration::from_secs(3), seen_rx.recv())
                .await
                .unwrap()
                .unwrap();
        run.abort();

        assert_eq!(first, "sub", "subscribe goes out immediately");
        assert_eq!(second, "req", "held extras follow");
        let gap = t_second.duration_since(t_first);
        assert!(
            gap >= Duration::from_millis(250),
            "extras must trail by ~the configured delay, gap was {gap:?}"
        );
    }

    #[tokio::test]
    async fn silent_socket_exits_stale() {
        // A server that completes the WS handshake and then never sends a
        // frame: the transport must exit `Stale` at the deadline instead of
        // parking on the read forever.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            sleep(Duration::from_secs(30)).await;
        });

        let transport = WssTransport::<SilentHooks, NoopDecoder>::new(
            Arc::new(SilentHooks(format!("ws://{addr}"))),
            vec!["TEST".to_string()],
            Prepared_DS::all(),
        );
        let (tx, _rx) = mpsc::channel(8);
        let exit = transport
            .run_with_deadlines(
                tx,
                Arc::new(AtomicU64::new(0)),
                SourceMetrics::default(),
                Duration::from_secs(5),
                Duration::from_millis(250),
            )
            .await;
        assert!(
            matches!(exit, WssExitReason::Stale { .. }),
            "expected Stale, got: {exit}"
        );
    }

    #[tokio::test]
    async fn unroutable_connect_times_out() {
        // 203.0.113.0/24 is TEST-NET-3 (RFC 5737): never routable, so the SYN
        // black-holes and only the connect timeout can end the attempt.
        let transport = WssTransport::<SilentHooks, NoopDecoder>::new(
            Arc::new(SilentHooks("ws://203.0.113.1:9".to_string())),
            vec!["TEST".to_string()],
            Prepared_DS::all(),
        );
        let (tx, _rx) = mpsc::channel(8);
        let exit = transport
            .run_with_deadlines(
                tx,
                Arc::new(AtomicU64::new(0)),
                SourceMetrics::default(),
                Duration::from_millis(300),
                Duration::from_secs(5),
            )
            .await;
        match exit {
            WssExitReason::ConnectionFailed(msg) => {
                assert!(msg.contains("timed out"), "unexpected failure: {msg}")
            }
            // Some environments actively reject TEST-NET traffic instead of
            // black-holing it — a transport error is an acceptable fast-fail.
            WssExitReason::Transport(_) => {}
            other => panic!("expected ConnectionFailed/Transport, got: {other}"),
        }
    }
}
