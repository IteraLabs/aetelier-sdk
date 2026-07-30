//! Coinbase Advanced Trade `heartbeats` channel payload.
//!
//! One frame per second. Wire shape (live capture, 2026-07-16):
//!
//! ```json
//! {"channel":"heartbeats","timestamp":"2026-07-16T20:28:53.166419882Z",
//!  "sequence_num":33,
//!  "events":[{"current_time":"2026-07-16 20:28:53.16 +0000 UTC m=+75497.31",
//!             "heartbeat_counter":75497}]}
//! ```
//!
//! `heartbeat_counter` is a JSON integer and is SERVER-global (a per-process
//! uptime counter, not per-connection), so its absolute value is arbitrary —
//! only its per-connection delta carries meaning. `sequence_num` is the
//! connection-wide message counter every Advanced Trade frame carries.

use serde::Deserialize;

/// Envelope of one `heartbeats` frame.
#[derive(Debug, Clone, Deserialize)]
pub struct CoinbaseHeartbeatResponse {
    /// Connection-wide message counter (every frame on the socket bumps it).
    pub sequence_num: u64,
    /// The frame's event array — one entry in practice.
    pub events: Vec<CoinbaseHeartbeatEvent>,
}

/// One heartbeat event.
#[derive(Debug, Clone, Deserialize)]
pub struct CoinbaseHeartbeatEvent {
    /// Server-global heartbeat counter; +1 per second per server process.
    pub heartbeat_counter: u64,
}
