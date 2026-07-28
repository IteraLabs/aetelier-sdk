//! Bitso `diff-orders` (per-order L3 book) wire payloads.

use serde::Deserialize;

/// One per-order change. `t`: 0 = bid, 1 = ask. `s`: open/cancelled/completed.
#[derive(Deserialize, Debug, Clone)]
pub struct BitsoDiffOrder {
    /// Order timestamp (ms); absent on some entries, so default to 0 (the
    /// L3 apply is keyed by order id, not this timestamp).
    #[serde(default)]
    pub d: u64,
    /// Rate (price), a precision-significant decimal string. Absent on some
    /// cancellations.
    #[serde(default)]
    pub r: Option<String>,
    /// Amount, a decimal string. Absent once the order is gone.
    #[serde(default)]
    pub a: Option<String>,
    /// Side: 0 = bid, 1 = ask.
    pub t: u8,
    /// Order id (the L3 key).
    pub o: String,
    /// Status: `"open"` / `"cancelled"` / `"completed"`.
    #[serde(default)]
    pub s: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct BitsoDiffMessage {
    pub book: String,
    pub payload: Vec<BitsoDiffOrder>,
    /// Per-book envelope sequence — DENSE (+1 per diff-orders message for
    /// this book; verified contiguous across 22,394 live frames,
    /// 2026-07-16). The book-channel loss sentinel: a hole is a dropped
    /// message, exactly counted. Absent on acks/legacy frames.
    #[serde(default)]
    pub sequence: Option<u64>,
}
