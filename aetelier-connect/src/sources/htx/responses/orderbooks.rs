//! HTX MBP (market-by-price) depth wire types for the
//! `market.<sym>.mbp.150` channel and its in-band REQ snapshot reply.

use serde::Deserialize;

/// A `[price, size]` level (HTX sends numbers, not strings).
#[derive(Deserialize, Debug, Clone)]
pub struct HtxLevel(pub f64, pub f64);

#[derive(Deserialize, Debug, Clone)]
pub struct HtxMbpTick {
    #[serde(rename = "seqNum")]
    pub seq_num: u64,
    #[serde(rename = "prevSeqNum", default)]
    pub prev_seq_num: Option<u64>,
    #[serde(default)]
    pub bids: Vec<HtxLevel>,
    #[serde(default)]
    pub asks: Vec<HtxLevel>,
}

/// Incremental push: `{"ch":"market.btcusdt.mbp.150","ts":…,"tick":{…}}`.
#[derive(Deserialize, Debug, Clone)]
pub struct HtxMbpUpdate {
    pub ch: String,
    /// Envelope event time (ms); absent on some frames → defaults to 0.
    #[serde(default)]
    pub ts: u64,
    pub tick: HtxMbpTick,
}

/// In-band REQ reply that seeds the book: `{"rep":"market.btcusdt.mbp.150","ts":…,"data":{…}}`.
#[derive(Deserialize, Debug, Clone)]
pub struct HtxMbpSnapshot {
    pub rep: String,
    /// Envelope event time (ms); absent on some frames → defaults to 0.
    #[serde(default)]
    pub ts: u64,
    pub data: HtxMbpTick,
}
