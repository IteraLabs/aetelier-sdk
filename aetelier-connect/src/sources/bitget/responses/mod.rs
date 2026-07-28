//! Bitget V2 wire response types for the public `books` and `trade` channels.
//!
//! The `arg` envelope ([`BitgetArg`]) is shared by both frame families, so all
//! wire types live in this single module.

use serde::Deserialize;

/// The `arg` envelope identifying a frame's channel + symbol.
#[derive(Deserialize, Debug, Clone)]
pub struct BitgetArg {
    #[serde(default)]
    pub channel: String,
    #[serde(rename = "instId", default)]
    pub inst_id: String,
}

/// One `books` payload. Bitget sends `["price","size"]` string levels, a
/// millisecond `ts`, and a `seq`/`pseq` pair (current + previous sequence)
/// that drives prev-id continuity.
#[derive(Deserialize, Debug, Clone)]
pub struct BitgetBookData {
    #[serde(default)]
    pub asks: Vec<[String; 2]>,
    #[serde(default)]
    pub bids: Vec<[String; 2]>,
    /// Millisecond timestamp (string), the `update_id` fallback when no seq.
    #[serde(default)]
    pub ts: String,
    /// Current sequence (the `update_id`).
    #[serde(default)]
    pub seq: Option<u64>,
    /// Previous sequence — the prev-id continuity pointer (0 on the snapshot).
    #[serde(default)]
    pub pseq: Option<u64>,
}

/// Book push: `{"action":"snapshot"|"update","arg":{…},"data":[{…}]}`.
#[derive(Deserialize, Debug, Clone)]
pub struct BitgetBookFrame {
    #[serde(default)]
    pub action: String,
    pub arg: BitgetArg,
    #[serde(default)]
    pub data: Vec<BitgetBookData>,
}

/// One trade print. `side` is the taker side (`"buy"`/`"sell"`).
#[derive(Deserialize, Debug, Clone)]
pub struct BitgetTradeData {
    /// Millisecond timestamp (string).
    pub ts: String,
    pub price: String,
    pub size: String,
    pub side: String,
    #[serde(rename = "tradeId", default)]
    pub trade_id: String,
}

/// Trade push: `{"action":…,"arg":{"channel":"trade",…},"data":[{…}]}`.
#[derive(Deserialize, Debug, Clone)]
pub struct BitgetTradeFrame {
    pub arg: BitgetArg,
    #[serde(default)]
    pub data: Vec<BitgetTradeData>,
}
