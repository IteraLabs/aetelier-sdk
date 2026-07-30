//! Poloniex v2 `book_lv2` channel wire types.

use serde::Deserialize;

/// One `book_lv2` entry. `id`/`lastId` are the ExactPrev pointer pair:
/// `id` is this update, `lastId` is the immediately preceding one.
#[derive(Deserialize, Debug, Clone)]
pub struct PoloniexBookData {
    pub symbol: String,
    #[serde(default)]
    pub asks: Vec<[String; 2]>,
    #[serde(default)]
    pub bids: Vec<[String; 2]>,
    pub id: u64,
    #[serde(rename = "lastId", default)]
    pub last_id: u64,
    /// Exchange event time (ms); absent on some frames → defaults to 0.
    #[serde(default)]
    pub ts: u64,
}

/// `book_lv2` push: `{"channel":"book_lv2","data":[…],"action":"snapshot"|"update"}`.
#[derive(Deserialize, Debug, Clone)]
pub struct PoloniexBookFrame {
    #[serde(default)]
    pub data: Vec<PoloniexBookData>,
    /// `"snapshot"` (the self-seed) or `"update"` (an incremental delta).
    #[serde(default)]
    pub action: String,
}
