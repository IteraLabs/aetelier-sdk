//! KuCoin REST snapshot seeder.
//!
//! Fetches and parses the public `level2_100` depth snapshot used to seed
//! `SeqDelta` book reconstruction.

use serde::Deserialize;

use aetelier_types::exchanges::Exchange;
use aetelier_types::orderbooks::NormalizedDelta;

use crate::errors::ExchangeError;
use crate::framework::rest::{GenericRestSnapshot, RestSnapshot};

/// KuCoin public REST base.
const KUCOIN_REST_URL: &str = "https://api.kucoin.com";
/// `level2_100` snapshot path.
const KUCOIN_LEVEL2_PATH: &str = "/api/v1/market/orderbook/level2_100";

/// `level2_100` envelope; `data` is null on an error code.
#[derive(Deserialize)]
struct Response {
    data: Option<Data>,
}

/// Partial (top-100) depth snapshot. `sequence` is the snapshot's sequence;
/// WSS deltas whose `sequenceEnd <= sequence` are already included.
#[derive(Deserialize)]
struct Data {
    sequence: String,
    #[serde(default)]
    bids: Vec<[String; 2]>,
    #[serde(default)]
    asks: Vec<[String; 2]>,
}

/// Seeds a KuCoin level2 book from the public `level2_100` REST snapshot,
/// over the rate-limited [`HttpClient`](crate::clients::http::http_client)
/// (keyed on `Exchange::Kucoin`). The seed is fetched once per connect; the
/// snapshot is top-100 only, so deeper levels fill in as deltas arrive.
pub struct KucoinRestSnapshot {
    rest: GenericRestSnapshot,
}

impl KucoinRestSnapshot {
    pub fn new() -> Self {
        Self {
            rest: GenericRestSnapshot::for_venue(
                Exchange::Kucoin,
                KUCOIN_REST_URL,
                KUCOIN_LEVEL2_PATH,
                10,
            ),
        }
    }
}

impl Default for KucoinRestSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl RestSnapshot for KucoinRestSnapshot {
    async fn fetch_snapshot(
        &self,
        symbol: &str,
    ) -> Result<NormalizedDelta, ExchangeError> {
        let body = self.rest.get_raw(&format!("?symbol={symbol}")).await?;
        parse_snapshot(&body, symbol)
    }
}

/// Parse a raw KuCoin `level2` REST body into the seeding snapshot.
/// Shared by `fetch_snapshot` (live) and the adapter's `replay_seed`
/// (offline fixture).
pub fn parse_snapshot(
    body: &str,
    symbol: &str,
) -> Result<NormalizedDelta, ExchangeError> {
    let resp: Response = serde_json::from_str(body).map_err(ExchangeError::JsonError)?;
    let data = resp.data.ok_or_else(|| {
        ExchangeError::IoError(std::io::Error::other("kucoin level2: empty data"))
    })?;
    // `sequence` is the snapshot's update id; the WSS delta carries
    // `sequenceEnd` on `update_id`, so the runtime discards deltas with
    // `sequenceEnd <= sequence` and replays the rest (RangeInclusive).
    let seq = data.sequence.parse::<u64>().unwrap_or(0);
    let levels = |v: Vec<[String; 2]>| {
        v.into_iter()
            .map(|[price, size]| (price, size))
            .collect::<Vec<_>>()
    };
    Ok(NormalizedDelta {
        symbol: symbol.to_string(),
        bids: levels(data.bids),
        asks: levels(data.asks),
        update_id: seq,
        sequence: seq,
        source_orderbook_ts_us: 0,
        local_orderbook_ts_us: 0,
        source_orderbook_rtt_us: 0,
        checksum: None,
        orders: Vec::new(),
        is_snapshot: true,
    })
}
