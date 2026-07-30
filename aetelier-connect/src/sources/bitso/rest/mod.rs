//! Bitso REST L3 snapshot seeder: fetches and parses the public
//! `v3/order_book` (aggregate=false) snapshot that seeds the L3 book.

use serde::Deserialize;

use aetelier_types::exchanges::Exchange;
use aetelier_types::orderbooks::{L3Order, NormalizedDelta};

use crate::errors::ExchangeError;
use crate::framework::rest::{GenericRestSnapshot, RestSnapshot};

/// Bitso public REST base.
const BITSO_REST_URL: &str = "https://api.bitso.com";
/// Full L3 order-book snapshot path.
const BITSO_ORDERBOOK_PATH: &str = "/v3/order_book";

/// `v3/order_book?aggregate=false` response. Bitso wraps every reply in
/// `{success, payload | error}`: a success carries `payload`, while an
/// unknown/unlisted book yields `{"success":false,"error":{...}}` with NO
/// `payload`. Both fields are optional so an error reply deserializes (and
/// is reported as a clean [`ExchangeError::SnapshotUnavailable`]) instead of
/// failing with a cryptic serde "missing field `payload`".
#[derive(Deserialize)]
struct Response {
    #[serde(default)]
    success: bool,
    #[serde(default)]
    payload: Option<Payload>,
    #[serde(default)]
    error: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct Payload {
    #[serde(default)]
    bids: Vec<Entry>,
    #[serde(default)]
    asks: Vec<Entry>,
}

/// One resting order: `price`/`amount` decimal strings, `oid` the L3 key.
#[derive(Deserialize)]
struct Entry {
    price: String,
    amount: String,
    #[serde(default)]
    oid: String,
}

/// Seeds a Bitso L3 book from the public `v3/order_book` (aggregate=false)
/// snapshot, which carries the per-order `oid` matching the `diff-orders`
/// stream, over the rate-limited [`HttpClient`](crate::clients::http::http_client)
/// (keyed on `Exchange::Bitso`). The seed is fetched once per connect.
pub struct BitsoRestSnapshot {
    rest: GenericRestSnapshot,
}

impl BitsoRestSnapshot {
    pub fn new() -> Self {
        Self {
            rest: GenericRestSnapshot::for_venue(
                Exchange::Bitso,
                BITSO_REST_URL,
                BITSO_ORDERBOOK_PATH,
                10,
            ),
        }
    }
}

impl Default for BitsoRestSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse a `v3/order_book` body into an L3 seed snapshot. A non-success /
/// payload-less reply (unknown or unlisted pair) returns a clean
/// [`ExchangeError::SnapshotUnavailable`] naming the book + bitso's reason,
/// rather than a cryptic serde error. Pure → unit-testable without the net.
pub fn parse_snapshot(
    body: &str,
    symbol: &str,
) -> Result<NormalizedDelta, ExchangeError> {
    let resp: Response = serde_json::from_str(body).map_err(ExchangeError::JsonError)?;
    let payload = match resp.payload {
        Some(p) if resp.success => p,
        _ => {
            let detail = resp
                .error
                .map(|e| e.to_string())
                .unwrap_or_else(|| body.chars().take(160).collect::<String>());
            return Err(ExchangeError::SnapshotUnavailable(format!(
                "bitso order_book for '{symbol}': {detail}"
            )));
        }
    };
    let mut orders = Vec::with_capacity(payload.bids.len() + payload.asks.len());
    let push = |orders: &mut Vec<L3Order>, side: Vec<Entry>, is_ask: bool| {
        for e in side {
            orders.push(L3Order {
                order_id: e.oid,
                is_ask,
                price: e.price,
                size: e.amount,
                removed: false,
            });
        }
    };
    push(&mut orders, payload.bids, false);
    push(&mut orders, payload.asks, true);
    // L3 has no per-book sequence; the order-id apply is idempotent for
    // re-applied opens, so seed at id 0 and replay every buffered diff
    // (we subscribe before fetching, so buffered diffs are post-snapshot).
    Ok(NormalizedDelta {
        symbol: symbol.to_string(),
        bids: Vec::new(),
        asks: Vec::new(),
        update_id: 0,
        sequence: 0,
        source_orderbook_ts_us: 0,
        local_orderbook_ts_us: 0,
        source_orderbook_rtt_us: 0,
        checksum: None,
        orders,
        is_snapshot: true,
    })
}

#[async_trait::async_trait]
impl RestSnapshot for BitsoRestSnapshot {
    async fn fetch_snapshot(
        &self,
        symbol: &str,
    ) -> Result<NormalizedDelta, ExchangeError> {
        let body = self
            .rest
            .get_raw(&format!("?book={symbol}&aggregate=false"))
            .await?;
        parse_snapshot(&body, symbol)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_success_snapshot_into_l3_orders() {
        let body = r#"{"success":true,"payload":{
                "bids":[{"price":"1000000","amount":"0.5","oid":"b1"}],
                "asks":[{"price":"1000100","amount":"0.3","oid":"a1"}]}}"#;
        let snap = parse_snapshot(body, "btc_mxn").expect("success body parses");
        assert!(snap.is_snapshot);
        assert_eq!(snap.orders.len(), 2);
        assert_eq!(snap.orders[0].order_id, "b1");
        assert!(!snap.orders[0].is_ask);
        assert!(snap.orders[1].is_ask);
    }

    #[test]
    fn unknown_pair_yields_clean_snapshot_unavailable() {
        // The exact failure shape behind the DOGE/MXN seed error: bitso
        // replies success:false + error{} with NO payload.
        let body =
            r#"{"success":false,"error":{"code":"0301","message":"Unknown OrderBook"}}"#;
        let err = parse_snapshot(body, "doge_mxn").expect_err("error body must Err");
        assert!(
            matches!(err, ExchangeError::SnapshotUnavailable(_)),
            "want SnapshotUnavailable, got {err:?}"
        );
        let msg = err.to_string();
        assert!(msg.contains("doge_mxn"), "names the book: {msg}");
        assert!(
            msg.contains("Unknown OrderBook"),
            "carries bitso reason: {msg}"
        );
    }
}
