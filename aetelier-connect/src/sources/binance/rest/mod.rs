//! Binance spot public REST client for orderbook snapshots.
//!
//! [`BinanceRestClient`] fetches a single depth snapshot from the
//! Binance REST API. It is a thin wrapper over `reqwest::Client`
//! with no rate-limiting (single-call usage).

use super::responses::orderbooks::BinanceDepthSnapshot;

/// Binance REST API client for public market data.
#[derive(Clone)]
pub struct BinanceRestClient {
    base_url: String,
    http: reqwest::Client,
}

impl BinanceRestClient {
    /// Create a new client pointing at the given base URL.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            http: reqwest::Client::new(),
        }
    }

    /// Fetch a full depth snapshot for the given symbol.
    ///
    /// Calls `GET /api/v3/depth?symbol={symbol}&limit={limit}`.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails or the response
    /// cannot be deserialized.
    pub async fn fetch_depth(
        &self,
        symbol: &str,
        limit: u32,
    ) -> anyhow::Result<BinanceDepthSnapshot> {
        let url = format!(
            "{}/api/v3/depth?symbol={}&limit={}",
            self.base_url, symbol, limit,
        );
        let resp = self.http.get(&url).send().await?.error_for_status()?;
        let snapshot: BinanceDepthSnapshot = resp.json().await?;
        Ok(snapshot)
    }
}
