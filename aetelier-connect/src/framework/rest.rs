//! REST snapshot seam over the shared rate-limited [`HttpClient`].
//!
//! Provides the order-book snapshot fetch used by venue adapters; each venue
//! supplies the request path and a JSON body parser.

use std::time::Duration;

use aetelier_types::exchanges::Exchange;
use aetelier_types::orderbooks::NormalizedDelta;
use async_rate_limiter::RateLimiter;

use crate::clients::http::http_client::{HttpClient, HttpClientBuilder};
use crate::errors::ExchangeError;

/// Per-request timeout for a seed fetch (a hung fetch must not stall the
/// runtime's seed/reconcile).
const SEED_TIMEOUT_SECS: u64 = 10;

/// Fetches an order-book snapshot for a symbol. Implemented per venue.
#[async_trait::async_trait]
pub trait RestSnapshot: Send + Sync {
    async fn fetch_snapshot(
        &self,
        symbol: &str,
    ) -> Result<NormalizedDelta, ExchangeError>;
}

/// Generic snapshot helper: a rate-limited GET against the venue's base URL.
/// Venues compose this with their own JSON → `NormalizedDelta` parser.
pub struct GenericRestSnapshot {
    pub http: HttpClient,
    pub path: String,
}

impl GenericRestSnapshot {
    pub fn new(http: HttpClient, path: impl Into<String>) -> Self {
        Self {
            http,
            path: path.into(),
        }
    }

    /// Build a rate-limited snapshot client for a venue: a `reqwest` client with
    /// a request timeout, behind the shared [`HttpClient`] token bucket
    /// (`rate_per_sec` permits/sec). Used by the seeders that aren't backed by a
    /// dedicated per-source REST client (KuCoin/Bitso).
    pub fn for_venue(
        exchange: Exchange,
        base_url: impl Into<String>,
        path: impl Into<String>,
        rate_per_sec: usize,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(SEED_TIMEOUT_SECS))
            .build()
            .expect("reqwest client builds with a timeout");
        let http = HttpClientBuilder::new()
            .client(client)
            .exchange(exchange)
            .rate_limiter(RateLimiter::new(rate_per_sec))
            .base_url(base_url.into())
            .timeout(Duration::from_secs(SEED_TIMEOUT_SECS))
            .build()
            .expect("all HttpClient fields are set");
        Self::new(http, path)
    }

    /// Rate-limited GET of `{base_url}{path}{query}`; returns the raw body.
    pub async fn get_raw(&self, query: &str) -> Result<String, ExchangeError> {
        // Respect the venue's token bucket.
        self.http.rate_limiter.acquire().await;
        let url = format!("{}{}{}", self.http.base_url, self.path, query);
        let resp =
            self.http.client.get(&url).send().await.map_err(|e| {
                ExchangeError::IoError(std::io::Error::other(e.to_string()))
            })?;
        resp.text()
            .await
            .map_err(|e| ExchangeError::IoError(std::io::Error::other(e.to_string())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn for_venue_builds_a_rate_limited_client_with_timeout() {
        let rest = GenericRestSnapshot::for_venue(
            Exchange::Kucoin,
            "https://api.kucoin.com",
            "/api/v1/market/orderbook/level2_100",
            10,
        );
        assert_eq!(rest.http.exchange, Exchange::Kucoin);
        assert_eq!(rest.http.base_url, "https://api.kucoin.com");
        assert_eq!(rest.path, "/api/v1/market/orderbook/level2_100");
        assert_eq!(rest.http.timeout, Duration::from_secs(SEED_TIMEOUT_SECS));
    }
}
