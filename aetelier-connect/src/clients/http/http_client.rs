use aetelier_types::errors::BuildError;
use aetelier_types::exchanges::Exchange;
use async_rate_limiter::RateLimiter;
use reqwest::Client;
use tokio::time::Duration;

/// Rate-limited HTTP client bound to a specific exchange.
///
/// Bundles a [`reqwest::Client`], a token-bucket
/// [`RateLimiter`], and per-exchange
/// settings (base URL, timeout) into a single, cloneable handle.
///
/// Use [`HttpClientBuilder`] to construct an instance — all five fields
/// are required.
#[derive(Clone)]
pub struct HttpClient {
    /// The underlying `reqwest` HTTP client (connection pool, TLS config).
    pub client: Client,
    /// Exchange this client is configured for.
    pub exchange: Exchange,
    /// Token-bucket rate limiter that throttles outgoing requests to
    /// stay within the exchange's API rate limits.
    pub rate_limiter: RateLimiter,
    /// Base URL for REST endpoints (e.g. `"https://api.bybit.com"`).
    pub base_url: String,
    /// Per-request timeout applied to every outgoing HTTP call.
    pub timeout: Duration,
}

/// Builder for [`HttpClient`].
///
/// All five fields are required.  Calling [`build()`](Self::build) with
/// any field missing returns `Err(String)` naming the absent field.
///
/// # Example
///
/// ```rust,ignore
///
/// let http = HttpClientBuilder::new()
///     .client(reqwest::Client::new())
///     .exchange(Exchange::Bybit)
///     .rate_limiter(RateLimiter::new(10))
///     .base_url("https://api.bybit.com".into())
///     .timeout(Duration::from_secs(5))
///     .build()
///     .expect("all fields set");
/// ```
#[derive(Clone)]
pub struct HttpClientBuilder {
    client: Option<Client>,
    exchange: Option<Exchange>,
    rate_limiter: Option<RateLimiter>,
    base_url: Option<String>,
    timeout: Option<Duration>,
}

impl Default for HttpClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpClientBuilder {
    /// Create an empty builder with all fields set to `None`.
    pub fn new() -> Self {
        HttpClientBuilder {
            client: None,
            exchange: None,
            rate_limiter: None,
            base_url: None,
            timeout: None,
        }
    }

    /// Set the [`reqwest::Client`] that provides the HTTP connection pool.
    pub fn client(mut self, client: Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Set the target [`Exchange`].
    pub fn exchange(mut self, exchange: Exchange) -> Self {
        self.exchange = Some(exchange);
        self
    }

    /// Set the token-bucket [`RateLimiter`].
    pub fn rate_limiter(mut self, rate_limiter: RateLimiter) -> Self {
        self.rate_limiter = Some(rate_limiter);
        self
    }

    /// Set the REST API base URL (e.g. `"https://api.bybit.com"`).
    pub fn base_url(mut self, base_url: String) -> Self {
        self.base_url = Some(base_url);
        self
    }

    /// Set the per-request timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Consume the builder and produce an [`HttpClient`].
    ///
    /// # Errors
    ///
    /// Returns [`BuildError`] if any required field is missing.
    /// Validation order: `client` → `exchange` → `rate_limiter` →
    /// `base_url` → `timeout`.
    pub fn build(self) -> Result<HttpClient, BuildError> {
        let client = self.client.ok_or(BuildError::MissingField("client"))?;
        let exchange = self.exchange.ok_or(BuildError::MissingField("exchange"))?;
        let rate_limiter = self
            .rate_limiter
            .ok_or(BuildError::MissingField("rate_limiter"))?;
        let base_url = self.base_url.ok_or(BuildError::MissingField("base_url"))?;
        let timeout = self.timeout.ok_or(BuildError::MissingField("timeout"))?;

        Ok(HttpClient {
            client,
            exchange,
            rate_limiter,
            base_url,
            timeout,
        })
    }
}
