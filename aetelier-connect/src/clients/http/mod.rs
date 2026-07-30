//! Rate-limited HTTP polling client.
//!
//! [`HttpClient`] wraps a [`reqwest::Client`] together with an
//! [`async_rate_limiter::RateLimiter`], an [`Exchange`](aetelier_types::exchanges::Exchange)
//! identifier, and a per-request timeout.  It is used by REST-based data
//! sources (e.g. funding rates, open interest snapshots) that complement
//! the real-time WebSocket feeds.

pub mod http_client;
pub use http_client::HttpClient;
