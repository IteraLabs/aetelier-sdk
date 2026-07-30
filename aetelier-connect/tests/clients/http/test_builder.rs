//! Integration tests for [`HttpClientBuilder`] validation and error paths.
//!
//! `HttpClient` does not implement `Debug` (because `RateLimiter` from
//! `async-rate-limiter` does not), so we use `let-else` pattern matching
//! instead of `expect_err()` / `unwrap_err()` which require `Debug` on
//! the `Ok` type.

#[cfg(test)]
mod tests {
    use aetelier_connect::clients::http::http_client::{HttpClient, HttpClientBuilder};
    use aetelier_types::exchanges::Exchange;
    use async_rate_limiter::RateLimiter;
    use reqwest::Client;
    use std::time::Duration;

    // ── Helpers ──────────────────────────────────────────────────────────

    /// Constructs a default `RateLimiter` suitable for testing.
    ///
    /// The actual rate-limit parameters are irrelevant here — we only need
    /// a valid value to satisfy the builder.
    fn test_rate_limiter() -> RateLimiter {
        // 10 requests per second — value is irrelevant for builder tests.
        RateLimiter::new(10)
    }

    /// Assert that a builder `Result` is `Err` and the message contains
    /// the expected substring.  Works without `Debug` on `HttpClient`.
    fn assert_build_err(
        result: Result<HttpClient, aetelier_types::errors::BuildError>,
        expected_substr: &str,
    ) {
        let Err(err) = result else {
            panic!("expected build() to fail with '{expected_substr}', but it succeeded");
        };
        let msg = err.to_string();
        assert!(
            msg.to_lowercase().contains(&expected_substr.to_lowercase()),
            "error message should mention '{expected_substr}', got: {msg}",
        );
    }

    // ── Test 15 (HTTP variant): missing-field errors ─────────────────────
    //
    // `HttpClientBuilder` validates fields in declaration order:
    //   client → exchange → rate_limiter → base_url → timeout
    //
    // Each test omits exactly one field and provides all predecessors so
    // the validation reaches the intended check.

    #[test]
    fn test_http_builder_missing_client_errors() {
        let result = HttpClientBuilder::new()
            // .client(...)                         ← intentionally omitted
            .exchange(Exchange::Bybit)
            .rate_limiter(test_rate_limiter())
            .base_url("https://api.bybit.com".into())
            .timeout(Duration::from_secs(5))
            .build();

        assert_build_err(result, "client");
    }

    #[test]
    fn test_http_builder_missing_exchange_errors() {
        let result = HttpClientBuilder::new()
            .client(Client::new())
            // .exchange(...)                       ← intentionally omitted
            .rate_limiter(test_rate_limiter())
            .base_url("https://api.bybit.com".into())
            .timeout(Duration::from_secs(5))
            .build();

        assert_build_err(result, "exchange");
    }

    #[test]
    fn test_http_builder_missing_rate_limiter_errors() {
        let result = HttpClientBuilder::new()
            .client(Client::new())
            .exchange(Exchange::Bybit)
            // .rate_limiter(...)                   ← intentionally omitted
            .base_url("https://api.bybit.com".into())
            .timeout(Duration::from_secs(5))
            .build();

        assert_build_err(result, "rate_limiter");
    }

    #[test]
    fn test_http_builder_missing_base_url_errors() {
        let result = HttpClientBuilder::new()
            .client(Client::new())
            .exchange(Exchange::Bybit)
            .rate_limiter(test_rate_limiter())
            // .base_url(...)                       ← intentionally omitted
            .timeout(Duration::from_secs(5))
            .build();

        assert_build_err(result, "base_url");
    }

    #[test]
    fn test_http_builder_missing_timeout_errors() {
        let result = HttpClientBuilder::new()
            .client(Client::new())
            .exchange(Exchange::Bybit)
            .rate_limiter(test_rate_limiter())
            .base_url("https://api.bybit.com".into())
            // .timeout(...)                        ← intentionally omitted
            .build();

        assert_build_err(result, "timeout");
    }

    // ── Test 16 (HTTP variant): complete build succeeds ──────────────────

    #[test]
    fn test_http_builder_complete_builds_successfully() {
        let result = HttpClientBuilder::new()
            .client(Client::new())
            .exchange(Exchange::Bybit)
            .rate_limiter(test_rate_limiter())
            .base_url("https://api.bybit.com".into())
            .timeout(Duration::from_secs(10))
            .build();

        let Ok(http_client) = result else {
            panic!("complete builder should succeed");
        };
        assert_eq!(http_client.exchange, Exchange::Bybit);
        assert_eq!(http_client.base_url, "https://api.bybit.com");
        assert_eq!(http_client.timeout, Duration::from_secs(10));
    }

    // ── Test 18: error type is BuildError ────────────────────────────────

    /// Contract test: `HttpClientBuilder::build()` returns
    /// `Result<_, BuildError>`.
    ///
    /// If the error type is ever changed away from `BuildError`, this test
    /// will fail at compile time, alerting the developer to update downstream
    /// callers that match on its variants.
    #[test]
    fn test_http_builder_error_type_is_build_error() {
        use aetelier_types::errors::BuildError;
        let result: Result<HttpClient, BuildError> = HttpClientBuilder::new().build();
        // The type annotation above pins the error type at compile time; an
        // empty builder fails on the first field it validates (`client`).
        let Err(err) = result else {
            panic!("empty builder should fail");
        };
        assert_eq!(err, BuildError::MissingField("client"));
    }

    // ── Default trait ────────────────────────────────────────────────────

    #[test]
    fn test_http_builder_default_is_equivalent_to_new() {
        // Both paths should produce an empty builder that fails identically.
        let Err(from_new) = HttpClientBuilder::new().build() else {
            panic!("new() builder with no fields should fail");
        };
        let Err(from_default) = HttpClientBuilder::default().build() else {
            panic!("default() builder with no fields should fail");
        };
        assert_eq!(
            from_new, from_default,
            "Default::default() and new() should produce the same initial state",
        );
    }
}
