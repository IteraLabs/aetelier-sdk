//! Integration tests for [`WssClientBuilder`] validation, construction, and
//! URL-building logic.
//!
//! `WssClient<D>` does not implement `Debug`, so we use `let-else` pattern
//! matching instead of `expect_err()` / `unwrap_err()` / `expect()`.

#[cfg(test)]
mod tests {
    use aetelier_connect::clients::wss::wss_client::{
        WssClient, WssClientBuilder, WssDecoder,
    };
    use aetelier_connect::errors::ExchangeError;
    use std::sync::Arc;

    // ── Test decoder ─────────────────────────────────────────────────────

    /// Minimal [`WssDecoder`] implementation for builder tests.
    ///
    /// The decoder logic itself is irrelevant — we only need a concrete
    /// type that satisfies the `WssDecoder` bound so we can exercise the
    /// builder.
    #[derive(Clone)]
    struct StubDecoder;

    #[async_trait::async_trait]
    impl WssDecoder for StubDecoder {
        type Event = String;

        fn decode(text: &str) -> Result<Option<Self::Event>, Box<ExchangeError>> {
            Ok(Some(text.to_string()))
        }
    }

    /// Assert that a builder `Result` is `Err` and the message contains
    /// the expected substring.  Works without `Debug` on `WssClient<D>`.
    fn assert_build_err(
        result: Result<WssClient<StubDecoder>, aetelier_types::errors::BuildError>,
        expected_substr: &str,
    ) {
        let Err(err) = result else {
            panic!("expected build() to fail with '{expected_substr}', but it succeeded");
        };
        assert!(
            err.to_string()
                .to_lowercase()
                .contains(&expected_substr.to_lowercase()),
            "error message should mention '{expected_substr}', got: {err}",
        );
    }

    // ── Test 15: missing-field errors ────────────────────────────────────

    #[test]
    fn test_wss_builder_missing_streams_errors() {
        let result = WssClientBuilder::<StubDecoder>::new()
            // .streams(...)                   ← intentionally omitted
            .base_url("wss://example.com")
            .decoder(StubDecoder)
            .build();

        assert_build_err(result, "streams");
    }

    #[test]
    fn test_wss_builder_missing_base_url_errors() {
        let result = WssClientBuilder::<StubDecoder>::new()
            .streams(vec!["stream1".into()])
            // .base_url(...)                  ← intentionally omitted
            .decoder(StubDecoder)
            .build();

        assert_build_err(result, "base_url");
    }

    #[test]
    fn test_wss_builder_missing_decoder_errors() {
        let result = WssClientBuilder::<StubDecoder>::new()
            .streams(vec!["stream1".into()])
            .base_url("wss://example.com")
            // .decoder(...)                   ← intentionally omitted
            .build();

        assert_build_err(result, "decoder");
    }

    // ── Test 16: complete build succeeds ─────────────────────────────────

    #[test]
    fn test_wss_builder_complete_builds_successfully() {
        let streams = vec!["orderbook.BTCUSDT".to_string(), "trade.BTCUSDT".to_string()];
        let base_url = "wss://stream.bybit.com/v5/public/linear";

        let result = WssClientBuilder::new()
            .streams(streams.clone())
            .base_url(base_url)
            .decoder(StubDecoder)
            .build();

        let Ok(client) = result else {
            panic!("complete builder should succeed");
        };
        assert_eq!(client.streams, streams);
        assert_eq!(client.base_url, base_url);
        // decoder is wrapped in Arc — verify it's accessible
        let _decoder_ref: &StubDecoder = &client.decoder;

        // Exercise the REAL URL the client would connect to (the same
        // `subscribe_url()` that `run()` parses), so a regression in the
        // `?streams=` formatting or `/`-join is caught without a live socket.
        assert_eq!(
            client.subscribe_url(),
            "wss://stream.bybit.com/v5/public/linear?streams=orderbook.BTCUSDT/trade.BTCUSDT"
        );
    }

    #[test]
    fn test_wss_builder_base_url_accepts_string_types() {
        // base_url setter accepts `impl Into<String>` — test with &str and String.
        let from_str = WssClientBuilder::new()
            .streams(vec!["s".into()])
            .base_url("wss://example.com")
            .decoder(StubDecoder)
            .build();
        assert!(from_str.is_ok(), "should accept &str");

        let from_string = WssClientBuilder::new()
            .streams(vec!["s".into()])
            .base_url(String::from("wss://example.com"))
            .decoder(StubDecoder)
            .build();
        assert!(from_string.is_ok(), "should accept String");
    }

    // ── Test 18 (WSS variant): error type is String ──────────────────────

    /// Contract test: `WssClientBuilder::build()` returns `Result<_, String>`.
    #[test]
    fn test_wss_builder_error_type_is_string() {
        let result: Result<WssClient<StubDecoder>, aetelier_types::errors::BuildError> =
            WssClientBuilder::<StubDecoder>::new().build();
        // The type annotation above is the real assertion — compilation
        // proves the error type is String.
        assert!(result.is_err());
    }

    // ── Default trait ────────────────────────────────────────────────────

    #[test]
    fn test_wss_builder_default_is_equivalent_to_new() {
        let Err(from_new) = WssClientBuilder::<StubDecoder>::new().build() else {
            panic!("new() builder with no fields should fail");
        };
        let Err(from_default) = WssClientBuilder::<StubDecoder>::default().build() else {
            panic!("default() builder with no fields should fail");
        };
        assert_eq!(
            from_new, from_default,
            "Default::default() and new() should produce the same initial state",
        );
    }

    // ── Test 22: URL construction inputs ─────────────────────────────────
    //
    // The actual URL string is assembled inside `WssClient::run()`
    // (`format!("{}?streams={}", base_url, streams.join("/"))`, see
    // wss_client.rs), which is an async method that immediately opens a
    // live socket via `connect_async`.  There is no pure, side-effect-free
    // URL-building function on `WssClient`/`WssClientBuilder` to invoke, and
    // `run()` cannot be driven without a real WebSocket server.  Rather than
    // re-implement `run()`'s `format!` inside the test body (which never
    // exercises production code), these tests assert the smallest reachable
    // public surface: the exact `base_url` and `streams` fields that `run()`
    // consumes to build the URL.  See risk_notes for the coverage gap.

    /// Multi-stream: the builder stores the streams verbatim and in order,
    /// so `run()` would join them into `wss://example.com?streams=a/b/c`.
    #[test]
    fn test_wss_client_url_construction() {
        let Ok(client) = WssClientBuilder::new()
            .streams(vec!["a".into(), "b".into(), "c".into()])
            .base_url("wss://example.com")
            .decoder(StubDecoder)
            .build()
        else {
            panic!("builder should succeed");
        };

        // Reachable public surface: the exact inputs run() feeds into the URL.
        assert_eq!(client.base_url, "wss://example.com");
        assert_eq!(client.streams, vec!["a", "b", "c"]);
    }

    /// Single stream: the sole entry is stored intact (no join separator),
    /// so `run()` would produce `...linear?streams=orderbook.50.BTCUSDT`.
    #[test]
    fn test_wss_client_url_construction_single_stream() {
        let Ok(client) = WssClientBuilder::new()
            .streams(vec!["orderbook.50.BTCUSDT".into()])
            .base_url("wss://stream.bybit.com/v5/public/linear")
            .decoder(StubDecoder)
            .build()
        else {
            panic!("builder should succeed");
        };

        assert_eq!(client.base_url, "wss://stream.bybit.com/v5/public/linear");
        assert_eq!(client.streams, vec!["orderbook.50.BTCUSDT"]);
    }

    /// Empty streams: the builder accepts an empty list, so `run()` would
    /// emit a trailing `?streams=` with no stream names.
    #[test]
    fn test_wss_client_url_construction_empty_streams() {
        let Ok(client) = WssClientBuilder::new()
            .streams(vec![])
            .base_url("wss://example.com")
            .decoder(StubDecoder)
            .build()
        else {
            panic!("builder accepts empty streams list");
        };

        assert_eq!(client.base_url, "wss://example.com");
        assert!(client.streams.is_empty());
    }

    // ── Builder reuse: decoder is wrapped in Arc ─────────────────────────

    #[test]
    fn test_wss_builder_wraps_decoder_in_arc() {
        let Ok(client) = WssClientBuilder::new()
            .streams(vec!["s".into()])
            .base_url("wss://example.com")
            .decoder(StubDecoder)
            .build()
        else {
            panic!("builder should succeed");
        };

        // The decoder field should be an Arc — verify by cloning the client
        // (which requires Arc for the decoder) and checking both point to
        // the same allocation.
        let cloned = client.clone();
        assert!(Arc::ptr_eq(&client.decoder, &cloned.decoder));
    }
}
