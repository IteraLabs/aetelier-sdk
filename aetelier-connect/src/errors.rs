use thiserror::Error;

/// Errors from exchange client operations (WebSocket, HTTP, config).
#[derive(Error, Debug)]
pub enum ExchangeError {
    /// WebSocket transport failure. Carries the transport's message rather
    /// than the transport library's error type, so upgrading the WebSocket
    /// dependency is never a public API break.
    #[error("WebSocket connection error: {0}")]
    WebSocketError(String),

    #[error("URL parsing error: {0}")]
    UrlParseError(#[from] url::ParseError),

    #[error("JSON deserialization error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Channel send error")]
    ChannelSendError,

    #[error("An IO error occurred: {0}")]
    IoError(#[from] std::io::Error),

    /// A REST snapshot endpoint returned an error/empty response (e.g. an
    /// unknown or unlisted trading pair) rather than a usable snapshot. Carries
    /// a human-readable reason; non-fatal — the affected book is skipped.
    #[error("REST snapshot unavailable: {0}")]
    SnapshotUnavailable(String),

    /// A requested capability is not available for this venue (e.g. offline
    /// frame replay not yet implemented for a not-yet-certified adapter).
    #[error("unsupported: {0}")]
    Unsupported(String),
}

impl From<tokio_tungstenite::tungstenite::Error> for ExchangeError {
    fn from(error: tokio_tungstenite::tungstenite::Error) -> Self {
        ExchangeError::WebSocketError(error.to_string())
    }
}

/// Errors from loading worker configs and constructing workers.
///
/// Covers the config-parse and worker-build entry points so callers can
/// match on the failure mode rather than inspect an opaque string.
#[derive(Error, Debug)]
pub enum ConnectError {
    /// A config file could not be read from disk.
    #[error("failed to read {path}: {source}")]
    Read {
        /// Path that could not be read.
        path: String,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// The TOML contents could not be parsed into the config type.
    #[error("failed to parse worker config: {0}")]
    Parse(String),
    /// The configured exchange name is not a recognised venue.
    #[error("unknown exchange: {0}")]
    Exchange(String),
    /// Building the worker from its validated config failed.
    #[error("worker build failed: {0}")]
    Build(String),
    /// The worker's output sinks could not be constructed.
    #[error("failed to build output sinks: {0}")]
    Sink(String),
}
