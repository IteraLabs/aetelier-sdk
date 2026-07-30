use thiserror::Error;

/// Errors from order-book level operations (find, insert, delete, modify).
#[derive(Error, Debug, Clone)]
pub enum LevelError {
    /// A requested price level does not exist.
    #[error("Level not found")]
    LevelNotFound,

    /// Level information could not be retrieved.
    #[error("Level info not available")]
    LevelInfoNotAvailable,

    /// Level deletion operation failed.
    #[error("Level deletion not successful")]
    LevelDeletionFailed,

    /// Level modification operation failed.
    #[error("Level modification not successful")]
    LevelModificationFailed,

    /// Level insertion operation failed.
    #[error("Level insertion not successful")]
    LevelInsertionFailed,
}

/// Errors from order operations within a level.
#[derive(Error, Debug)]
pub enum OrderError {
    /// A requested order does not exist.
    #[error("Order not found")]
    OrderNotFound,

    /// Order information could not be retrieved.
    #[error("Order info not available")]
    OrderInfoNotAvailable,

    /// Order deletion operation failed.
    #[error("Order deletion not successful")]
    OrderDeletionFailed,

    /// Order modification operation failed.
    #[error("Order modification not successful")]
    OrderModificationFailed,

    /// Order insertion operation failed.
    #[error("Order insertion not successful")]
    OrderInsertionFailed,
}

/// Errors related to orderbook operations.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum OrderbookError {
    /// Orderbook not yet initialized with a snapshot.
    #[error("Orderbook not initialized, waiting for snapshot")]
    NotInitialized,
    /// Update sequence ID gap detected, indicating missed messages.
    #[error("Sequence gap: expected {expected}, received {received}")]
    SequenceGap {
        /// The next expected sequence number.
        expected: u64,
        /// The sequence number that was actually received.
        received: u64,
    },
    /// Received update for a different symbol than expected.
    #[error("Symbol mismatch: expected {expected}, received {received}")]
    SymbolMismatch {
        /// The symbol this orderbook was initialized with.
        expected: String,
        /// The symbol found in the incoming update.
        received: String,
    },
    /// Failed to parse price or quantity value.
    #[error("Parse error: {0}")]
    ParseError(String),
    /// Operation attempted on an empty orderbook.
    #[error("Orderbook contents error: {0}")]
    ContentsError(String),
    /// No price level exists at the given price.
    #[error("Level not found at price {price}")]
    LevelNotFound {
        /// The price that was looked up.
        price: String,
    },
    /// No order exists with the given timestamp.
    #[error("Order not found with timestamp {order_ts_us}")]
    OrderNotFound {
        /// The timestamp of the order that was looked up.
        order_ts_us: u64,
    },
    /// Invalid arguments supplied to a method.
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    /// Order builder construction failed.
    #[error("Order builder error: {0}")]
    BuilderError(String),
    /// System clock or timestamp operation error.
    #[error("Timestamp error: {0}")]
    TimestampError(String),
}

/// Errors from persistence operations (file I/O, serialization).
#[derive(Error, Debug)]
pub enum PersistError {
    /// File system I/O error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// JSON serialization or deserialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    /// Data parsing error.
    #[error("Parse error: {0}")]
    Parse(String),
    /// Unsupported output format.
    #[error("Unsupported format: {0}")]
    UnsupportedFormat(String),
}

/// Errors from builder `build()` methods across the data types.
///
/// Replaces the previous stringly-typed `Result<_, String>` builder
/// signatures with a matchable taxonomy while keeping the human-readable
/// message equally informative.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum BuildError {
    /// A required builder field was never set.
    #[error("missing required field: {0}")]
    MissingField(&'static str),
    /// A field was set to a value that fails validation.
    #[error("invalid {field}: {reason}")]
    InvalidField {
        /// Name of the offending field.
        field: &'static str,
        /// Why the value was rejected.
        reason: String,
    },
}

/// Errors related to data loading operations.
#[derive(Error, Debug)]
pub enum LoaderError {
    /// No data provided to process.
    #[error("Empty data: no timestamps to process")]
    EmptyData,

    /// File system or Parquet I/O operation failed.
    #[error("I/O error: {0}")]
    IoError(String),
}

/// Errors related to temporal data validation.
#[derive(Error, Debug)]
pub enum TemporalError {
    /// No timestamps were provided for validation or computation.
    #[error("Empty data: no timestamps to process")]
    EmptyData,

    /// Timestamps are not in strictly increasing order.
    #[error("Non-monotonic timestamps at index {index}: prev={prev}, curr={curr}")]
    NonMonotonic {
        /// Index where the violation was detected.
        index: usize,
        /// Previous timestamp value.
        prev: u64,
        /// Current timestamp value (not greater than prev).
        curr: u64,
    },

    /// Not enough data to perform the requested operation.
    #[error("Insufficient data: {0}")]
    InsufficientData(String),
}

/// Errors from loading a TOML worker manifest or market config.
#[derive(Error, Debug)]
pub enum ConfigError {
    /// The config file could not be read from disk.
    #[error("failed to read config {path}: {source}")]
    Read {
        /// Path that could not be read.
        path: String,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// The TOML contents could not be parsed into the config type.
    #[error("failed to parse config TOML: {0}")]
    Parse(Box<toml::de::Error>),
}
