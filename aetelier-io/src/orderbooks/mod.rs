//! Orderbook persistence: CSV, JSON, Parquet, and terminal rendering.
//!
//! Every writer takes an [`aetelier_types::orderbooks::OrderbookDelta`] and
//! serializes the book it currently holds; every reader returns the rows it
//! finds, with malformed input surfacing as a typed
//! [`aetelier_types::errors::PersistError`] rather than a panic or a
//! fabricated value.
//!
//! | Format | Writer | Reader | Use |
//! |---|---|---|---|
//! | CSV | [`crate::orderbooks::write_csv`] | — | inspection, spreadsheet import |
//! | JSON | [`crate::orderbooks::write_json`] | — | checkpointing, full metadata |
//! | Parquet | `write_ob_parquet` | `read_ob_parquet` | analytics, long-term storage |
//! | Terminal | [`crate::orderbooks::print_orderbook_state`] | — | live monitoring |
//!
//! Parquet is the read-back format; CSV and JSON are write-only surfaces.
//!
//! ```no_run
//! use aetelier_io::orderbooks::{write_csv, write_json};
//! use std::path::Path;
//! # fn demo(book: &aetelier_types::orderbooks::OrderbookDelta)
//! # -> Result<(), Box<dyn std::error::Error>> {
//! write_csv(book, Path::new("book.csv"))?;
//! write_json(book, Path::new("book.json"))?;
//! # Ok(())
//! # }
//! ```

/// Long-format CSV: one row per price level, both sides.
pub mod ob_csv;
pub use ob_csv::write_csv;

/// JSON with derived metrics and sequencing metadata.
pub mod ob_json;
pub use ob_json::write_json;

/// Snappy-compressed Apache Parquet for analytical workloads.
#[cfg(feature = "parquet")]
pub mod ob_parquet;

#[cfg(feature = "parquet")]
pub use ob_parquet::{read_ob_parquet, write_ob_delta_parquet, write_ob_parquet};

/// Human-readable book rendering for live terminal monitoring.
pub mod ob_terminal;
pub use ob_terminal::{Stats, print_orderbook_state};

/// Timestamped and fixed-path snapshot helpers over the format writers.
pub mod persist;
pub use persist::{save_orderbook_state, save_orderbook_timestamped};
