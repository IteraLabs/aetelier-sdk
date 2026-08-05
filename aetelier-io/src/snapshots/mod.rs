#[cfg(feature = "parquet")]
pub mod aggregate_io;
pub mod decompose;

#[cfg(feature = "parquet")]
pub use aggregate_io::{read_market_aggregate_parquet, write_market_aggregate_parquet};
pub use decompose::{DecomposedSnapshots, decompose_snapshots};
