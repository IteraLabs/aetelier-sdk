#[cfg(feature = "parquet")]
pub mod oi_parquet;

#[cfg(feature = "parquet")]
pub use oi_parquet::{read_oi_parquet, write_oi_parquet, write_oi_parquet_timestamped};
