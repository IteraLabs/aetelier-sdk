#[cfg(feature = "parquet")]
pub mod funding_parquet;

#[cfg(feature = "parquet")]
pub use funding_parquet::{
    read_funding_parquet, write_funding_parquet, write_funding_parquet_timestamped,
};
