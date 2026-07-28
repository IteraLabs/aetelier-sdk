#[cfg(feature = "parquet")]
pub mod liq_parquet;

#[cfg(feature = "parquet")]
pub use liq_parquet::{
    read_liquidations_parquet, write_liquidations_parquet,
    write_liquidations_parquet_timestamped,
};
