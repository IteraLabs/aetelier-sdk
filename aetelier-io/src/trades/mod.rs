#[cfg(feature = "parquet")]
pub mod trades_parquet;

#[cfg(feature = "parquet")]
pub use trades_parquet::{
    read_trades_parquet, write_trades_parquet, write_trades_parquet_timestamped,
};
