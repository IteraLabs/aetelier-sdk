use aetelier_types::errors::PersistError;
use aetelier_types::orderbooks::OrderbookDelta;
use std::{fs::File, io::BufWriter, path::Path};

/// Saves an orderbook snapshot to a JSON file with full metadata.
///
/// Writes a comprehensive JSON representation of the orderbook state,
/// including derived metrics (mid price, spread, volume imbalance) and
/// sequencing information useful for replaying or validating data integrity.
///
/// This format preserves the most information and is recommended for
/// checkpointing or debugging. For analytics workloads, consider
/// `orderbooks::io::ob_parquet::write_ob_parquet` instead.
///
/// # Output Structure
///
/// The JSON contains an [`aetelier_types::orderbooks::persist::OrderbookSnapshot`] with:
///
/// - **Metadata**: `timestamp_us`, `symbol`, `update_id`, `sequence`, `delta_count`
/// - **Depth info**: `bid_depth`, `ask_depth`
/// - **Derived metrics**: `mid_price`, `spread`, `spread_bps`, `volume_imbalance`
/// - **Volume totals**: `total_bid_volume`, `total_ask_volume`
/// - **Price levels**: `bids` and `asks` as arrays of `[price, size]` pairs
///
/// # Arguments
///
/// * `ob` - Reference to the [`aetelier_types::orderbooks::delta::OrderbookDelta`] containing the current book state
/// * `path` - Destination file path. Parent directories must exist.
///
/// # Returns
///
/// * `Ok(())` - File was written successfully
/// * `Err(PersistError::Io)` - Failed to create file
/// * `Err(PersistError::Json)` - Serialization failed (should not occur with valid data)
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
/// use aetelier_io::orderbooks::write_json;
/// use aetelier_types::orderbooks::OrderbookDelta;
/// use aetelier_types::trading_pair::TradingPair;
///
/// let ob = OrderbookDelta::new(TradingPair::new("ETH", "USDT"));
/// // ... process some snapshots/deltas ...
///
/// write_json(&ob, Path::new("./snapshots/eth_orderbook.json"))?;
/// # Ok::<(), aetelier_types::errors::PersistError>(())
/// ```
///
/// # Output Example
///
/// ```json
/// {
///   "timestamp_us": 1706500000000,
///   "symbol": "ETHUSDT",
///   "update_id": 18521288,
///   "sequence": 7961638724,
///   "delta_count": 42,
///   "bid_depth": 50,
///   "ask_depth": 50,
///   "mid_price": "2450.25000000",
///   "spread": "0.50000000",
///   "spread_bps": "2.0408",
///   "volume_imbalance": "0.123456",
///   "total_bid_volume": "125.50000000",
///   "total_ask_volume": "98.25000000",
///   "bids": [["2450.00", "1.5"], ["2449.50", "2.3"]],
///   "asks": [["2450.50", "0.8"], ["2451.00", "1.2"]]
/// }
/// ```
///
/// # See Also
///
/// - [`crate::orderbooks::write_csv`] - Lightweight format for simple analysis
/// - `orderbooks::ob_parquet::write_ob_parquet` - Compressed columnar format for large-scale analytics
/// - `orderbooks::ob_parquet::read_ob_parquet` - The supported read-back path
pub fn write_json(ob: &OrderbookDelta, path: &Path) -> Result<(), PersistError> {
    let snapshot = OrderbookDelta::produce_snapshot(&mut ob.clone());
    let file = File::create(path)?;
    let writer = BufWriter::new(file);
    serde_json::to_writer_pretty(writer, &snapshot)?;
    Ok(())
}
