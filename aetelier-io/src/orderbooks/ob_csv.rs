use aetelier_types::OrderSide;
use aetelier_types::TimestampUs;
use aetelier_types::errors::PersistError;
use aetelier_types::orderbooks::OrderbookDelta;
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
};

/// Saves an orderbook snapshot to a CSV file.
///
/// Writes the orderbook state in a long-format CSV with one row per price level.
/// The output includes both bid and ask sides, sorted by level index where
/// level 0 represents the best bid/ask (top of book).
///
/// # Output Format
///
/// The CSV contains the following columns:
///
/// | Column | Type | Description |
/// |--------|------|-------------|
/// | `timestamp_us` | `u64` | Unix timestamp in ms when the snapshot was taken |
/// | `symbol` | `String` | Trading pair symbol (e.g., "BTCUSDT") |
/// | `side` | `String` | Either "bid" or "ask" |
/// | `level` | `usize` | Depth level (0 = best price, 1 = second best, etc.) |
/// | `price` | `Decimal` | Price at this level |
/// | `size` | `Decimal` | Total quantity at this level |
///
/// # Arguments
///
/// * `ob` - Reference to the [`OrderbookDelta`] containing the current book state
/// * `path` - Destination file path. Parent directories must exist.
///
/// # Returns
///
/// * `Ok(())` - File was written successfully
/// * `Err(PersistError::Io)` - Failed to create file or write data
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
/// use aetelier_io::orderbooks::write_csv;
/// use aetelier_types::orderbooks::OrderbookDelta;
/// use aetelier_types::trading_pair::TradingPair;
///
/// let ob = OrderbookDelta::new(TradingPair::new("BTC", "USDT"));
/// // ... process some snapshots/deltas ...
///
/// write_csv(&ob, Path::new("./snapshots/btc_orderbook.csv"))?;
/// # Ok::<(), aetelier_types::errors::PersistError>(())
/// ```
///
/// # Output Example
///
/// ```csv
/// timestamp_us,symbol,side,level,price,size
/// 1706500000000,BTCUSDT,bid,0,42150.50,1.5000
/// 1706500000000,BTCUSDT,bid,1,42150.00,2.3000
/// 1706500000000,BTCUSDT,ask,0,42151.00,0.8000
/// 1706500000000,BTCUSDT,ask,1,42151.50,1.2000
/// ```
pub fn write_csv(ob: &OrderbookDelta, path: &Path) -> Result<(), PersistError> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);

    // Header
    writeln!(writer, "timestamp_us,symbol,side,level,price,size")?;

    let timestamp = TimestampUs::now().as_micros();
    let symbol = ob.pair().to_canonical();

    // Write bids (level 0 = best bid)
    for (level, (price, size)) in ob.top_bids(ob.bid_depth()).iter().enumerate() {
        writeln!(
            writer,
            "{},{},{},{},{},{}",
            timestamp,
            symbol,
            OrderSide::Bids,
            level,
            price,
            size
        )?;
    }

    // Write asks (level 0 = best ask)
    for (level, (price, size)) in ob.top_asks(ob.ask_depth()).iter().enumerate() {
        writeln!(
            writer,
            "{},{},{},{},{},{}",
            timestamp,
            symbol,
            OrderSide::Asks,
            level,
            price,
            size
        )?;
    }

    writer.flush()?;
    Ok(())
}
