//! Bybit/tooling
//!
//! Ancillary tooling for bybit's APIs
//!

///
/// Creates a topic according to Bybit's nomenclature
///
/// # Paramters
///
/// - `topic`: The name of the dataset or endpoitn to stream from, e.g. ['orderbook'].
/// - `symbols`: list of symbols, e.g. [`SOLUSDT`, `BTCUSDT`].
/// - `depths`: Orderbook Depths, accepted ones are these [1, 50, 200, 1000].
///
/// # Returns
///
/// Vec of strings with the topics
///
pub fn create_streams_topics(
    topic: &String,
    symbols: &[String],
    depths: &[i32],
) -> Vec<String> {
    let streams: Vec<String> = depths
        .iter()
        .map(|&i_depth| {
            symbols
                .iter()
                .map(move |i_symbol| format!("{}.{}.{}", topic, i_depth, i_symbol))
                .collect::<Vec<String>>()
        })
        .fold(vec![], |mut v, mut dat| {
            v.append(&mut dat);
            v
        });

    streams
}
