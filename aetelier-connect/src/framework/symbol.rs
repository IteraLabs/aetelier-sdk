//! Venue-agnostic symbol codec: encodes and decodes trading pairs to and
//! from each venue's wire spelling, selected per venue by the registry.

use aetelier_types::trading_pair::TradingPair;

/// How a venue spells a trading pair on the wire.
#[derive(Debug, Clone)]
pub enum SymbolCodec {
    /// `BTCUSDT` (Binance/Bybit/Bitget); `upper` controls case (HTX = false).
    Concat {
        upper: bool,
    },
    /// `BTC-USDT` (Coinbase/OKX/KuCoin), base first.
    Hyphen,
    /// `BTC/USDT` (Kraken).
    Slash,
    /// `BTC_USDT` (Gate.io/Poloniex/Bitso); `upper` controls case.
    Underscore {
        upper: bool,
    },
    /// `KRW-BTC` (Upbit), quote first.
    QuoteFirst {
        sep: char,
    },
    BareCoin {
        quote: &'static str,
    },
}

impl SymbolCodec {
    /// Render a canonical pair to this venue's wire symbol.
    pub fn encode(&self, pair: &TradingPair) -> String {
        let (b, q) = (pair.base(), pair.quote());
        match self {
            SymbolCodec::Concat { upper } => Self::case(format!("{b}{q}"), *upper),
            SymbolCodec::Hyphen => format!("{b}-{q}"),
            SymbolCodec::Slash => format!("{b}/{q}"),
            SymbolCodec::Underscore { upper } => Self::case(format!("{b}_{q}"), *upper),
            SymbolCodec::QuoteFirst { sep } => format!("{q}{sep}{b}"),
            SymbolCodec::BareCoin { .. } => b.to_string(),
        }
    }

    /// Parse a venue wire symbol back to a canonical pair.
    ///
    /// `Concat` (no separator) splits against the shared known-quote suffix
    /// table in `aetelier-types`; the other codecs split on their separator.
    pub fn decode(&self, raw: &str) -> Option<TradingPair> {
        let up = raw.to_uppercase();
        let (base, quote) = match self {
            SymbolCodec::Hyphen => up.split_once('-')?,
            SymbolCodec::Slash => up.split_once('/')?,
            SymbolCodec::Underscore { .. } => up.split_once('_')?,
            SymbolCodec::QuoteFirst { sep } => {
                let (q, b) = up.split_once(*sep)?;
                (b, q)
            }
            // No separator → suffix-match the known-quote table.
            SymbolCodec::Concat { .. } => return TradingPair::from_concatenated(&up),
            SymbolCodec::BareCoin { quote } => {
                if up.is_empty() || up.contains([':', '@', '/', '-', '_']) {
                    return None;
                }
                (up.as_str(), *quote)
            }
        };
        Some(TradingPair::new(base, quote))
    }

    fn case(s: String, upper: bool) -> String {
        if upper {
            s.to_uppercase()
        } else {
            s.to_lowercase()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codecs_round_trip() {
        let pair = TradingPair::new("BTC", "USDT");
        assert_eq!(SymbolCodec::Concat { upper: true }.encode(&pair), "BTCUSDT");
        assert_eq!(
            SymbolCodec::Concat { upper: false }.encode(&pair),
            "btcusdt"
        );
        assert_eq!(SymbolCodec::Hyphen.encode(&pair), "BTC-USDT");
        assert_eq!(
            SymbolCodec::Underscore { upper: false }.encode(&pair),
            "btc_usdt"
        );
        assert_eq!(
            SymbolCodec::QuoteFirst { sep: '-' }.encode(&pair),
            "USDT-BTC"
        );

        assert_eq!(SymbolCodec::Hyphen.decode("BTC-USDT"), Some(pair.clone()));
        assert_eq!(
            SymbolCodec::Slash.decode("BTC/USD"),
            Some(TradingPair::new("BTC", "USD"))
        );
        assert_eq!(
            SymbolCodec::Underscore { upper: false }.decode("BTC_USDT"),
            Some(pair.clone())
        );
        assert_eq!(
            SymbolCodec::QuoteFirst { sep: '-' }.decode("KRW-BTC"),
            Some(TradingPair::new("BTC", "KRW"))
        );
        // Concat is total: it suffix-matches the known-quote table.
        assert_eq!(
            SymbolCodec::Concat { upper: true }.decode("BTCUSDT"),
            Some(pair.clone())
        );
        assert_eq!(
            SymbolCodec::Concat { upper: false }.decode("btcusdt"),
            Some(pair)
        );
    }

    #[test]
    fn bare_coin_maps_to_fixed_quote_and_rejects_non_default_dex() {
        let codec = SymbolCodec::BareCoin { quote: "USDC" };
        assert_eq!(codec.decode("BTC"), Some(TradingPair::new("BTC", "USDC")));
        assert_eq!(codec.encode(&TradingPair::new("BTC", "USDC")), "BTC");
        for raw in [
            "xyz:XYZ100",
            "@107",
            "PURR/USDC",
            "BTC-USDC",
            "BTC_USDC",
            "",
        ] {
            assert_eq!(codec.decode(raw), None, "{raw:?} must be rejected");
        }
    }
}
