//! Canonical trading pair representation.
//!
//! [`TradingPair`] decomposes a trading pair into its base and quote
//! assets, providing format conversions for every exchange and a
//! canonical `"BASE/QUOTE"` form that serves as the universal join key
//! across the aetelier ecosystem.
//!
//! # Exchange formats
//!
//! | Exchange  | Wire format      | Example       |
//! |-----------|------------------|---------------|
//! | Bybit     | `BASEQUOTE`      | `SOLUSDT`     |
//! | Binance   | `basequote`      | `solusdt`     |
//! | Coinbase  | `BASE-QUOTE`     | `SOL-USD`     |
//! | Kraken    | `BASE/QUOTE`     | `SOL/USDT`    |
//!
//! The canonical form matches the backend DB `pair` column convention
//! (`SOL/USDT`, slash-separated, uppercase).

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::exchanges::Exchange;

// Known quote assets (longest first for greedy suffix matching)
//
// Used by `from_concatenated()` to split unseparated symbols like "SOLUSDT".
// Ordered longest-first so "USDT" matches before "USD".
//
// Covers the three- and four-letter quotes the certified venues trade.

const KNOWN_QUOTES: &[&str] = &[
    // 4-letter
    "USDT", "USDC", "BUSD", "TUSD", "FDUSD", "USDD", // 3-letter
    "USD", "BTC", "ETH", "EUR", "GBP", "DAI", "BNB",
];

/// Canonical trading pair — base and quote stored separately, uppercase.
///
/// Serializes as the canonical `"BASE/QUOTE"` string (e.g. `"SOL/USDT"`)
/// for wire compatibility with the backend DB and REST API.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TradingPair {
    base: String,
    quote: String,
}

// ── Custom serde: transparent canonical string ─────────────────────────────

impl Serialize for TradingPair {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_canonical())
    }
}

impl<'de> Deserialize<'de> for TradingPair {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        TradingPair::from_canonical(&s)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid trading pair: {s}")))
    }
}

// ── Construction & parsing ─────────────────────────────────────────────────

impl TradingPair {
    /// Create a new trading pair from explicit base and quote assets.
    ///
    /// Both are stored uppercase, trimmed.
    pub fn new(base: impl Into<String>, quote: impl Into<String>) -> Self {
        Self {
            base: base.into().trim().to_uppercase(),
            quote: quote.into().trim().to_uppercase(),
        }
    }

    /// Parse from the canonical `"BASE/QUOTE"` format (e.g. `"SOL/USDT"`).
    ///
    /// Case-insensitive; stored uppercase.
    pub fn from_canonical(s: &str) -> Option<Self> {
        let (base, quote) = s.split_once('/')?;
        let base = base.trim();
        let quote = quote.trim();
        if base.is_empty() || quote.is_empty() {
            return None;
        }
        Some(Self {
            base: base.to_uppercase(),
            quote: quote.to_uppercase(),
        })
    }

    /// Parse from an exchange-native symbol string.
    ///
    /// Uses the exchange to determine the wire format:
    /// - **Bybit / Binance**: concatenated, no separator → suffix-matched
    ///   against the crate-private `KNOWN_QUOTES` quote-symbol list.
    /// - **Coinbase**: hyphen-separated (`BASE-QUOTE`).
    /// - **Kraken**: slash-separated (`BASE/QUOTE`).
    pub fn from_exchange_symbol(raw: &str, exchange: Exchange) -> Option<Self> {
        let raw = raw.trim();
        if raw.is_empty() {
            return None;
        }
        match exchange {
            Exchange::Coinbase | Exchange::Okx | Exchange::Kucoin => {
                Self::from_hyphenated(raw)
            }
            Exchange::Kraken => Self::from_canonical(raw),
            Exchange::Gateio | Exchange::Poloniex | Exchange::Bitso => {
                Self::from_underscore(raw)
            }
            Exchange::Bybit | Exchange::Binance | Exchange::Htx | Exchange::Bitget => {
                Self::from_concatenated(raw)
            }
            Exchange::Upbit => Self::from_quote_first_hyphen(raw),
            Exchange::Hyperliquid => Self::from_bare_coin(raw),
        }
    }

    fn from_bare_coin(raw: &str) -> Option<Self> {
        let coin = raw.trim();
        if coin.is_empty() || coin.contains([':', '@', '/', '-', '_']) {
            return None;
        }
        Some(Self {
            base: coin.to_uppercase(),
            quote: "USDC".to_string(),
        })
    }

    /// Parse a quote-first hyphen symbol (`"KRW-BTC"` → base `BTC`, quote `KRW`).
    /// Upbit lists the quote currency first.
    fn from_quote_first_hyphen(raw: &str) -> Option<Self> {
        let (quote, base) = raw.split_once('-')?;
        let base = base.trim();
        let quote = quote.trim();
        if base.is_empty() || quote.is_empty() {
            return None;
        }
        Some(Self {
            base: base.to_uppercase(),
            quote: quote.to_uppercase(),
        })
    }

    /// Parse a hyphen-separated symbol (`"SOL-USD"`, `"BTC-USDT"`).
    fn from_hyphenated(raw: &str) -> Option<Self> {
        let (base, quote) = raw.split_once('-')?;
        let base = base.trim();
        let quote = quote.trim();
        if base.is_empty() || quote.is_empty() {
            return None;
        }
        Some(Self {
            base: base.to_uppercase(),
            quote: quote.to_uppercase(),
        })
    }

    /// Parse an underscore-separated symbol (`"SOL_USDT"`, `"BTC_USDT"`).
    ///
    /// This is Gate.io's wire format.
    fn from_underscore(raw: &str) -> Option<Self> {
        let (base, quote) = raw.split_once('_')?;
        let base = base.trim();
        let quote = quote.trim();
        if base.is_empty() || quote.is_empty() {
            return None;
        }
        Some(Self {
            base: base.to_uppercase(),
            quote: quote.to_uppercase(),
        })
    }

    /// Parse a concatenated symbol (`"SOLUSDT"`, `"BTCUSD"`) by matching
    /// the longest known quote suffix.
    pub fn from_concatenated(raw: &str) -> Option<Self> {
        let upper = raw.to_uppercase();
        for &q in KNOWN_QUOTES {
            if let Some(base) = upper.strip_suffix(q)
                && !base.is_empty()
            {
                return Some(Self {
                    base: base.to_string(),
                    quote: q.to_string(),
                });
            }
        }
        None
    }

    // ── Accessors ──────────────────────────────────────────────────────

    /// The base asset (e.g. `"SOL"`, `"BTC"`).
    pub fn base(&self) -> &str {
        &self.base
    }

    /// The quote asset (e.g. `"USDT"`, `"USD"`).
    pub fn quote(&self) -> &str {
        &self.quote
    }

    // ── Output formats ─────────────────────────────────────────────────

    /// Canonical `"BASE/QUOTE"` format — matches backend DB `pair` column.
    pub fn to_canonical(&self) -> String {
        format!("{}/{}", self.base, self.quote)
    }

    /// Exchange-native symbol string.
    ///
    /// - **Bybit**: uppercase concatenated (`"SOLUSDT"`)
    /// - **Binance**: lowercase concatenated (`"solusdt"`) — Binance WSS
    ///   topics require lowercase.
    /// - **Coinbase**: uppercase hyphenated (`"SOL-USD"`)
    /// - **Kraken**: uppercase slash-separated (`"SOL/USDT"`)
    pub fn to_exchange_symbol(&self, exchange: Exchange) -> String {
        match exchange {
            Exchange::Bybit => format!("{}{}", self.base, self.quote),
            Exchange::Binance => format!("{}{}", self.base, self.quote).to_lowercase(),
            Exchange::Coinbase => format!("{}-{}", self.base, self.quote),
            Exchange::Kraken => format!("{}/{}", self.base, self.quote),
            Exchange::Okx => format!("{}-{}", self.base, self.quote),
            Exchange::Gateio => format!("{}_{}", self.base, self.quote),
            Exchange::Kucoin => format!("{}-{}", self.base, self.quote),
            Exchange::Poloniex => format!("{}_{}", self.base, self.quote),
            Exchange::Bitso => format!("{}_{}", self.base, self.quote).to_lowercase(),
            Exchange::Htx => format!("{}{}", self.base, self.quote).to_lowercase(),
            Exchange::Bitget => format!("{}{}", self.base, self.quote),
            // Upbit lists the quote currency first (`KRW-BTC`).
            Exchange::Upbit => format!("{}-{}", self.quote, self.base),
            Exchange::Hyperliquid => self.base.clone(),
        }
    }

    /// Normalized lowercase, separator-free form for cross-exchange comparison.
    ///
    /// `SOL/USDT`, `SOLUSDT`, `SOL-USDT` all produce `"solusdt"`.
    pub fn to_normalized(&self) -> String {
        format!("{}{}", self.base, self.quote).to_lowercase()
    }
}

// ── Display / FromStr ──────────────────────────────────────────────────────

impl fmt::Display for TradingPair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.base, self.quote)
    }
}

impl FromStr for TradingPair {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Try canonical first, then hyphenated, then concatenated.
        Self::from_canonical(s)
            .or_else(|| Self::from_hyphenated(s))
            .or_else(|| Self::from_concatenated(s))
            .ok_or_else(|| format!("cannot parse trading pair: '{s}'"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Construction ───────────────────────────────────────────────────

    #[test]
    fn new_normalizes_to_uppercase() {
        let pair = TradingPair::new("sol", "usdt");
        assert_eq!(pair.base(), "SOL");
        assert_eq!(pair.quote(), "USDT");
    }

    #[test]
    fn new_trims_whitespace() {
        let pair = TradingPair::new("  btc ", " usd ");
        assert_eq!(pair.base(), "BTC");
        assert_eq!(pair.quote(), "USD");
    }

    // ── from_canonical ─────────────────────────────────────────────────

    #[test]
    fn from_canonical_basic() {
        let pair = TradingPair::from_canonical("SOL/USDT").unwrap();
        assert_eq!(pair.base(), "SOL");
        assert_eq!(pair.quote(), "USDT");
    }

    #[test]
    fn from_canonical_case_insensitive() {
        let pair = TradingPair::from_canonical("sol/usdt").unwrap();
        assert_eq!(pair.base(), "SOL");
        assert_eq!(pair.quote(), "USDT");
    }

    #[test]
    fn from_canonical_rejects_missing_slash() {
        assert!(TradingPair::from_canonical("SOLUSDT").is_none());
    }

    #[test]
    fn from_canonical_rejects_empty_parts() {
        assert!(TradingPair::from_canonical("/USDT").is_none());
        assert!(TradingPair::from_canonical("SOL/").is_none());
        assert!(TradingPair::from_canonical("/").is_none());
    }

    // ── from_exchange_symbol — Bybit ───────────────────────────────────

    #[test]
    fn from_bybit_concatenated() {
        let pair = TradingPair::from_exchange_symbol("SOLUSDT", Exchange::Bybit).unwrap();
        assert_eq!(pair.base(), "SOL");
        assert_eq!(pair.quote(), "USDT");
    }

    #[test]
    fn from_bybit_btcusd() {
        let pair = TradingPair::from_exchange_symbol("BTCUSD", Exchange::Bybit).unwrap();
        assert_eq!(pair.base(), "BTC");
        assert_eq!(pair.quote(), "USD");
    }

    #[test]
    fn from_bybit_btcusdt() {
        let pair = TradingPair::from_exchange_symbol("BTCUSDT", Exchange::Bybit).unwrap();
        assert_eq!(pair.base(), "BTC");
        assert_eq!(pair.quote(), "USDT");
    }

    #[test]
    fn from_bybit_ethbtc() {
        let pair = TradingPair::from_exchange_symbol("ETHBTC", Exchange::Bybit).unwrap();
        assert_eq!(pair.base(), "ETH");
        assert_eq!(pair.quote(), "BTC");
    }

    // ── from_exchange_symbol — Binance ─────────────────────────────────

    #[test]
    fn from_binance_lowercase() {
        let pair =
            TradingPair::from_exchange_symbol("solusdt", Exchange::Binance).unwrap();
        assert_eq!(pair.base(), "SOL");
        assert_eq!(pair.quote(), "USDT");
    }

    #[test]
    fn from_binance_uppercase_also_works() {
        let pair =
            TradingPair::from_exchange_symbol("ETHUSDC", Exchange::Binance).unwrap();
        assert_eq!(pair.base(), "ETH");
        assert_eq!(pair.quote(), "USDC");
    }

    // ── from_exchange_symbol — Coinbase ────────────────────────────────

    #[test]
    fn from_coinbase_hyphenated() {
        let pair =
            TradingPair::from_exchange_symbol("SOL-USD", Exchange::Coinbase).unwrap();
        assert_eq!(pair.base(), "SOL");
        assert_eq!(pair.quote(), "USD");
    }

    #[test]
    fn from_coinbase_btc_usdt() {
        let pair =
            TradingPair::from_exchange_symbol("BTC-USDT", Exchange::Coinbase).unwrap();
        assert_eq!(pair.base(), "BTC");
        assert_eq!(pair.quote(), "USDT");
    }

    // ── from_exchange_symbol — Kraken ──────────────────────────────────

    #[test]
    fn from_kraken_slash() {
        let pair =
            TradingPair::from_exchange_symbol("SOL/USDT", Exchange::Kraken).unwrap();
        assert_eq!(pair.base(), "SOL");
        assert_eq!(pair.quote(), "USDT");
    }

    #[test]
    fn from_kraken_btc_usd() {
        let pair =
            TradingPair::from_exchange_symbol("BTC/USD", Exchange::Kraken).unwrap();
        assert_eq!(pair.base(), "BTC");
        assert_eq!(pair.quote(), "USD");
    }

    // ── from_exchange_symbol — edge cases ──────────────────────────────

    #[test]
    fn from_exchange_symbol_rejects_empty() {
        assert!(TradingPair::from_exchange_symbol("", Exchange::Bybit).is_none());
        assert!(TradingPair::from_exchange_symbol("  ", Exchange::Coinbase).is_none());
    }

    #[test]
    fn from_concatenated_rejects_unknown_quote() {
        // "SOLXYZ" — XYZ is not a known quote
        assert!(TradingPair::from_exchange_symbol("SOLXYZ", Exchange::Bybit).is_none());
    }

    #[test]
    fn from_concatenated_greedy_longest_quote() {
        // "SOLUSD" should match "USD" (3-letter), not fail
        let pair = TradingPair::from_exchange_symbol("SOLUSD", Exchange::Bybit).unwrap();
        assert_eq!(pair.base(), "SOL");
        assert_eq!(pair.quote(), "USD");

        // "SOLUSDT" should match "USDT" (4-letter) first, not "USD" leaving "T"
        let pair = TradingPair::from_exchange_symbol("SOLUSDT", Exchange::Bybit).unwrap();
        assert_eq!(pair.base(), "SOL");
        assert_eq!(pair.quote(), "USDT");
    }

    // ── to_exchange_symbol ─────────────────────────────────────────────

    #[test]
    fn to_exchange_symbol_bybit() {
        let pair = TradingPair::new("SOL", "USDT");
        assert_eq!(pair.to_exchange_symbol(Exchange::Bybit), "SOLUSDT");
    }

    #[test]
    fn to_exchange_symbol_binance_lowercase() {
        let pair = TradingPair::new("SOL", "USDT");
        assert_eq!(pair.to_exchange_symbol(Exchange::Binance), "solusdt");
    }

    #[test]
    fn to_exchange_symbol_coinbase_hyphenated() {
        let pair = TradingPair::new("SOL", "USD");
        assert_eq!(pair.to_exchange_symbol(Exchange::Coinbase), "SOL-USD");
    }

    #[test]
    fn to_exchange_symbol_kraken_slash() {
        let pair = TradingPair::new("SOL", "USDT");
        assert_eq!(pair.to_exchange_symbol(Exchange::Kraken), "SOL/USDT");
    }

    // ── to_canonical / Display ─────────────────────────────────────────

    #[test]
    fn to_canonical_format() {
        let pair = TradingPair::new("ETH", "USDC");
        assert_eq!(pair.to_canonical(), "ETH/USDC");
        assert_eq!(pair.to_string(), "ETH/USDC");
    }

    // ── to_normalized — cross-exchange equality ────────────────────────

    #[test]
    fn normalized_cross_exchange_equal() {
        let bybit =
            TradingPair::from_exchange_symbol("SOLUSDT", Exchange::Bybit).unwrap();
        let coinbase =
            TradingPair::from_exchange_symbol("SOL-USDT", Exchange::Coinbase).unwrap();
        let kraken =
            TradingPair::from_exchange_symbol("SOL/USDT", Exchange::Kraken).unwrap();

        assert_eq!(bybit.to_normalized(), "solusdt");
        assert_eq!(bybit.to_normalized(), coinbase.to_normalized());
        assert_eq!(coinbase.to_normalized(), kraken.to_normalized());
    }

    #[test]
    fn normalized_different_quotes_not_equal() {
        let usdt = TradingPair::new("SOL", "USDT");
        let usd = TradingPair::new("SOL", "USD");
        assert_ne!(usdt.to_normalized(), usd.to_normalized());
    }

    // ── Serde round-trip ───────────────────────────────────────────────

    #[test]
    fn serde_round_trip_json() {
        let pair = TradingPair::new("SOL", "USDT");
        let json = serde_json::to_string(&pair).unwrap();
        assert_eq!(json, r#""SOL/USDT""#);

        let deserialized: TradingPair = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, pair);
    }

    #[test]
    fn serde_deserialize_canonical_string() {
        let pair: TradingPair = serde_json::from_str(r#""BTC/USD""#).unwrap();
        assert_eq!(pair.base(), "BTC");
        assert_eq!(pair.quote(), "USD");
    }

    #[test]
    fn serde_deserialize_rejects_invalid() {
        let result = serde_json::from_str::<TradingPair>(r#""SOLUSDT""#);
        assert!(
            result.is_err(),
            "concatenated symbol without slash should fail serde"
        );
    }

    // ── FromStr (lenient: tries all formats) ───────────────────────────

    #[test]
    fn from_str_canonical() {
        let pair: TradingPair = "SOL/USDT".parse().unwrap();
        assert_eq!(pair.base(), "SOL");
        assert_eq!(pair.quote(), "USDT");
    }

    #[test]
    fn from_str_hyphenated() {
        let pair: TradingPair = "SOL-USD".parse().unwrap();
        assert_eq!(pair.base(), "SOL");
        assert_eq!(pair.quote(), "USD");
    }

    #[test]
    fn from_str_concatenated() {
        let pair: TradingPair = "SOLUSDT".parse().unwrap();
        assert_eq!(pair.base(), "SOL");
        assert_eq!(pair.quote(), "USDT");
    }

    #[test]
    fn from_str_rejects_garbage() {
        assert!("XYZ".parse::<TradingPair>().is_err());
        assert!("".parse::<TradingPair>().is_err());
    }

    // ── Eq / Hash — structural equality ────────────────────────────────

    #[test]
    fn eq_across_construction_paths() {
        let from_canonical = TradingPair::from_canonical("SOL/USDT").unwrap();
        let from_bybit =
            TradingPair::from_exchange_symbol("SOLUSDT", Exchange::Bybit).unwrap();
        let from_coinbase =
            TradingPair::from_exchange_symbol("SOL-USDT", Exchange::Coinbase).unwrap();
        let from_kraken =
            TradingPair::from_exchange_symbol("SOL/USDT", Exchange::Kraken).unwrap();
        let from_new = TradingPair::new("SOL", "USDT");

        assert_eq!(from_canonical, from_bybit);
        assert_eq!(from_bybit, from_coinbase);
        assert_eq!(from_coinbase, from_kraken);
        assert_eq!(from_kraken, from_new);
    }

    #[test]
    fn hash_consistent_with_eq() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(TradingPair::from_canonical("SOL/USDT").unwrap());
        set.insert(
            TradingPair::from_exchange_symbol("SOLUSDT", Exchange::Bybit).unwrap(),
        );
        set.insert(
            TradingPair::from_exchange_symbol("SOL-USDT", Exchange::Coinbase).unwrap(),
        );
        // All three refer to the same pair — set should have exactly 1 entry.
        assert_eq!(set.len(), 1);
    }

    // ── Round-trip: exchange → canonical → exchange ────────────────────

    #[test]
    fn round_trip_bybit() {
        let pair = TradingPair::from_exchange_symbol("ETHUSDT", Exchange::Bybit).unwrap();
        assert_eq!(pair.to_canonical(), "ETH/USDT");
        assert_eq!(pair.to_exchange_symbol(Exchange::Bybit), "ETHUSDT");
    }

    #[test]
    fn round_trip_binance() {
        let pair =
            TradingPair::from_exchange_symbol("ethusdc", Exchange::Binance).unwrap();
        assert_eq!(pair.to_canonical(), "ETH/USDC");
        assert_eq!(pair.to_exchange_symbol(Exchange::Binance), "ethusdc");
    }

    #[test]
    fn round_trip_coinbase() {
        let pair =
            TradingPair::from_exchange_symbol("BTC-USD", Exchange::Coinbase).unwrap();
        assert_eq!(pair.to_canonical(), "BTC/USD");
        assert_eq!(pair.to_exchange_symbol(Exchange::Coinbase), "BTC-USD");
    }

    #[test]
    fn round_trip_kraken() {
        let pair =
            TradingPair::from_exchange_symbol("BTC/USD", Exchange::Kraken).unwrap();
        assert_eq!(pair.to_canonical(), "BTC/USD");
        assert_eq!(pair.to_exchange_symbol(Exchange::Kraken), "BTC/USD");
    }

    // ── from_exchange_symbol — OKX (hyphenated, like Coinbase) ──────────

    #[test]
    fn from_okx_hyphenated() {
        let pair = TradingPair::from_exchange_symbol("BTC-USDT", Exchange::Okx).unwrap();
        assert_eq!(pair.base(), "BTC");
        assert_eq!(pair.quote(), "USDT");
    }

    #[test]
    fn round_trip_okx() {
        let pair = TradingPair::from_exchange_symbol("ETH-USDT", Exchange::Okx).unwrap();
        assert_eq!(pair.to_canonical(), "ETH/USDT");
        assert_eq!(pair.to_exchange_symbol(Exchange::Okx), "ETH-USDT");
    }

    // ── from_exchange_symbol — Gate (underscore) ───────────────────────

    #[test]
    fn from_gate_underscore() {
        let pair =
            TradingPair::from_exchange_symbol("BTC_USDT", Exchange::Gateio).unwrap();
        assert_eq!(pair.base(), "BTC");
        assert_eq!(pair.quote(), "USDT");
    }

    #[test]
    fn round_trip_gate() {
        let pair =
            TradingPair::from_exchange_symbol("SOL_USDT", Exchange::Gateio).unwrap();
        assert_eq!(pair.to_canonical(), "SOL/USDT");
        assert_eq!(pair.to_exchange_symbol(Exchange::Gateio), "SOL_USDT");
    }

    #[test]
    fn gate_rejects_non_underscore() {
        assert!(TradingPair::from_exchange_symbol("BTCUSDT", Exchange::Gateio).is_none());
    }

    #[test]
    fn okx_gate_cross_exchange_equal() {
        let okx = TradingPair::from_exchange_symbol("BTC-USDT", Exchange::Okx).unwrap();
        let gate =
            TradingPair::from_exchange_symbol("BTC_USDT", Exchange::Gateio).unwrap();
        assert_eq!(okx, gate);
        assert_eq!(okx.to_normalized(), "btcusdt");
    }

    #[test]
    fn from_exchange_symbol_new_venues() {
        // Upbit lists the quote first (`KRW-BTC` → base BTC, quote KRW).
        let p = TradingPair::from_exchange_symbol("KRW-BTC", Exchange::Upbit).unwrap();
        assert_eq!((p.base(), p.quote()), ("BTC", "KRW"));
        // HTX: concatenated lowercase.
        let p = TradingPair::from_exchange_symbol("btcusdt", Exchange::Htx).unwrap();
        assert_eq!((p.base(), p.quote()), ("BTC", "USDT"));
        // KuCoin: hyphen.
        let p = TradingPair::from_exchange_symbol("BTC-USDT", Exchange::Kucoin).unwrap();
        assert_eq!((p.base(), p.quote()), ("BTC", "USDT"));
        // Poloniex: underscore.
        let p =
            TradingPair::from_exchange_symbol("BTC_USDT", Exchange::Poloniex).unwrap();
        assert_eq!((p.base(), p.quote()), ("BTC", "USDT"));
        // Bitso: underscore lowercase.
        let p = TradingPair::from_exchange_symbol("btc_mxn", Exchange::Bitso).unwrap();
        assert_eq!((p.base(), p.quote()), ("BTC", "MXN"));
        // Bitget: concatenated.
        let p = TradingPair::from_exchange_symbol("BTCUSDT", Exchange::Bitget).unwrap();
        assert_eq!((p.base(), p.quote()), ("BTC", "USDT"));
    }

    #[test]
    fn from_exchange_symbol_hyperliquid_bare_coin_is_usdc_margined() {
        let p = TradingPair::from_exchange_symbol("BTC", Exchange::Hyperliquid).unwrap();
        assert_eq!((p.base(), p.quote()), ("BTC", "USDC"));
        assert_eq!(p.to_exchange_symbol(Exchange::Hyperliquid), "BTC");
        let p =
            TradingPair::from_exchange_symbol("kPEPE", Exchange::Hyperliquid).unwrap();
        assert_eq!((p.base(), p.quote()), ("KPEPE", "USDC"));
    }

    #[test]
    fn from_exchange_symbol_hyperliquid_rejects_non_default_dex_and_spot_forms() {
        for raw in [
            "xyz:XYZ100",
            "@107",
            "PURR/USDC",
            "BTC-USDC",
            "BTC_USDC",
            "",
        ] {
            assert_eq!(
                TradingPair::from_exchange_symbol(raw, Exchange::Hyperliquid),
                None,
                "{raw:?} must be rejected"
            );
        }
    }

    #[test]
    fn to_exchange_symbol_new_venues_round_trip() {
        let p = TradingPair::new("BTC", "USDT");
        assert_eq!(p.to_exchange_symbol(Exchange::Kucoin), "BTC-USDT");
        assert_eq!(p.to_exchange_symbol(Exchange::Poloniex), "BTC_USDT");
        assert_eq!(p.to_exchange_symbol(Exchange::Htx), "btcusdt");
        assert_eq!(p.to_exchange_symbol(Exchange::Bitget), "BTCUSDT");
        // Quote-first / lowercase venues round-trip through from_exchange_symbol.
        let upbit = TradingPair::new("BTC", "KRW");
        let wire = upbit.to_exchange_symbol(Exchange::Upbit);
        assert_eq!(wire, "KRW-BTC");
        assert_eq!(
            TradingPair::from_exchange_symbol(&wire, Exchange::Upbit).unwrap(),
            upbit
        );
        let bitso = TradingPair::new("BTC", "MXN");
        let wire = bitso.to_exchange_symbol(Exchange::Bitso);
        assert_eq!(wire, "btc_mxn");
        assert_eq!(
            TradingPair::from_exchange_symbol(&wire, Exchange::Bitso).unwrap(),
            bitso
        );
    }
}
