use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[cfg(feature = "wasm")]
use tsify_next::Tsify;
#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;

/// Instrument market type.
///
/// Determines which WebSocket endpoint a client connects to and how
/// symbols are interpreted by the exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "wasm", derive(Tsify), tsify(into_wasm_abi, from_wasm_abi))]
pub enum MarketType {
    /// Spot (cash) market — immediate settlement.
    #[default]
    Spot,
    /// USD-margined perpetual futures — no expiry, funding rate mechanism.
    Perpetual,
    /// Coin-margined inverse perpetual or fixed-maturity contracts.
    Inverse,
}

impl fmt::Display for MarketType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MarketType::Spot => write!(f, "spot"),
            MarketType::Perpetual => write!(f, "perpetual"),
            MarketType::Inverse => write!(f, "inverse"),
        }
    }
}

impl MarketType {
    /// Attempt to parse a market type from a loose string, returning `None`
    /// for unrecognised input instead of a hard error.
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "spot" => Some(MarketType::Spot),
            "perpetual" | "perp" | "linear" => Some(MarketType::Perpetual),
            "inverse" | "coin" => Some(MarketType::Inverse),
            _ => None,
        }
    }
}

impl FromStr for MarketType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        MarketType::from_str_loose(s)
            .ok_or_else(|| format!("unsupported market type: {}", s))
    }
}

/// Supported exchange identifiers.
///
/// Used for dispatch in workers, config validation, and output path
/// partitioning.  The string representation is always lowercase
/// (e.g. `"bybit"`, `"coinbase"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "wasm", derive(Tsify), tsify(into_wasm_abi, from_wasm_abi))]
pub enum Exchange {
    /// Bybit perpetual futures exchange.
    Bybit,
    /// Coinbase spot and derivatives exchange.
    Coinbase,
    /// Kraken spot and derivatives exchange.
    Kraken,
    /// Binance spot and perpetual futures exchange.
    Binance,
    /// OKX (formerly OKEx) spot and derivatives exchange.
    Okx,
    /// Gate.io spot and derivatives exchange.
    Gateio,
    /// Upbit (Korea) spot exchange — quote-first hyphen symbols (`KRW-BTC`).
    Upbit,
    /// Poloniex spot exchange — underscore symbols (`BTC_USDT`).
    Poloniex,
    /// HTX (formerly Huobi) spot and derivatives exchange — concatenated
    /// lowercase symbols (`btcusdt`).
    Htx,
    /// KuCoin spot and derivatives exchange — hyphen symbols (`BTC-USDT`).
    Kucoin,
    /// Bitget spot and derivatives exchange — concatenated symbols (`BTCUSDT`).
    Bitget,
    /// Bitso (LatAm) spot exchange — underscore lowercase symbols (`btc_mxn`).
    Bitso,
    Hyperliquid,
}

impl fmt::Display for Exchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Exchange::Bybit => write!(f, "bybit"),
            Exchange::Coinbase => write!(f, "coinbase"),
            Exchange::Kraken => write!(f, "kraken"),
            Exchange::Binance => write!(f, "binance"),
            Exchange::Okx => write!(f, "okx"),
            Exchange::Gateio => write!(f, "gateio"),
            Exchange::Upbit => write!(f, "upbit"),
            Exchange::Poloniex => write!(f, "poloniex"),
            Exchange::Htx => write!(f, "htx"),
            Exchange::Kucoin => write!(f, "kucoin"),
            Exchange::Bitget => write!(f, "bitget"),
            Exchange::Bitso => write!(f, "bitso"),
            Exchange::Hyperliquid => write!(f, "hyperliquid"),
        }
    }
}

impl Exchange {
    /// Every supported exchange, in declaration order. The authoritative,
    /// compile-time-exhaustive list of venues the SDK can collect from.
    pub const fn all() -> [Exchange; 13] {
        [
            Exchange::Bybit,
            Exchange::Coinbase,
            Exchange::Kraken,
            Exchange::Binance,
            Exchange::Okx,
            Exchange::Gateio,
            Exchange::Upbit,
            Exchange::Poloniex,
            Exchange::Htx,
            Exchange::Kucoin,
            Exchange::Bitget,
            Exchange::Bitso,
            Exchange::Hyperliquid,
        ]
    }

    /// Attempt to parse an exchange from a loose string, returning `None`
    /// for unrecognised input instead of a hard error.
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "bybit" => Some(Exchange::Bybit),
            "coinbase" => Some(Exchange::Coinbase),
            "kraken" => Some(Exchange::Kraken),
            "binance" => Some(Exchange::Binance),
            "okx" | "okex" => Some(Exchange::Okx),
            "gateio" | "gate" | "gate.io" => Some(Exchange::Gateio),
            "upbit" => Some(Exchange::Upbit),
            "poloniex" => Some(Exchange::Poloniex),
            "htx" | "huobi" => Some(Exchange::Htx),
            "kucoin" => Some(Exchange::Kucoin),
            "bitget" => Some(Exchange::Bitget),
            "bitso" => Some(Exchange::Bitso),
            "hyperliquid" => Some(Exchange::Hyperliquid),
            _ => None,
        }
    }
}

impl FromStr for Exchange {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Exchange::from_str_loose(s).ok_or_else(|| format!("unsupported exchange: {}", s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [(&str, Exchange); 13] = [
        ("bybit", Exchange::Bybit),
        ("coinbase", Exchange::Coinbase),
        ("kraken", Exchange::Kraken),
        ("binance", Exchange::Binance),
        ("okx", Exchange::Okx),
        ("gateio", Exchange::Gateio),
        ("upbit", Exchange::Upbit),
        ("poloniex", Exchange::Poloniex),
        ("htx", Exchange::Htx),
        ("kucoin", Exchange::Kucoin),
        ("bitget", Exchange::Bitget),
        ("bitso", Exchange::Bitso),
        ("hyperliquid", Exchange::Hyperliquid),
    ];

    #[test]
    fn every_venue_name_round_trips() {
        for (name, ex) in ALL {
            assert_eq!(ex.to_string(), name, "Display for {ex:?}");
            assert_eq!(name.parse::<Exchange>().unwrap(), ex, "FromStr for {name}");
            assert_eq!(Exchange::from_str_loose(name), Some(ex));
        }
    }

    #[test]
    fn all_matches_the_round_trip_set() {
        let all = Exchange::all();
        assert_eq!(all.len(), ALL.len());
        for (name, ex) in ALL {
            assert!(all.contains(&ex), "Exchange::all() missing {name}");
        }
    }

    #[test]
    fn loose_aliases_parse() {
        assert_eq!(Exchange::from_str_loose("huobi"), Some(Exchange::Htx));
        assert_eq!(Exchange::from_str_loose("okex"), Some(Exchange::Okx));
        assert_eq!(Exchange::from_str_loose("gate"), Some(Exchange::Gateio));
        assert_eq!(Exchange::from_str_loose("UPBIT"), Some(Exchange::Upbit));
        assert_eq!(Exchange::from_str_loose("nope"), None);
    }

    #[test]
    fn serde_uses_lowercase_names() {
        let json = serde_json::to_string(&Exchange::Kucoin).unwrap();
        assert_eq!(json, "\"kucoin\"");
        let back: Exchange = serde_json::from_str("\"bitso\"").unwrap();
        assert_eq!(back, Exchange::Bitso);
    }
}
