//! Per-venue wire layer — decoders, event enums, and response structs for all
//! twelve venues — plus the unified [`ExchangeEvent`] enum. The six original
//! venues additionally carry the deprecated pre-framework `client/` path.

pub mod binance;
pub mod bitget;
pub mod bitso;
pub mod bybit;
pub mod coinbase;
pub mod events;
pub mod gateio;
pub mod htx;
pub mod kraken;
pub mod kucoin;
pub mod okx;
pub mod poloniex;
pub mod upbit;

pub use events::ExchangeEvent;
