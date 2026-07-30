//! Unified exchange event wrapper.
//!
//! [`ExchangeEvent`] is the single event type that flows through the worker
//! pipeline. Each exchange decoder produces its own native event type, which
//! is then wrapped in an `ExchangeEvent` variant before being sent to the
//! [`DataWorker`](crate::workers::data_worker::DataWorker) event loop.

use crate::sources::binance::events::BinanceWssEvent;
use crate::sources::bybit::events::BybitWssEvent;
use crate::sources::coinbase::events::CoinbaseWssEvent;
use crate::sources::gateio::events::GateioWssEvent;
use crate::sources::kraken::events::KrakenWssEvent;
use crate::sources::okx::events::OkxWssEvent;

/// Exchange-agnostic event wrapper.
///
/// Workers receive `ExchangeEvent` from the channel and `match` on the
/// variant to dispatch into the appropriate `process_*` handler.
#[derive(Debug, Clone)]
pub enum ExchangeEvent {
    Bybit(BybitWssEvent),
    Coinbase(CoinbaseWssEvent),
    Kraken(KrakenWssEvent),
    Binance(BinanceWssEvent),
    Okx(OkxWssEvent),
    Gateio(GateioWssEvent),
}
