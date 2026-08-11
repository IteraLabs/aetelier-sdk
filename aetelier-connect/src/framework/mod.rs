//! Exchange-abstraction framework for ingesting public order-book and trade
//! data from many exchanges.
//!
//! Layers:
//! - [`protocol`](crate::framework::protocol) — `ProtocolHooks`: per-venue subscribe, heartbeat, control,
//!   codec, and bootstrap behavior.
//! - [`transport`](crate::framework::transport) — `WssTransport`: the connect→read→decode loop, returning
//!   [`WssExitReason`](crate::clients::disconnect::WssExitReason).
//! - [`rest`](crate::framework::rest) — `RestSnapshot` over the shared rate-limited `HttpClient`.
//! - [`model`](crate::framework::model) — `SourcedOrderbook` / `SourcedTradebook` drive
//!   `Empty → Synced ⇄ Gapped → Closed`; the apply step is chosen by a
//!   `ReconstructionModel`, and `Normalizer` turns decoded frames into
//!   `DomainEvent`s.
//! - [`symbol`](crate::framework::symbol) — `SymbolCodec` for venue-agnostic symbol formatting.
//! - [`budget`](crate::framework::budget) — `ConnectionBudget` / `StreamBudget` / `SourceMetrics`.
//! - [`registry`](mod@crate::framework::registry) — `ExchangeAdapter` and the `register_all` registry;
//!   `TaskExit` reports a task's terminal outcome.

pub mod adapters;
pub mod atlas;
pub mod budget;
pub mod checksum;
pub mod driver;
pub mod entrepot;
pub mod feed;
pub mod model;
pub mod protocol;
pub mod reconcile;
pub mod registry;
pub mod rest;
pub mod runtime;
pub mod symbol;
pub mod transport;

pub use budget::{
    BufferOverflow, ConnectionBudget, RateWindow, SourceMetrics, StreamBudget,
};
pub use feed::{Feed, FeedDatatype, FeedId, FeedState};
pub use model::{
    BookOutput, ChecksumFmt, DomainEvent, Normalizer, OrderBookState,
    ReconstructionModel, RecoveryAction, ResyncNeeded, SeqPredicate, SnapshotSource,
    SourcedOrderbook, SourcedTradebook, TradeApply, TradeBookState,
};
pub use protocol::{
    ControlAction, FrameCodec, FramePayload, Heartbeat, Prepared, ProtocolHooks,
};
pub use registry::{ExchangeAdapter, ExchangeProfile, TaskExit, registry, resolve};
pub use rest::{GenericRestSnapshot, RestSnapshot};
pub use symbol::SymbolCodec;
pub use transport::WssTransport;
