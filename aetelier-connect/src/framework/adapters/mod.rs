//! Per-venue adapters — the only venue-specific code behind the framework.
//!
//! Each adapter wires three small impls against the generic seam:
//! - [`ProtocolHooks`](crate::framework::protocol::ProtocolHooks) — the
//!   transport axis (subscribe framing / heartbeat / control / codec / bootstrap).
//! - [`Normalizer`](crate::framework::model::Normalizer) — decode → `DomainEvent`.
//! - [`ExchangeAdapter`](crate::framework::registry::ExchangeAdapter) — the
//!   registry entry (profile + reconstruction-model + `spawn`).
//!
//! Where available, the decoder + wire response types are reused from
//! `crate::sources::<venue>`, so an adapter is a thin composition.
//!
//! Per-venue transport shapes:
//! - **binance** — combined-stream URL, WS-`Pong` heartbeat, `SeqDelta`
//!   (RangeInclusive) seeded by REST.
//! - **okx** — `{op,args:[{channel,instId}]}` framing, app-level `"ping"` text
//!   heartbeat, `ChecksumDelta` reconstruction.
//! - **upbit** — top-level JSON-array subscribe, `FullRefresh`, `QuoteFirst`
//!   (`KRW-BTC`) codec.
//! - **htx** — `FrameCodec::Gzip`, server-echo ping (`ControlAction::Reply`),
//!   in-band REQ seed (`prepare`→`extra_frames`), `SeqDelta{ExactPrev}`.
//! - **kucoin** — bullet-token `prepare` bootstrap (dynamic endpoint +
//!   server-dictated ping cadence).
//! - **bitso** — `{action,book,type}` subscribe, `L3` order-id-keyed book.

pub mod binance;
pub mod bitget;
pub mod bitso;
pub mod bybit;
pub mod coinbase;
pub mod gateio;
pub mod htx;
pub mod hyperliquid;
pub mod kraken;
pub mod kucoin;
pub mod okx;
pub mod poloniex;
pub mod upbit;
