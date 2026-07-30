//! Generic WebSocket client and decoding trait.
//!
//! This module provides [`WssClient<D>`](crate::clients::wss::WssClient), a transport-level WebSocket pump
//! parameterised over a [`WssDecoder`](crate::clients::wss::WssDecoder) that transforms raw text frames into
//! typed application events. The client handles connection lifecycle
//! (connect, read loop, Ping/Pong, graceful close) while the decoder owns
//! the JSON → domain-event mapping.
//!
//! Exchange-specific decoders live in [`crate::sources`] (e.g.
//! `BybitDecoder`).
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐           ┌───────────┐           ┌────────────────┐
//! │ Exchange WS │ ◄─ TCP ─► │ WssClient │ ── tx ──► │ mpsc::Receiver │
//! │  endpoint   │           │  (pump)   │           │    (caller)    │
//! └─────────────┘           └───────────┘           └────────────────┘
//!                                 │
//!                              encoded
//!                                 │
//!                                 ▼
//!                            WssDecoder<D>
//! ```

pub mod wss_client;
pub use wss_client::{WssClient, WssClientBuilder, WssDecoder};
