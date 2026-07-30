//! Communication clients for exchange connectivity.
//!
//! This module provides generic transport clients ([`WssClient`](crate::clients::WssClient), [`HttpClient`](crate::clients::HttpClient))
//! and the reconnection infrastructure ([`reconnect`](crate::clients::reconnect)) used by higher-level
//! workers.
//!
//! Exchange-specific clients (e.g. `BybitWssClient`) live in [`crate::sources`]
//! and build on top of the types defined here.  Reconnection is **not** handled
//! inside the clients themselves — it is the caller's responsibility, typically
//! via a [`ReconnectPolicy`](crate::clients::reconnect::ReconnectPolicy).

/// Policy for disconnections
pub mod disconnect;
/// Http client implementation
pub mod http;
/// Policy for reconnecting
pub mod reconnect;
/// RPC client implementation
pub mod rpc;
/// WSS client implementation
pub mod wss;

/// Connection lifecycle manager with state tracking and reconnection.
pub mod connection_manager;
/// Formal connection state machine for the data worker WSS lifecycle.
pub mod connection_state;

pub use http::http_client::HttpClient;
pub use wss::wss_client::WssClient;
