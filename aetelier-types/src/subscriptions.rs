//! Subscription lifecycle types.
//!
//! [`SubscriptionStatus`] tracks the state of a user's market-data
//! subscription.  It is the canonical definition shared by the SDK,
//! backend, and any downstream consumers.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Lifecycle state of a market-data subscription.
///
/// # Serialization
///
/// Serde, `Display`, and `as_str` all produce lowercase strings
/// (`"active"`, `"paused"`, `"deleted"`) to match the PostgreSQL
/// `CHECK` constraint and REST/Kafka wire format.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SubscriptionStatus {
    /// Subscription is live and receiving data.
    Active,
    /// Subscription is temporarily suspended.
    Paused,
    /// Subscription has been soft-deleted.
    Deleted,
}

impl Default for SubscriptionStatus {
    /// Matches the DB column default (`'active'`).
    fn default() -> Self {
        Self::Active
    }
}

impl fmt::Display for SubscriptionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl SubscriptionStatus {
    /// Lowercase string: `"active"`, `"paused"`, or `"deleted"`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Deleted => "deleted",
        }
    }

    /// Lenient parser (case-insensitive).
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "active" => Some(Self::Active),
            "paused" => Some(Self::Paused),
            "deleted" => Some(Self::Deleted),
            _ => None,
        }
    }

    /// Returns `true` when the subscription should receive data.
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active)
    }

    /// Returns `true` when the subscription is soft-deleted.
    pub fn is_deleted(&self) -> bool {
        matches!(self, Self::Deleted)
    }
}

impl FromStr for SubscriptionStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_loose(s)
            .ok_or_else(|| format!("unknown subscription status: {s:?}"))
    }
}
