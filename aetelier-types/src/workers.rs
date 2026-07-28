//! Worker identity types.
//!
//! [`WorkerId`] is the canonical, globally-unique identifier for a
//! market-data worker across the SDK, backend, and webapp.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Globally-unique worker identifier backed by a UUID v4.
///
/// # Serialization
///
/// Serde serializes as a hyphenated UUID string
/// (e.g. `"550e8400-e29b-41d4-a716-446655440000"`).  `Display` and
/// `FromStr` use the same representation.
///
/// # Construction
///
/// Use [`WorkerId::new()`] to generate a fresh random ID, or parse
/// an existing UUID string via [`FromStr`] / [`WorkerId::from_uuid`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkerId(pub Uuid);

impl WorkerId {
    /// Generate a new random worker ID (UUID v4).
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Wrap an existing UUID.
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Adopt an externally-assigned canonical task id as this worker's
    /// identity.
    ///
    /// A managed remote worker runs one dispatched task, and the platform
    /// expects the `task_id` this worker reports on the wire (telemetry,
    /// artifact lineage) to match the id it was assigned — so the worker
    /// takes that id as its own. Use this on the manifest-dispatch path (id
    /// hydrated from the manifest's task metadata); [`WorkerId::new`] remains
    /// the fallback for offline / unit-test runs that have no manifest source.
    pub fn from_canonical(canonical_task_id: Uuid) -> Self {
        Self(canonical_task_id)
    }

    /// Return the inner UUID.
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    /// Hyphenated UUID string.
    pub fn as_str(&self) -> String {
        self.0.to_string()
    }
}

impl Default for WorkerId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for WorkerId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s).map(Self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_canonical_preserves_the_task_id() {
        let task_id = Uuid::new_v4();
        let id = WorkerId::from_canonical(task_id);
        // The worker's identity IS the canonical task id, so the `task_id` it
        // reports on the wire matches the id the platform assigned it.
        assert_eq!(*id.as_uuid(), task_id);
        assert_eq!(id.as_str(), task_id.to_string());
    }

    #[test]
    fn new_ids_are_distinct_from_a_canonical_one() {
        let task_id = Uuid::new_v4();
        assert_ne!(WorkerId::new(), WorkerId::from_canonical(task_id));
    }
}
