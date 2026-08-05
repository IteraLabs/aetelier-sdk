//! Adapter registry: the single explicit list of compiled-in venue adapters,
//! keyed by venue id and resolved at boot.

use std::collections::HashMap;
use std::sync::OnceLock;

use super::budget::{ConnectionBudget, SourceMetrics};
use super::model::{DomainEvent, ReconstructionModel};
use super::symbol::SymbolCodec;
use crate::clients::disconnect::DisconnectReason;

/// Static, data-only description of a venue, versioned via `schema_version` /
/// `protocol_revision`.
#[derive(Debug, Clone)]
pub struct ExchangeProfile {
    pub id: &'static str,
    pub symbol_codec: SymbolCodec,
    pub budget: ConnectionBudget,
    pub schema_version: u32,
    /// e.g. `"okx-v5"`, `"kraken-v2"` — pinned so a wire bump is caught at boot.
    pub protocol_revision: &'static str,
}

/// Terminal outcome of an Ingest Task run, used by the worker driving this
/// adapter to advance the task to its terminal state. Encodes graceful
/// Stop/Drain.
#[derive(Debug)]
pub enum TaskExit {
    /// Natural end, or a clean Stop fully drained within
    /// [`STOP_DRAIN_TIMEOUT`](crate::framework::driver::STOP_DRAIN_TIMEOUT).
    Completed,
    /// Stop requested but drain exceeded
    /// [`STOP_DRAIN_TIMEOUT`](crate::framework::driver::STOP_DRAIN_TIMEOUT);
    /// outstanding artifacts are marked `partial`.
    DrainTimedOut,
    /// Runtime failure beyond the retry budget; carries the transport cause for
    /// telemetry.
    Failed(DisconnectReason),
}

/// A venue's full integration behind the registry. Object-safe.
pub trait ExchangeAdapter: Send + Sync + 'static {
    /// Stable venue id (`"binance"`, `"okx"`, …).
    fn id(&self) -> &'static str;

    /// Data-only profile (codec, budget, version).
    fn profile(&self) -> &ExchangeProfile;

    /// The venue's REST snapshot seeder, when its reconstruction model
    /// seeds from REST (`model.needs_rest()`). The adapter is the single
    /// source of truth: declaring a REST-seeded model while returning
    /// `None` here is a construction error the worker rejects loudly.
    fn rest_seeder(
        &self,
    ) -> Option<std::sync::Arc<dyn crate::framework::rest::RestSnapshot>> {
        None
    }

    /// Reconstruction-model for a given channel — keyed per channel, not per
    /// venue (a venue may run FullRefresh on one channel and SeqDelta on another).
    fn book_model(&self, channel: &str) -> ReconstructionModel;

    fn supported_datatypes(
        &self,
    ) -> &'static [aetelier_types::config::markets::market_config::DeclaredDatatype] {
        use aetelier_types::config::markets::market_config::DeclaredDatatype as DD;
        &[DD::Orderbook, DD::Trades]
    }

    /// Spawn one Ingest connection carrying a SET of symbols, pumping
    /// `DomainEvent`s into `tx`.
    ///
    /// `shutdown` drives a graceful Stop/Drain, bounded by
    /// [`STOP_DRAIN_TIMEOUT`](crate::framework::driver::STOP_DRAIN_TIMEOUT).
    /// The returned handle resolves to a [`TaskExit`] the caller maps onto the
    /// task's terminal transition.
    ///
    /// `tx` is the in-process Ingest→Sync buffer.
    fn spawn(
        &self,
        symbols: Vec<String>,
        declared: aetelier_types::config::markets::market_config::DeclaredSet,
        tx: tokio::sync::mpsc::Sender<DomainEvent>,
        shutdown: tokio::sync::watch::Receiver<bool>,
        metrics: SourceMetrics,
    ) -> tokio::task::JoinHandle<TaskExit>;

    /// Decode and normalize one raw wire frame OFFLINE — the same
    /// decoder + normalizer `spawn` drives, without a socket. This is the
    /// fixture-replay path the conformance harness (and any replay tooling)
    /// runs committed captures through, so a kind exercises the real
    /// production decode path rather than a re-implementation.
    ///
    /// Returns the `DomainEvent`s the frame yields (empty for a control /
    /// heartbeat / non-data frame). The default reports the venue as not yet
    /// offline-replayable; each adapter overrides it during its conformance
    /// cycle.
    fn subscribe_frames_preview(
        &self,
        _symbols: &[String],
        _declared: &aetelier_types::config::markets::market_config::DeclaredSet,
    ) -> Vec<String> {
        Vec::new()
    }

    fn replay_frame(
        &self,
        _raw: &str,
    ) -> Result<Vec<DomainEvent>, Box<crate::errors::ExchangeError>> {
        Err(Box::new(crate::errors::ExchangeError::Unsupported(
            format!("offline replay not implemented for venue '{}'", self.id()),
        )))
    }

    /// Parse a raw REST seed-snapshot body OFFLINE into the normalized delta
    /// that seeds a `SourcedOrderbook` — the same parse `rest_seeder` performs
    /// live, without a network fetch. Lets the conformance harness seed a
    /// REST-model book from a committed snapshot fixture and replay the
    /// straddling deltas generically.
    ///
    /// `Ok(None)` for a self-seeding venue (no REST seed to parse). The
    /// default reports the venue as not yet offline-seedable; REST-model
    /// adapters override it during their conformance cycle.
    fn replay_seed(
        &self,
        _raw: &str,
        _wire_symbol: &str,
    ) -> Result<
        Option<aetelier_types::orderbooks::NormalizedDelta>,
        Box<crate::errors::ExchangeError>,
    > {
        Err(Box::new(crate::errors::ExchangeError::Unsupported(
            format!(
                "offline seed replay not implemented for venue '{}'",
                self.id()
            ),
        )))
    }
}

/// The single explicit adapter list, one line per compiled-in venue.
fn register_all() -> Vec<&'static dyn ExchangeAdapter> {
    vec![
        &super::adapters::binance::BINANCE,
        &super::adapters::okx::OKX,
        &super::adapters::upbit::UPBIT,
        &super::adapters::htx::HTX,
        &super::adapters::kucoin::KUCOIN,
        &super::adapters::bitso::BITSO,
        &super::adapters::bybit::BYBIT,
        &super::adapters::coinbase::COINBASE,
        &super::adapters::kraken::KRAKEN,
        &super::adapters::gateio::GATEIO,
        &super::adapters::bitget::BITGET,
        &super::adapters::poloniex::POLONIEX,
        &super::adapters::hyperliquid::HYPERLIQUID,
    ]
}

/// Process-wide adapter registry, keyed by venue id. Built once.
pub fn registry() -> &'static HashMap<&'static str, &'static dyn ExchangeAdapter> {
    static R: OnceLock<HashMap<&'static str, &'static dyn ExchangeAdapter>> =
        OnceLock::new();
    R.get_or_init(|| register_all().into_iter().map(|a| (a.id(), a)).collect())
}

/// Boot self-check: every requested venue resolves to a registered adapter.
/// Turns a silent empty stream into a fail-fast at startup. Returns the list of
/// unknown venue ids on failure.
pub fn resolve(ids: &[&str]) -> Result<(), Vec<String>> {
    let reg = registry();
    let missing: Vec<String> = ids
        .iter()
        .filter(|id| !reg.contains_key(**id))
        .map(|id| id.to_string())
        .collect();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(missing)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// All twelve venues are compiled in and self-describe a stable id matching
    /// their registry key.
    #[test]
    fn all_thirteen_venues_register_and_resolve() {
        let ids = [
            "binance",
            "okx",
            "upbit",
            "htx",
            "kucoin",
            "bitso",
            "bybit",
            "coinbase",
            "kraken",
            "gateio",
            "bitget",
            "poloniex",
            "hyperliquid",
        ];
        let reg = registry();
        assert_eq!(reg.len(), ids.len());
        for id in ids {
            let adapter = reg.get(id).unwrap_or_else(|| panic!("{id} not registered"));
            assert_eq!(adapter.id(), id, "registry key must equal adapter.id()");
            assert_eq!(adapter.profile().id, id, "profile.id must equal venue id");
        }
        assert!(resolve(&ids).is_ok());
    }

    #[test]
    fn resolve_reports_unknown_venues() {
        let err = resolve(&["binance", "ftx"]).unwrap_err();
        assert_eq!(err, vec!["ftx".to_string()]);
    }
}
