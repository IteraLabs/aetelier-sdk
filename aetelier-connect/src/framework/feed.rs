//! Ingest subscription primitive: a `Feed` is a live subscription to one
//! `(venue, instrument, datatype)` market-data stream from an external
//! exchange. Its `FeedId` is allocated at creation and its state is reported
//! via the telemetry channel.

use std::sync::{Arc, Mutex};

use aetelier_types::exchanges::Exchange;
use aetelier_types::trading_pair::TradingPair;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Identifier for one `Feed` (UUIDv4). Distinct newtype so it cannot be
/// confused with the wire `ArtifactId`/`TaskId`/`SinkId`. The wire shape stays
/// a flat string; this wraps it in the language layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FeedId(pub Uuid);

impl FeedId {
    /// Allocate a fresh `FeedId` at Feed creation.
    pub fn new() -> Self {
        FeedId(Uuid::new_v4())
    }
}

impl Default for FeedId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for FeedId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The market-data type a `Feed` subscribes to. Selects the bound book
/// (orders → `SourcedOrderbook`, trades → `SourcedTradebook`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FeedDatatype {
    Orders,
    Trades,
    FundingRates,
    OpenInterest,
}

/// `Feed` lifecycle state. `Closed`/`Rejected`/`Failed` are terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FeedState {
    /// Created from the TaskSpec; FeedId allocated; nothing on the wire.
    Requested,
    /// Subscribe frame(s) sent; awaiting first data / seed.
    Subscribing,
    /// Streaming; the bound book is Live.
    Live,
    /// Recovering from a book `Gapped`; re-seed per the recovery action.
    Resubscribing,
    /// The shared venue connection dropped; awaiting re-establishment.
    Reconnecting,
    /// Task Stop; flushing in-flight, bounded by [`STOP_DRAIN_TIMEOUT`](crate::framework::driver::STOP_DRAIN_TIMEOUT).
    Draining,
    /// Normal terminal (possibly `partial`).
    Closed,
    /// Terminal; venue/datatype unsupported (surfaces an ErrorKind).
    Rejected,
    /// Terminal; connection unrecoverable within the reconnect budget.
    Failed,
}

impl FeedState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            FeedState::Closed | FeedState::Rejected | FeedState::Failed
        )
    }

    /// The legal transition graph — the executable form of the Feed FSM's
    /// edge set (atlas DAT-F-T-*). Setters `debug_assert!` against it, so an
    /// illegal jump panics under test/CI and costs nothing in release.
    /// Terminal states have no outgoing edges.
    pub fn can_transition_to(self, next: FeedState) -> bool {
        use FeedState::*;
        matches!(
            (self, next),
            (Requested, Subscribing | Draining)
                | (Subscribing, Live | Resubscribing | Reconnecting | Draining)
                | (Live, Resubscribing | Reconnecting | Draining)
                | (Resubscribing, Live | Reconnecting | Draining)
                | (Reconnecting, Subscribing | Rejected | Failed | Draining)
                | (Draining, Closed)
        )
    }
}

/// One live `(venue, instrument, datatype)` subscription. The bound book is
/// held by the Sync layer keyed off `id`; this struct carries the subscription
/// identity and FSM state.
#[derive(Debug, Clone)]
pub struct Feed {
    id: FeedId,
    venue: Exchange,
    instrument: TradingPair,
    datatype: FeedDatatype,
    state: FeedState,
    /// Set when the drain exceeded its bound: `Closed` with outstanding
    /// in-flight data (`TaskExit::DrainTimedOut`).
    closed_partial: bool,
    /// Set on entry to `Rejected`/`Failed`: the venue rejection or give-up
    /// reason, so terminal feeds carry WHY on the status/telemetry surface.
    terminal_reason: Option<String>,
}

impl Feed {
    /// Create a Feed for a TaskSpec entry; allocates the `FeedId` and
    /// lands in `Requested`.
    pub fn new(venue: Exchange, instrument: TradingPair, datatype: FeedDatatype) -> Self {
        Self {
            id: FeedId::new(),
            venue,
            instrument,
            datatype,
            state: FeedState::Requested,
            closed_partial: false,
            terminal_reason: None,
        }
    }

    pub fn id(&self) -> FeedId {
        self.id
    }
    pub fn venue(&self) -> Exchange {
        self.venue
    }
    pub fn instrument(&self) -> &TradingPair {
        &self.instrument
    }
    pub fn datatype(&self) -> FeedDatatype {
        self.datatype
    }
    pub fn state(&self) -> FeedState {
        self.state
    }

    // FSM transitions. The worker invokes these at the transition boundaries
    // after performing the guard/effects; this records the state. Each
    // setter asserts the edge against `FeedState::can_transition_to` in
    // debug builds.

    fn transition(&mut self, next: FeedState) {
        debug_assert!(
            self.state.can_transition_to(next),
            "illegal Feed transition {:?} -> {:?} (feed {})",
            self.state,
            next,
            self.id
        );
        self.state = next;
    }

    /// Subscribe sent (from Requested or Reconnecting).
    pub fn to_subscribing(&mut self) {
        self.transition(FeedState::Subscribing);
    }
    /// First data + bound book Live (from Subscribing or Resubscribing).
    pub fn to_live(&mut self) {
        self.transition(FeedState::Live);
    }
    /// Bound book `Gapped`; begin recovery.
    pub fn to_resubscribing(&mut self) {
        self.transition(FeedState::Resubscribing);
    }
    /// Shared venue connection dropped (retryable).
    pub fn to_reconnecting(&mut self) {
        self.transition(FeedState::Reconnecting);
    }
    /// Task Stop/Drain.
    pub fn to_draining(&mut self) {
        self.transition(FeedState::Draining);
    }
    /// Drained (clean or partial-past-timeout) → terminal.
    pub fn to_closed(&mut self) {
        self.transition(FeedState::Closed);
    }
    /// Drain exceeded its bound → terminal with outstanding data (`partial`).
    pub fn to_closed_partial(&mut self) {
        self.transition(FeedState::Closed);
        self.closed_partial = true;
    }
    /// Whether a `Closed` feed drained past the bound with data outstanding.
    pub fn closed_partial(&self) -> bool {
        self.closed_partial
    }
    /// Venue/datatype unsupported → terminal, carrying the venue reason.
    pub fn to_rejected(&mut self, reason: &str) {
        self.transition(FeedState::Rejected);
        self.terminal_reason = Some(reason.to_string());
    }
    /// Reconnect budget exhausted → terminal, carrying the give-up reason.
    pub fn to_failed(&mut self, reason: &str) {
        self.transition(FeedState::Failed);
        self.terminal_reason = Some(reason.to_string());
    }
    /// The rejection/give-up reason of a `Rejected`/`Failed` feed.
    pub fn terminal_reason(&self) -> Option<&str> {
        self.terminal_reason.as_deref()
    }
}

/// Point-in-time view of one feed, for status/telemetry fan-out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedSnapshot {
    /// The `FeedId`, stringified for the wire.
    pub id: String,
    /// Canonical instrument (e.g. `BTC/USDT`).
    pub instrument: String,
    pub datatype: FeedDatatype,
    pub state: FeedState,
    /// Meaningful only in `Closed`: drain exceeded its bound.
    pub partial: bool,
    /// Meaningful only in `Rejected`/`Failed`: the venue rejection or
    /// give-up reason (`serde(default)` keeps older producers valid).
    #[serde(default)]
    pub reason: Option<String>,
}

/// The worker's set of feeds for one connection — one per
/// (instrument, datatype) it subscribes. Owned by the worker so `FeedId`s
/// survive socket reconnects; shared into each `SourceRuntime` iteration.
///
/// Terminal states absorb: every `mark_*` skips feeds whose state
/// `is_terminal()`, so a `Closed`/`Rejected`/`Failed` feed never regresses.
#[derive(Debug, Default)]
pub struct FeedSet {
    feeds: Vec<Feed>,
}

/// Shared handle: worker owns it, runtimes transition it. Lock discipline:
/// never held across an await.
pub type SharedFeedSet = Arc<Mutex<FeedSet>>;

impl FeedSet {
    pub fn new(feeds: Vec<Feed>) -> Self {
        Self { feeds }
    }

    /// Wrap into the shared handle the worker hands to each runtime.
    pub fn into_shared(self) -> SharedFeedSet {
        Arc::new(Mutex::new(self))
    }

    fn each_live_mut(&mut self, f: impl Fn(&mut Feed)) {
        for feed in self.feeds.iter_mut().filter(|x| !x.state().is_terminal()) {
            f(feed);
        }
    }

    /// Subscribe frames sent for this connection (initial or after reconnect).
    pub fn mark_all_subscribing(&mut self) {
        self.each_live_mut(|f| f.to_subscribing());
    }

    /// The shared venue connection dropped; every non-terminal feed waits.
    pub fn mark_all_reconnecting(&mut self) {
        self.each_live_mut(|f| f.to_reconnecting());
    }

    /// First data for `(instrument, datatype)` — its bound book is live.
    pub fn mark_live(&mut self, instrument: &TradingPair, datatype: FeedDatatype) {
        if let Some(f) = self.get_mut(instrument, datatype)
            && !f.state().is_terminal()
            && f.state() != FeedState::Live
        {
            f.to_live();
        }
    }

    /// The bound book for `(instrument, datatype)` gapped; recovery begins.
    pub fn mark_resubscribing(
        &mut self,
        instrument: &TradingPair,
        datatype: FeedDatatype,
    ) {
        if let Some(f) = self.get_mut(instrument, datatype)
            && !f.state().is_terminal()
            && f.state() != FeedState::Resubscribing
        {
            f.to_resubscribing();
        }
    }

    /// Task Stop received; drain begins.
    pub fn mark_all_draining(&mut self) {
        self.each_live_mut(|f| f.to_draining());
    }

    /// Drain finished; `partial` when it exceeded the bound.
    pub fn mark_all_closed(&mut self, partial: bool) {
        self.each_live_mut(|f| {
            if partial {
                f.to_closed_partial();
            } else {
                f.to_closed();
            }
        });
    }

    /// Venue rejected the subscription protocol — terminal for every feed,
    /// each carrying the rejection reason.
    pub fn mark_all_rejected(&mut self, reason: &str) {
        self.each_live_mut(|f| f.to_rejected(reason));
    }

    /// Connection unrecoverable — terminal for every feed, each carrying the
    /// give-up reason.
    pub fn mark_all_failed(&mut self, reason: &str) {
        self.each_live_mut(|f| f.to_failed(reason));
    }

    fn get_mut(
        &mut self,
        instrument: &TradingPair,
        datatype: FeedDatatype,
    ) -> Option<&mut Feed> {
        self.feeds
            .iter_mut()
            .find(|f| f.instrument() == instrument && f.datatype() == datatype)
    }

    /// Point-in-time snapshot for status/telemetry.
    pub fn snapshot(&self) -> Vec<FeedSnapshot> {
        self.feeds
            .iter()
            .map(|f| FeedSnapshot {
                id: f.id().to_string(),
                instrument: f.instrument().to_canonical(),
                datatype: f.datatype(),
                state: f.state(),
                partial: f.closed_partial(),
                reason: f.terminal_reason().map(str::to_string),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feed_allocates_id_and_walks_to_live() {
        let mut f = Feed::new(
            Exchange::Binance,
            TradingPair::new("BTC", "USDT"),
            FeedDatatype::Orders,
        );
        assert_eq!(f.state(), FeedState::Requested);
        let id = f.id();
        f.to_subscribing();
        f.to_live();
        assert_eq!(f.state(), FeedState::Live);
        assert_eq!(f.id(), id); // FeedId stable across transitions
        assert!(!f.state().is_terminal());
    }

    #[test]
    fn feed_ids_are_unique() {
        let a = FeedId::new();
        let b = FeedId::new();
        assert_ne!(a, b);
    }

    fn set() -> FeedSet {
        let pair = TradingPair::new("BTC", "USDT");
        FeedSet::new(vec![
            Feed::new(Exchange::Binance, pair.clone(), FeedDatatype::Orders),
            Feed::new(Exchange::Binance, pair, FeedDatatype::Trades),
        ])
    }

    fn state_of(fs: &FeedSet, dt: FeedDatatype) -> FeedState {
        fs.snapshot()
            .iter()
            .find(|f| f.datatype == dt)
            .unwrap()
            .state
    }

    #[test]
    fn feed_set_walks_the_happy_lifecycle() {
        let pair = TradingPair::new("BTC", "USDT");
        let mut fs = set();
        fs.mark_all_subscribing();
        fs.mark_live(&pair, FeedDatatype::Orders);
        assert_eq!(state_of(&fs, FeedDatatype::Orders), FeedState::Live);
        assert_eq!(state_of(&fs, FeedDatatype::Trades), FeedState::Subscribing);
        fs.mark_resubscribing(&pair, FeedDatatype::Orders);
        assert_eq!(
            state_of(&fs, FeedDatatype::Orders),
            FeedState::Resubscribing
        );
        fs.mark_all_reconnecting();
        fs.mark_all_subscribing();
        fs.mark_live(&pair, FeedDatatype::Trades);
        fs.mark_all_draining();
        fs.mark_all_closed(false);
        assert_eq!(state_of(&fs, FeedDatatype::Orders), FeedState::Closed);
        assert!(!fs.snapshot()[0].partial);
    }

    #[test]
    fn terminal_states_absorb_every_marking() {
        let pair = TradingPair::new("BTC", "USDT");
        let mut fs = set();
        fs.mark_all_subscribing();
        fs.mark_all_reconnecting();
        fs.mark_all_rejected("test rejection");
        fs.mark_all_subscribing();
        fs.mark_live(&pair, FeedDatatype::Orders);
        fs.mark_all_closed(true);
        assert_eq!(state_of(&fs, FeedDatatype::Orders), FeedState::Rejected);
        assert_eq!(state_of(&fs, FeedDatatype::Trades), FeedState::Rejected);
        assert!(!fs.snapshot()[0].partial);
    }

    #[test]
    fn edge_table_matches_the_wired_paths() {
        use FeedState::*;
        let legal = [
            (Requested, Subscribing),
            (Requested, Draining),
            (Subscribing, Live),
            (Subscribing, Resubscribing),
            (Subscribing, Reconnecting),
            (Subscribing, Draining),
            (Live, Resubscribing),
            (Live, Reconnecting),
            (Live, Draining),
            (Resubscribing, Live),
            (Resubscribing, Reconnecting),
            (Resubscribing, Draining),
            (Reconnecting, Subscribing),
            (Reconnecting, Rejected),
            (Reconnecting, Failed),
            (Reconnecting, Draining),
            (Draining, Closed),
        ];
        for (from, to) in legal {
            assert!(
                from.can_transition_to(to),
                "{from:?} -> {to:?} must be legal"
            );
        }
        let all = [
            Requested,
            Subscribing,
            Live,
            Resubscribing,
            Reconnecting,
            Draining,
            Closed,
            Rejected,
            Failed,
        ];
        for terminal in [Closed, Rejected, Failed] {
            for next in all {
                assert!(
                    !terminal.can_transition_to(next),
                    "terminal {terminal:?} must have no outgoing edges"
                );
            }
        }
        for state in all {
            assert!(
                !state.can_transition_to(state),
                "{state:?} self-loop must be illegal"
            );
        }
        assert!(!Draining.can_transition_to(Live));
        assert!(!Requested.can_transition_to(Live));
        assert!(!Reconnecting.can_transition_to(Live));
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "illegal Feed transition")]
    fn illegal_jump_panics_under_debug() {
        let mut f = Feed::new(
            Exchange::Binance,
            TradingPair::new("BTC", "USDT"),
            FeedDatatype::Orders,
        );
        f.to_live();
    }

    #[test]
    fn partial_close_records_the_timed_out_drain() {
        let mut fs = set();
        fs.mark_all_subscribing();
        fs.mark_all_draining();
        fs.mark_all_closed(true);
        assert_eq!(state_of(&fs, FeedDatatype::Orders), FeedState::Closed);
        assert!(fs.snapshot().iter().all(|f| f.partial));
    }
}
