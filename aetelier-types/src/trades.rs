use crate::errors::BuildError;
use std::fmt;
use std::str::FromStr;

use rand::prelude::IndexedRandom;
use rand::{Rng, rng};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::TimestampUs;
use crate::orderbooks::f64_to_decimal;
use crate::trading_pair::TradingPair;

/// Taker side of a trade or liquidation.
///
/// This is intentionally separate from [`OrderSide`](crate::orders::OrderSide),
/// which uses book-side semantics (`Bids` / `Asks`).  `TradeSide` represents
/// the direction of the taker — the party that *crossed* the spread.
///
/// Serialized as `"buy"` / `"sell"` (lowercase) via serde.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TradeSide {
    /// Taker was buying (lifted an ask).
    Buy,
    /// Taker was selling (hit a bid).
    Sell,
}

impl fmt::Display for TradeSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TradeSide::Buy => write!(f, "buy"),
            TradeSide::Sell => write!(f, "sell"),
        }
    }
}

impl TradeSide {
    /// Return the lowercase string representation (`"buy"` or `"sell"`).
    ///
    /// Unlike [`std::fmt::Display`], this returns a `&'static str` and is free to
    /// call — useful for columnar writers (Parquet, CSV) that collect
    /// `&str` slices.
    pub fn as_str(&self) -> &'static str {
        match self {
            TradeSide::Buy => "buy",
            TradeSide::Sell => "sell",
        }
    }

    /// Parse a trade side from a loose string, returning `None` for
    /// unrecognised input.
    ///
    /// Handles the various casings that exchanges use on the wire:
    /// `"buy"`, `"Buy"`, `"BUY"`, `"sell"`, `"Sell"`, `"SELL"`.
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "buy" => Some(TradeSide::Buy),
            "sell" => Some(TradeSide::Sell),
            _ => None,
        }
    }

    /// Return a pseudo-random `TradeSide`.
    pub fn random() -> Self {
        let sides = [TradeSide::Buy, TradeSide::Sell];
        let mut rng = rng();
        *sides.choose(&mut rng).expect("non-empty slice")
    }
}

impl FromStr for TradeSide {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        TradeSide::from_str_loose(s)
            .ok_or_else(|| format!("unsupported trade side: {}", s))
    }
}

/// How a trade print reached this process — the provenance marker the
/// completeness product stamps on every print.
///
/// `Ws` (the default) is the live WebSocket stream. `Rest` marks a print
/// recovered from the venue's REST trades endpoint by live reconciliation or
/// batch rehydration; downstream consumers can filter or weight by origin.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TradeOrigin {
    /// Delivered by the live WebSocket stream (the default).
    #[default]
    Ws,
    /// Recovered from the venue's REST trades endpoint (live reconciliation
    /// or batch rehydration).
    Rest,
}

impl TradeOrigin {
    /// Stable string form (persisted in the Parquet `origin` column).
    pub fn as_str(&self) -> &'static str {
        match self {
            TradeOrigin::Ws => "ws",
            TradeOrigin::Rest => "rest",
        }
    }
}

impl std::str::FromStr for TradeOrigin {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ws" => Ok(TradeOrigin::Ws),
            "rest" => Ok(TradeOrigin::Rest),
            other => Err(format!("unknown trade origin '{other}'")),
        }
    }
}

/// A single public trade observed on an exchange.
///
/// Trades are the most granular market event, recording every
/// fill on the matching engine.  This is the **exchange-agnostic**
/// normalised representation — exchange-specific wire formats are
/// mapped into `Trade` by the per-exchange client implementations.
///
/// Timestamps are in Unix microseconds as reported by the exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    /// Exchange-reported trade (match) time in Unix µs. Renamed from `trade_ts`.
    pub source_trade_ts_us: u64,
    /// Local receipt time in Unix µs, stamped by the transport driver the
    /// instant the frame is read off the socket. 0 until stamped.
    pub local_trade_ts_us: u64,
    /// Ping/pong round-trip on this connection in microseconds (0 if the venue
    /// has no measurable RTT yet). Stamped centrally by the transport driver.
    pub source_trade_rtt_us: u64,
    /// Canonical trading pair (e.g. `SOL/USDT`).
    pub pair: TradingPair,
    /// Taker side.
    pub side: TradeSide,
    /// Filled quantity in base-currency units. Exact decimal in memory;
    /// serialized as a JSON float and persisted as Parquet `Float64`.
    #[serde(with = "rust_decimal::serde::float")]
    pub amount: Decimal,
    /// Execution price in quote-currency units. Exact decimal in memory;
    /// serialized as a JSON float and persisted as Parquet `Float64`.
    #[serde(with = "rust_decimal::serde::float")]
    pub price: Decimal,
    /// Exchange name.
    pub exchange: String,
    /// Exchange-assigned unique trade identifier.
    pub id: String,
    /// Provenance: live WebSocket (default) or REST-recovered. `serde(default)`
    /// keeps every existing wire/JSON producer valid.
    #[serde(default)]
    pub origin: TradeOrigin,
}

impl Trade {
    /// Create a new [`TradeBuilder`].
    pub fn builder() -> TradeBuilder {
        TradeBuilder::new()
    }

    /// Generate a random `Trade` for testing and simulation.
    ///
    /// Produces a trade with the current wall-clock timestamp, a
    /// randomly chosen side and exchange, and price / amount drawn
    /// from uniform distributions.  The `symbol` and `id` fields are
    /// left as empty strings.
    pub fn random() -> Self {
        let r_ts = TimestampUs::now().as_micros();

        let mut rng = rng();
        let r_side = TradeSide::random();

        let exchanges = ["bybit", "kraken", "coinbase", "binance"];

        let r_pair = TradingPair::new("BTC", "USDT");
        let r_amount = rng.random_range(0.01..1.10);
        let r_price = rng.random_range(100_000.0..110_000.0);
        let r_exchange = exchanges
            .choose(&mut rng)
            .expect("Error in random side choice")
            .to_string();
        let r_id = "".to_string();

        Self {
            source_trade_ts_us: r_ts,
            local_trade_ts_us: 0,
            source_trade_rtt_us: 0,
            pair: r_pair,
            side: r_side,
            amount: f64_to_decimal(r_amount),
            price: f64_to_decimal(r_price),
            exchange: r_exchange,
            id: r_id,
            origin: TradeOrigin::default(),
        }
    }
}

/// Builder for constructing a [`Trade`] with validated fields.
///
/// Every field is required.  Calling [`build()`](Self::build) with any
/// field missing returns `Err(String)` naming the absent field.
///
/// # Example
///
/// ```rust,ignore
/// let trade = Trade::builder()
///     .trade_ts(1_700_000_000_000)
///     .pair(TradingPair::new("BTC", "USDT"))
///     .side(TradeSide::Buy)
///     .amount(0.5)
///     .price(42_000.0)
///     .exchange("bybit".into())
///     .id("abc123".into())
///     .build()
///     .expect("all fields set");
/// ```
#[derive(Debug, Clone)]
pub struct TradeBuilder {
    /// Exchange trade (match) time in Unix microseconds.
    pub source_trade_ts_us: Option<u64>,
    /// Canonical trading pair.
    pub pair: Option<TradingPair>,
    /// Taker side.
    pub side: Option<TradeSide>,
    /// Filled quantity in base-currency units.
    pub amount: Option<f64>,
    /// Execution price in quote-currency units.
    pub price: Option<f64>,
    /// Exchange name.
    pub exchange: Option<String>,
    /// Exchange-assigned trade identifier.
    pub id: Option<String>,
}

impl Default for TradeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TradeBuilder {
    /// Create an empty builder with all fields set to `None`.
    pub fn new() -> Self {
        TradeBuilder {
            source_trade_ts_us: None,
            pair: None,
            side: None,
            amount: None,
            price: None,
            exchange: None,
            id: None,
        }
    }

    /// Set the exchange trade (match) time (Unix µs).
    pub fn source_trade_ts_us(mut self, source_trade_ts_us: u64) -> Self {
        self.source_trade_ts_us = Some(source_trade_ts_us);
        self
    }

    /// Set the canonical trading pair.
    pub fn pair(mut self, pair: TradingPair) -> Self {
        self.pair = Some(pair);
        self
    }

    /// Set the taker side.
    pub fn side(mut self, side: TradeSide) -> Self {
        self.side = Some(side);
        self
    }

    /// Set the filled quantity in base-currency units.
    pub fn amount(mut self, amount: f64) -> Self {
        self.amount = Some(amount);
        self
    }

    /// Set the execution price in quote-currency units.
    pub fn price(mut self, price: f64) -> Self {
        self.price = Some(price);
        self
    }

    /// Set the exchange name (e.g. `"bybit"`).
    pub fn exchange(mut self, exchange: String) -> Self {
        self.exchange = Some(exchange);
        self
    }

    /// Set the exchange-assigned trade identifier.
    pub fn id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }

    /// Consume the builder and produce a [`Trade`].
    ///
    /// # Errors
    ///
    /// Returns [`BuildError`] if any required field is missing.
    pub fn build(self) -> Result<Trade, BuildError> {
        let source_trade_ts_us = self
            .source_trade_ts_us
            .ok_or(BuildError::MissingField("source_trade_ts_us"))?;
        let pair = self.pair.ok_or(BuildError::MissingField("pair"))?;
        let side = self.side.ok_or(BuildError::MissingField("side"))?;
        let amount = self.amount.ok_or(BuildError::MissingField("amount"))?;
        let price = self.price.ok_or(BuildError::MissingField("price"))?;
        let exchange = self.exchange.ok_or(BuildError::MissingField("exchange"))?;
        let id = self.id.ok_or(BuildError::MissingField("id"))?;

        Ok(Trade {
            source_trade_ts_us,
            local_trade_ts_us: 0,
            source_trade_rtt_us: 0,
            pair,
            side,
            // The builder accepts f64 for call-site convenience (venue
            // normalizers parse wire values as f64); the exact decimal is
            // retained here via `from_f64_retain`.
            amount: f64_to_decimal(amount),
            price: f64_to_decimal(price),
            exchange,
            id,
            origin: TradeOrigin::default(),
        })
    }
}

// ─── Identity and integrity ─────────────────────────────────────────────────

/// The fields that decide whether two records describe the same venue event.
///
/// A venue reports one row per resting order consumed, so a single taker
/// order arrives as several fills sharing one match-engine timestamp.
/// Timestamp equality is therefore evidence of simultaneity, never of
/// duplication, at any resolution — including a microsecond stamp truncated
/// to milliseconds and compared afterwards.
///
/// `source_trade_ts_us` is deliberately absent. An identity key must be
/// immutable, and event timestamps are normalized in practice: unit
/// migrations and legacy-fixture shims rewrite them, and a key that moves
/// re-identifies every row it describes. Timestamp belongs in
/// [`TradeFingerprint`], where a change is the signal rather than the defect.
///
/// `market_type` is a lineage literal applied at the storage boundary and is
/// constant within an in-memory batch, so the in-memory key is
/// `(exchange, pair, id)`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TradeIdentity {
    /// Exchange name.
    pub exchange: String,
    /// Canonical trading pair.
    pub pair: TradingPair,
    /// Exchange-assigned trade identifier.
    pub id: String,
}

/// Content fingerprint of a trade: the fields that decide whether two records
/// sharing a [`TradeIdentity`] carry the same content.
///
/// Answers a different question from identity. Identity asks whether two
/// records describe the same event; the fingerprint asks whether they agree
/// on what that event was. A mismatch under one identity means the venue
/// restated the print or a pipeline stage rewrote it — both invisible to
/// identity-only deduplication, which keeps whichever copy arrived first.
///
/// FNV-1a over the field bytes: stable across releases and processes, unlike
/// [`std::hash::DefaultHasher`], and dependency-free.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TradeFingerprint(pub u64);

impl TradeFingerprint {
    /// Fingerprint the content of `trade`: timestamp, side, price, amount.
    pub fn of(trade: &Trade) -> Self {
        const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const PRIME: u64 = 0x0000_0100_0000_01b3;

        let mut h = OFFSET;
        let mut eat = |bytes: &[u8]| {
            for b in bytes {
                h ^= *b as u64;
                h = h.wrapping_mul(PRIME);
            }
        };

        eat(&trade.source_trade_ts_us.to_le_bytes());
        eat(trade.side.as_str().as_bytes());
        // Decimal's normalized string form: exact, and blind to trailing
        // zeros that carry no numeric meaning.
        eat(trade.price.normalize().to_string().as_bytes());
        eat(trade.amount.normalize().to_string().as_bytes());

        TradeFingerprint(h)
    }
}

/// Outcome of one [`dedup_by_identity`] pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TradeDedupReport {
    /// Rows removed because an earlier row carried the same identity.
    pub removed: usize,
    /// Removed rows whose fingerprint differed from the retained copy. A
    /// non-zero count means one identity described two different events:
    /// a venue restatement, or a stage that rewrote a field in flight.
    pub conflicts: usize,
}

/// Borrowed form of [`TradeIdentity`] for paths that key many rows at once
/// and must not clone. Same fields, same meaning — the owned and borrowed
/// forms share one definition of what identity is.
pub type TradeIdentityRef<'a> = (&'a str, &'a TradingPair, &'a str);

impl Trade {
    /// This trade's identity key.
    pub fn identity(&self) -> TradeIdentity {
        TradeIdentity {
            exchange: self.exchange.clone(),
            pair: self.pair.clone(),
            id: self.id.clone(),
        }
    }

    /// This trade's identity key, borrowed.
    pub fn identity_ref(&self) -> TradeIdentityRef<'_> {
        (&self.exchange, &self.pair, &self.id)
    }

    /// This trade's content fingerprint.
    pub fn fingerprint(&self) -> TradeFingerprint {
        TradeFingerprint::of(self)
    }
}

// ─── Aggregation ────────────────────────────────────────────────────────────

/// Version of the aggregation rule set that produced a [`TradeAggregate`].
///
/// Recorded on every aggregate so a fit stays reproducible against the rules
/// in force when it ran.
pub const AGGREGATION_RULE_VERSION: &str = "trade-aggregation-v0.11";

/// Which ordering evidence the venue supplied, best first.
///
/// Levels below [`NumericId`](TradeOrderEvidence::NumericId) are asserted by
/// the pipeline rather than reported by the venue, so an aggregate carrying
/// one is ordered on weaker grounds than the exchange's own sequencing.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradeOrderEvidence {
    /// A venue sequence number distinct from the trade id.
    VenueSequence,
    /// The trade id parses as an integer and counts upward.
    NumericId,
    /// The trade id orders only as text.
    LexicographicId,
    /// Position in the input stream: no venue evidence at all.
    ArrivalIndex,
}

impl TradeOrderEvidence {
    /// Whether the venue itself established this order.
    ///
    /// False for the two weakest levels, where the ordering is the
    /// pipeline's assertion and any sequencing built on it inherits that.
    pub fn is_venue_reported(&self) -> bool {
        matches!(self, Self::VenueSequence | Self::NumericId)
    }
}

/// Why a group of fills became its own aggregate.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradeSplitReason {
    /// One fill: the aggregate is the fill.
    Single,
    /// Every fill executed at one price — one taker consuming several
    /// resting orders at a single book level.
    SamePrice,
    /// Prices walked in the side's sweep direction — one taker consuming
    /// successive book levels.
    Swept,
    /// The price sequence reversed against the side's direction, which one
    /// taker cannot do, so a second taker starts here.
    DirectionSplit,
}

/// One taker order, reconstructed from the fills it produced.
///
/// A venue reports one row per resting order consumed, so a single taker
/// order arrives as several [`Trade`] fills sharing a match-engine timestamp.
/// A `TradeAggregate` is that taker order: the arrival a point-process model
/// should see, with the fills' footprint preserved as marks.
///
/// Every derived field is computed by [`TradeAggregate::from_fills`] rather
/// than assigned, so `vwap`, `sweep_depth` and the price extent cannot drift
/// from the fills they summarize.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeAggregate {
    /// Event time in UTC epoch microseconds. Equal to `bucket_us` until the
    /// caller places the aggregate within its bucket (see
    /// [`place_at`](TradeAggregate::place_at)).
    pub ts_us: u64,
    /// Start of the resolution bucket these fills fell in, epoch microseconds.
    pub bucket_us: u64,
    /// Width of that bucket in microseconds: the resolution the source
    /// expresses, and the span across which tied aggregates are placed.
    pub w_us: u64,
    /// Exchange name.
    pub exchange: String,
    /// Canonical trading pair.
    pub pair: TradingPair,
    /// Taker side, uniform across the fills by construction.
    pub side: TradeSide,
    /// Number of fills consumed.
    pub n_fills: usize,
    /// Total filled quantity in base-currency units.
    #[serde(with = "rust_decimal::serde::float")]
    pub qty: Decimal,
    /// Total quote-currency value: the sum of price times amount.
    #[serde(with = "rust_decimal::serde::float")]
    pub notional: Decimal,
    /// Volume-weighted average price: `notional / qty`.
    #[serde(with = "rust_decimal::serde::float")]
    pub vwap: Decimal,
    /// Price of the first fill in order.
    #[serde(with = "rust_decimal::serde::float")]
    pub px_first: Decimal,
    /// Price of the last fill in order.
    #[serde(with = "rust_decimal::serde::float")]
    pub px_last: Decimal,
    /// Lowest fill price.
    #[serde(with = "rust_decimal::serde::float")]
    pub px_min: Decimal,
    /// Highest fill price.
    #[serde(with = "rust_decimal::serde::float")]
    pub px_max: Decimal,
    /// How far the taker walked the book: `|px_last - px_first|`. Zero
    /// when every fill executed at one level.
    #[serde(with = "rust_decimal::serde::float")]
    pub sweep_depth: Decimal,
    /// Trade id of the first fill — provenance back to the raw rows.
    pub id_first: String,
    /// Trade id of the last fill.
    pub id_last: String,
    /// Ordering evidence the fills were sequenced on.
    pub order_evidence: TradeOrderEvidence,
    /// Why this group became its own aggregate.
    pub split_reason: TradeSplitReason,
    /// Rule set that produced it.
    pub rule_version: String,
}

impl TradeAggregate {
    /// Reduce the fills of one taker order into an aggregate.
    ///
    /// `fills` must be non-empty, in the order established by
    /// `order_evidence`, and uniform in side, exchange and pair — the
    /// grouping rules guarantee all three, and each is rejected here rather
    /// than silently summarized.
    ///
    /// `ts_us` starts at `bucket_us`; call [`place_at`](Self::place_at) once
    /// the number of aggregates sharing the bucket is known.
    pub fn from_fills(
        fills: &[&Trade],
        bucket_us: u64,
        w_us: u64,
        split_reason: TradeSplitReason,
        order_evidence: TradeOrderEvidence,
    ) -> Result<Self, BuildError> {
        let first = fills.first().ok_or(BuildError::InvalidField {
            field: "fills",
            reason: "an aggregate needs at least one fill".into(),
        })?;

        if w_us == 0 {
            return Err(BuildError::InvalidField {
                field: "w_us",
                reason: "bucket width must be positive".into(),
            });
        }

        if fills.iter().any(|f| {
            f.side != first.side || f.exchange != first.exchange || f.pair != first.pair
        }) {
            return Err(BuildError::InvalidField {
                field: "fills",
                reason: "fills of one taker share side, exchange and pair".into(),
            });
        }

        let mut qty = Decimal::ZERO;
        let mut notional = Decimal::ZERO;
        let mut px_min = first.price;
        let mut px_max = first.price;
        for f in fills {
            qty += f.amount;
            notional += f.price * f.amount;
            px_min = px_min.min(f.price);
            px_max = px_max.max(f.price);
        }

        if qty <= Decimal::ZERO {
            return Err(BuildError::InvalidField {
                field: "qty",
                reason: format!("aggregate quantity must be positive, got {qty}"),
            });
        }

        let last = fills[fills.len() - 1];
        let px_first = first.price;
        let px_last = last.price;

        Ok(Self {
            ts_us: bucket_us,
            bucket_us,
            w_us,
            exchange: first.exchange.clone(),
            pair: first.pair.clone(),
            side: first.side,
            n_fills: fills.len(),
            qty,
            notional,
            vwap: notional / qty,
            px_first,
            px_last,
            px_min,
            px_max,
            sweep_depth: (px_last - px_first).abs(),
            id_first: first.id.clone(),
            id_last: last.id.clone(),
            order_evidence,
            split_reason,
            rule_version: AGGREGATION_RULE_VERSION.into(),
        })
    }

    /// Place this aggregate at `ts_us` within its bucket.
    ///
    /// The `k`-th of `m` aggregates sharing a bucket sits at
    /// `bucket_us + w_us * (k - 1) / m`, so a lone aggregate keeps the
    /// timestamp the venue reported and the rest spread across the span the
    /// source could not resolve.
    pub fn place_at(mut self, ts_us: u64) -> Self {
        self.ts_us = ts_us;
        self
    }

    /// Whether this aggregate's ordering rests on venue-reported evidence.
    pub fn is_venue_ordered(&self) -> bool {
        self.order_evidence.is_venue_reported()
    }
}

/// Remove records that repeat an identity already seen, keeping the first
/// occurrence in the given order.
///
/// Order-dependent by construction: sort the input first when the caller
/// needs a deterministic winner. Ties in timestamp never remove anything —
/// only a repeated `(exchange, pair, id)` does.
pub fn dedup_by_identity(trades: &mut Vec<Trade>) -> TradeDedupReport {
    use std::collections::HashMap;

    let mut seen: HashMap<TradeIdentity, TradeFingerprint> =
        HashMap::with_capacity(trades.len());
    let mut report = TradeDedupReport::default();

    trades.retain(|t| {
        let identity = t.identity();
        let fingerprint = t.fingerprint();
        match seen.get(&identity) {
            Some(kept) => {
                report.removed += 1;
                if *kept != fingerprint {
                    report.conflicts += 1;
                }
                false
            }
            None => {
                seen.insert(identity, fingerprint);
                true
            }
        }
    });

    report
}
