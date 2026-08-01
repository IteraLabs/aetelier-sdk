use aetelier_connect::framework::model::DomainEvent;
use aetelier_connect::framework::registry::registry;
use aetelier_types::orderbooks::{L3Order, NormalizedDelta, OrderbookDelta};
use aetelier_types::trades::{Trade, TradeSide};

pub enum LegacyInput<'a> {
    Frame(&'a str),
    RestSeed { body: &'a str, wire_symbol: &'a str },
}

#[derive(Debug, Clone)]
pub struct LegacyBook {
    pub symbol: String,
    pub bids: Vec<(String, String)>,
    pub asks: Vec<(String, String)>,
    pub orders: Vec<L3Order>,
    pub update_id: u64,
    pub sequence: u64,
    pub source_orderbook_ts_us: u64,
    pub checksum: Option<i64>,
    pub is_snapshot: bool,
}

impl LegacyBook {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            bids: Vec::new(),
            asks: Vec::new(),
            orders: Vec::new(),
            update_id: 0,
            sequence: 0,
            source_orderbook_ts_us: 0,
            checksum: None,
            is_snapshot: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LegacyTrade {
    pub id: String,
    pub price: f64,
    pub amount: f64,
    pub side: TradeSide,
    pub source_trade_ts_us: u64,
    pub sequence: Option<u64>,
}

pub struct LegacyDecoded {
    pub books: Vec<LegacyBook>,
    pub trades: Vec<LegacyTrade>,
    pub production_deltas: Option<Vec<NormalizedDelta>>,
}

impl LegacyDecoded {
    pub fn unmapped() -> Self {
        Self {
            books: Vec::new(),
            trades: Vec::new(),
            production_deltas: None,
        }
    }

    pub fn mapped() -> Self {
        Self {
            books: Vec::new(),
            trades: Vec::new(),
            production_deltas: Some(Vec::new()),
        }
    }

    pub fn with_book(mut self, book: LegacyBook) -> Self {
        self.books.push(book);
        self
    }

    pub fn with_trade(mut self, trade: LegacyTrade) -> Self {
        self.trades.push(trade);
        self
    }

    pub fn with_delta(mut self, delta: NormalizedDelta) -> Self {
        if let Some(deltas) = self.production_deltas.as_mut() {
            deltas.push(delta);
        }
        self
    }
}

pub type LegacyShim = fn(LegacyInput<'_>) -> Result<LegacyDecoded, String>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacyDeltaSource {
    NoProductionMapping,
    ProductionToNormalized,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeFanout {
    OnePrintPerFrame,
    BatchedPrints,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LevelCompare {
    ExactWireToken,
    NumericValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Seeding {
    SelfSeedFirstSnapshot,
    RestFixture,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaField {
    Symbol,
    UpdateId,
    Sequence,
    SourceTsUs,
    Checksum,
    Levels,
    Orders,
    IsSnapshot,
}

pub struct VenueParity {
    pub venue: &'static str,
    pub canonical_base: &'static str,
    pub canonical_quote: &'static str,
    pub wire_symbol: &'static str,
    pub ws_fixture: &'static str,
    pub rest_fixture: Option<&'static str>,
    pub shim: LegacyShim,
    pub legacy_delta_source: LegacyDeltaSource,
    pub seeding: Seeding,
    pub legacy_engine_max_depth: Option<usize>,
    pub level_compare: LevelCompare,
    pub declared_divergences: &'static [DeltaField],
    pub trade_fanout: TradeFanout,
    pub min_book_frames: usize,
    pub min_trades: usize,
    pub done: bool,
}

#[derive(Debug)]
pub enum Outcome {
    Pass,
    NotApplicable(String),
    Fail(String),
}

pub type KindFn = fn(&VenueParity) -> Outcome;

pub struct Kind {
    pub name: &'static str,
    pub proves: &'static str,
    pub run: KindFn,
}

pub struct PairedFrame {
    pub index: usize,
    pub legacy: LegacyDecoded,
    pub framework: Vec<DomainEvent>,
}

pub fn fixture_lines(rel: &str) -> Result<Vec<String>, String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("datasets")
        .join(rel);
    let text = std::fs::read_to_string(&path)
        .map_err(|e| format!("fixture {rel} unreadable: {e}"))?;
    Ok(text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect())
}

pub fn fixture_raw(rel: &str) -> Result<String, String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("datasets")
        .join(rel);
    std::fs::read_to_string(&path).map_err(|e| format!("fixture {rel} unreadable: {e}"))
}

pub fn replay_paired(desc: &VenueParity) -> Result<Vec<PairedFrame>, String> {
    let adapter = *registry()
        .get(desc.venue)
        .ok_or_else(|| format!("venue '{}' is not registered", desc.venue))?;
    let lines = fixture_lines(desc.ws_fixture)?;
    if lines.is_empty() {
        return Err(format!("fixture {} has no frames", desc.ws_fixture));
    }
    let mut paired = Vec::with_capacity(lines.len());
    for (index, line) in lines.iter().enumerate() {
        let legacy = (desc.shim)(LegacyInput::Frame(line))
            .map_err(|e| format!("legacy frame {index}: {e}"))?;
        let framework = adapter
            .replay_frame(line)
            .map_err(|e| format!("framework frame {index} failed to decode: {e}"))?
            .into_iter()
            .filter(|ev| !matches!(ev, DomainEvent::ConnectionGap { .. }))
            .collect();
        paired.push(PairedFrame {
            index,
            legacy,
            framework,
        });
    }
    Ok(paired)
}

pub fn framework_books(frame: &PairedFrame) -> Vec<&NormalizedDelta> {
    frame
        .framework
        .iter()
        .filter_map(|ev| match ev {
            DomainEvent::Book(d) => Some(d),
            _ => None,
        })
        .collect()
}

pub fn framework_trades(frame: &PairedFrame) -> Vec<(&Trade, Option<u64>)> {
    frame
        .framework
        .iter()
        .filter_map(|ev| match ev {
            DomainEvent::Trade { trade, sequence } => Some((trade, *sequence)),
            _ => None,
        })
        .collect()
}

fn levels_equal(
    legacy: &[(String, String)],
    framework: &[(String, String)],
    compare: LevelCompare,
) -> bool {
    if legacy.len() != framework.len() {
        return false;
    }
    legacy.iter().zip(framework).all(|(l, f)| match compare {
        LevelCompare::ExactWireToken => l == f,
        LevelCompare::NumericValue => {
            l.0.parse::<f64>().ok() == f.0.parse::<f64>().ok()
                && l.1.parse::<f64>().ok() == f.1.parse::<f64>().ok()
        }
    })
}

fn orders_equal(legacy: &[L3Order], framework: &[L3Order]) -> bool {
    legacy.len() == framework.len()
        && legacy.iter().zip(framework).all(|(l, f)| {
            l.order_id == f.order_id
                && l.is_ask == f.is_ask
                && l.price == f.price
                && l.size == f.size
                && l.removed == f.removed
        })
}

pub fn book_fields_differing(
    legacy: &LegacyBook,
    framework: &NormalizedDelta,
    compare: LevelCompare,
) -> Vec<DeltaField> {
    let mut differing = Vec::new();
    if legacy.symbol != framework.symbol {
        differing.push(DeltaField::Symbol);
    }
    if legacy.update_id != framework.update_id {
        differing.push(DeltaField::UpdateId);
    }
    if legacy.sequence != framework.sequence {
        differing.push(DeltaField::Sequence);
    }
    if legacy.source_orderbook_ts_us != framework.source_orderbook_ts_us {
        differing.push(DeltaField::SourceTsUs);
    }
    if legacy.checksum != framework.checksum {
        differing.push(DeltaField::Checksum);
    }
    if !levels_equal(&legacy.bids, &framework.bids, compare)
        || !levels_equal(&legacy.asks, &framework.asks, compare)
    {
        differing.push(DeltaField::Levels);
    }
    if !orders_equal(&legacy.orders, &framework.orders) {
        differing.push(DeltaField::Orders);
    }
    if legacy.is_snapshot != framework.is_snapshot {
        differing.push(DeltaField::IsSnapshot);
    }
    differing
}

pub fn delta_fields_differing(
    legacy: &NormalizedDelta,
    framework: &NormalizedDelta,
    compare: LevelCompare,
) -> Vec<DeltaField> {
    let projected = LegacyBook {
        symbol: legacy.symbol.clone(),
        bids: legacy.bids.clone(),
        asks: legacy.asks.clone(),
        orders: legacy.orders.clone(),
        update_id: legacy.update_id,
        sequence: legacy.sequence,
        source_orderbook_ts_us: legacy.source_orderbook_ts_us,
        checksum: legacy.checksum,
        is_snapshot: legacy.is_snapshot,
    };
    book_fields_differing(&projected, framework, compare)
}

pub fn books_equal(a: &OrderbookDelta, b: &OrderbookDelta) -> bool {
    a.top_bids(usize::MAX) == b.top_bids(usize::MAX)
        && a.top_asks(usize::MAX) == b.top_asks(usize::MAX)
}

pub fn assert_kind(desc: &VenueParity, kind: &Kind) {
    if registry().get(desc.venue).is_none() {
        panic!("venue '{}' is not registered", desc.venue);
    }
    match (kind.run)(desc) {
        Outcome::Pass => {}
        Outcome::NotApplicable(reason) => {
            eprintln!("n/a  {}::{} — {reason}", desc.venue, kind.name);
        }
        Outcome::Fail(reason) => {
            panic!("{}::{} FAILED — {reason}", desc.venue, kind.name);
        }
    }
}
