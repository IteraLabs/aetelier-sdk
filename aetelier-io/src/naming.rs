use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDateTime, Utc};

/// `chrono` format of the stamp the per-symbol writers put in file names.
pub const FILE_STAMP_FORMAT: &str = "%Y%m%d_%H%M%S%.6f";

/// Datatype tag of a per-symbol Parquet writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum FileKind {
    /// `ob`
    Orderbook,
    /// `trades`
    Trades,
    /// `liquidations`
    Liquidations,
    /// `funding`
    Funding,
    /// `funding_settlement`
    FundingSettlement,
    /// `oi`
    OpenInterest,
}

impl FileKind {
    /// Every kind, longest tag first so suffix matching is unambiguous.
    pub const ALL: &'static [FileKind] = &[
        FileKind::FundingSettlement,
        FileKind::Liquidations,
        FileKind::Funding,
        FileKind::Trades,
        FileKind::Orderbook,
        FileKind::OpenInterest,
    ];

    /// Tag written into file names.
    pub const fn tag(self) -> &'static str {
        match self {
            FileKind::Orderbook => "ob",
            FileKind::Trades => "trades",
            FileKind::Liquidations => "liquidations",
            FileKind::Funding => "funding",
            FileKind::FundingSettlement => "funding_settlement",
            FileKind::OpenInterest => "oi",
        }
    }
}

/// Components of `{exchange}_{symbol}_{kind}_{mode}_{YYYYMMDD}_{HHMMSS.uuuuuu}[-N].parquet`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct FileName<'a> {
    /// Venue, e.g. `binance`.
    pub exchange: &'a str,
    /// Symbol as written by [`file_symbol`], e.g. `BTC-USDT`.
    pub symbol: &'a str,
    /// Datatype.
    pub kind: FileKind,
    /// Writer mode, e.g. `sync` or `raw`.
    pub mode: &'a str,
    /// Earliest row time; write time for v0.1.0 files and for batches with no positive timestamp.
    pub stamp: DateTime<Utc>,
    /// `N` of a `-N` sibling created on a name collision.
    pub collision: Option<u64>,
}

/// Canonical pair with `/` → `-` and `:` → `_`.
pub fn file_symbol(canonical_pair: &str) -> String {
    canonical_pair.replace('/', "-").replace(':', "_")
}

/// File name a per-symbol writer gives a batch.
pub fn file_name(
    exchange: &str,
    canonical_pair: &str,
    kind: FileKind,
    mode: &str,
    stamp: DateTime<Utc>,
) -> String {
    format!(
        "{exchange}_{}_{}_{mode}_{}.parquet",
        file_symbol(canonical_pair),
        kind.tag(),
        stamp.format(FILE_STAMP_FORMAT)
    )
}

/// Parses a per-symbol writer file name; also accepts the millisecond stamps written by v0.1.0.
pub fn parse_file_name(name: &str) -> Option<FileName<'_>> {
    let stem = name.strip_suffix(".parquet")?;
    let (head, time) = stem.rsplit_once('_')?;
    let (head, date) = head.rsplit_once('_')?;
    let (head, mode) = head.rsplit_once('_')?;
    let (time, collision) = match time.split_once('-') {
        Some((time, n)) => (time, Some(parse_collision(n)?)),
        None => (time, None),
    };
    let digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    if date.len() != 8
        || !matches!(time.len(), 10 | 13)
        || time.as_bytes()[6] != b'.'
        || !digits(date)
        || !digits(&time[..6])
        || !digits(&time[7..])
        || &time[4..6] > "59"
    {
        return None;
    }
    let stamp =
        NaiveDateTime::parse_from_str(&format!("{date}_{time}"), "%Y%m%d_%H%M%S%.f")
            .ok()?
            .and_utc();
    let (rest, kind) = FileKind::ALL.iter().find_map(|&kind| {
        Some((head.strip_suffix(kind.tag())?.strip_suffix('_')?, kind))
    })?;
    let (exchange, symbol) = rest.split_once('_')?;
    (!exchange.is_empty() && !symbol.is_empty() && !mode.is_empty()).then_some(FileName {
        exchange,
        symbol,
        kind,
        mode,
        stamp,
        collision,
    })
}

fn parse_collision(n: &str) -> Option<u64> {
    if n.starts_with('0') || !n.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    n.parse().ok()
}

#[cfg(feature = "parquet")]
pub(crate) fn effective_us(primary: u64, fallback: u64) -> u64 {
    if primary > 0 { primary } else { fallback }
}

#[cfg(feature = "parquet")]
pub(crate) fn batch_stamp<I: IntoIterator<Item = u64>>(ts_us: I) -> DateTime<Utc> {
    ts_us
        .into_iter()
        .filter(|t| *t > 0)
        .min()
        .and_then(|min| i64::try_from(min).ok())
        .and_then(DateTime::from_timestamp_micros)
        .unwrap_or_else(Utc::now)
}

pub(crate) fn unique_path(dir: &Path, filename: &str) -> PathBuf {
    let candidate = dir.join(filename);
    if !candidate.exists() {
        return candidate;
    }
    let stem = filename.strip_suffix(".parquet").unwrap_or(filename);
    let mut n: u64 = 1;
    loop {
        let alt = dir.join(format!("{stem}-{n}.parquet"));
        if !alt.exists() {
            return alt;
        }
        n += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at_us(us: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_micros(us).unwrap()
    }

    #[test]
    fn file_name_round_trips_through_the_parser() {
        let stamp = at_us(1_791_321_067_680_123);
        let name = file_name(
            "hyperliquid",
            "xyz:TSLA/USDC",
            FileKind::FundingSettlement,
            "sync",
            stamp,
        );
        assert_eq!(
            name,
            "hyperliquid_xyz_TSLA-USDC_funding_settlement_sync_20261006_211107.680123.parquet"
        );
        let parsed = parse_file_name(&name).unwrap();
        assert_eq!(
            (
                parsed.exchange,
                parsed.symbol,
                parsed.kind,
                parsed.mode,
                parsed.stamp,
                parsed.collision
            ),
            (
                "hyperliquid",
                "xyz_TSLA-USDC",
                FileKind::FundingSettlement,
                "sync",
                stamp,
                None
            )
        );
    }

    #[test]
    fn every_kind_round_trips() {
        for &kind in FileKind::ALL {
            let name = file_name(
                "binance",
                "BTC/USDT",
                kind,
                "sync",
                at_us(1_791_321_067_680_123),
            );
            assert_eq!(parse_file_name(&name).map(|n| n.kind), Some(kind), "{name}");
        }
    }

    #[test]
    fn parses_collision_siblings() {
        let name = parse_file_name(
            "bybit_BTC-USDT_trades_sync_20261006_212927.433456-2.parquet",
        )
        .unwrap();
        assert_eq!((name.kind, name.collision), (FileKind::Trades, Some(2)));
    }

    #[test]
    fn parses_v0_1_0_millisecond_stamps() {
        let name =
            parse_file_name("binance_BTC-USDT_ob_sync_20261006_211107.680.parquet")
                .unwrap();
        assert_eq!(name.stamp, at_us(1_791_321_067_680_000));
    }

    #[test]
    fn rejects_names_outside_the_contract() {
        for name in [
            "BTCUSDC_ob_sync_1700000000000.parquet",
            "binance_BTC-USDT_ob_sync_20261006_211107.68.parquet",
            "binance_BTC-USDT_ob_sync_20261006_211107.6801234.parquet",
            "binance_BTC-USDT_ob_sync_20261006_2111070.680.parquet",
            "binance_BTC-USDT_ob_sync_20261345_211107.680123.parquet",
            "binance_BTC-USDT_ob_sync_20261006_251107.680123.parquet",
            "binance_BTC-USDT_ob_sync_20261006_211107.680123-0.parquet",
            "binance_BTC-USDT_ob_sync_20261006_211107.680123-01.parquet",
            "binance_BTC-USDT_ob_sync_20261006_211107.680123-+1.parquet",
            "binance_BTC-USDT_ob_sync_20261006_211107.680123-.parquet",
            "binance_BTC-USDT_book_sync_20261006_211107.680123.parquet",
            "binance_BTC-USDT_ob_20261006_211107.680123.parquet",
            "binance_BTC-USDT_trades_rehydrated_20261006_211107.parquet",
            "binance_BTC-USDT_ob_sync_20261006_211107.680123.csv",
            "binance_BTC-USDT_ob_sync_ 2026106_211107.680123.parquet",
            "binance_BTC-USDT_ob_sync_20261006_21 107.680123.parquet",
            "binance_BTC-USDT_ob_sync_20261006_211107. 80123.parquet",
            "binance_BTC-USDT_ob_sync_20261006_211160.680123.parquet",
        ] {
            assert_eq!(parse_file_name(name), None, "{name}");
        }
    }

    #[test]
    fn stamp_derives_from_the_min_positive_timestamp() {
        let stamp = batch_stamp([1_694_854_800_550_000, 1_694_854_800_000_000, 0]);
        assert_eq!(
            stamp.format(FILE_STAMP_FORMAT).to_string(),
            "20230916_090000.000000"
        );
    }

    #[test]
    fn effective_falls_back_when_the_venue_gave_no_timestamp() {
        assert_eq!(effective_us(0, 42), 42);
        assert_eq!(effective_us(7, 42), 7);
    }

    #[test]
    fn all_zero_timestamps_fall_back_to_now_never_epoch() {
        let stamp = batch_stamp([0, 0]);
        assert!(stamp > at_us(1_000_000_000_000_000));
    }

    #[test]
    fn colliding_names_get_a_numbered_sibling() {
        let dir = tempfile::tempdir().unwrap();
        let first = unique_path(dir.path(), "a_b_ob_sync_x.parquet");
        std::fs::write(&first, b"one").unwrap();
        let second = unique_path(dir.path(), "a_b_ob_sync_x.parquet");
        assert_eq!(
            second.file_name().unwrap().to_str().unwrap(),
            "a_b_ob_sync_x-1.parquet"
        );
        std::fs::write(&second, b"two").unwrap();
        let third = unique_path(dir.path(), "a_b_ob_sync_x.parquet");
        assert_eq!(
            third.file_name().unwrap().to_str().unwrap(),
            "a_b_ob_sync_x-2.parquet"
        );
        assert_eq!(std::fs::read(&first).unwrap(), b"one");
    }
}
