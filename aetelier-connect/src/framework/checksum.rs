//! Order-book checksum verification for ChecksumDelta venues.
//!
//! After applying a delta, the running book is hashed with the venue's recipe
//! and compared to the venue-reported checksum; a mismatch means a delta was
//! lost or misapplied and the book must be re-seeded. The recipes here are
//! validated against real captured OKX and Kraken frames (see the tests).

use aetelier_types::orderbooks::OrderbookDelta;
use rust_decimal::Decimal;

use super::model::ChecksumFmt;

/// Compute the venue checksum over the current top-of-book, to compare against
/// the venue-reported checksum (`NormalizedDelta.checksum`). OKX yields a signed
/// i32 (widened to i64); Kraken an unsigned u32 (widened to i64).
pub fn book_checksum(fmt: &ChecksumFmt, book: &OrderbookDelta) -> i64 {
    match fmt {
        // Bitget's classic CRC32 channel shared OKX's recipe; the live `books`
        // channel has since moved to seq/pseq, so this arm is unused in practice.
        ChecksumFmt::OkxTop25 | ChecksumFmt::BitgetBidFirstTop25 => okx_top25(book),
        ChecksumFmt::KrakenTop10 => kraken_top10(book),
    }
}

/// OKX `books`: CRC32 of the top-25 levels interleaved bid-then-ask per rank
/// (`bidPx:bidSz:askPx:askSz:…`), as a signed i32.
fn okx_top25(book: &OrderbookDelta) -> i64 {
    let bids = book.top_bids(25);
    let asks = book.top_asks(25);
    let mut parts: Vec<String> = Vec::with_capacity(100);
    for i in 0..25 {
        if let Some((p, s)) = bids.get(i) {
            parts.push(p.to_string());
            parts.push(s.to_string());
        }
        if let Some((p, s)) = asks.get(i) {
            parts.push(p.to_string());
            parts.push(s.to_string());
        }
    }
    crc32fast::hash(parts.join(":").as_bytes()) as i32 as i64
}

/// Kraken v2 `book`: CRC32 (unsigned) of the top-10 asks then top-10 bids, each
/// price and qty with the decimal point and leading zeros removed. The book
/// must be held at the subscribed depth (10).
fn kraken_top10(book: &OrderbookDelta) -> i64 {
    let asks = book.top_asks(10);
    let bids = book.top_bids(10);
    let mut s = String::new();
    for (p, q) in asks.iter().chain(bids.iter()) {
        push_kraken(&mut s, p);
        push_kraken(&mut s, q);
    }
    i64::from(crc32fast::hash(s.as_bytes()))
}

/// Append `d` with the decimal point and leading zeros stripped (Kraken digits).
fn push_kraken(out: &mut String, d: &Decimal) {
    let mut started = false;
    for c in d.to_string().chars() {
        if c == '.' {
            continue;
        }
        if !started && c == '0' {
            continue;
        }
        started = true;
        out.push(c);
    }
    if !started {
        out.push('0');
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aetelier_types::orderbooks::NormalizedDelta;
    use aetelier_types::trading_pair::TradingPair;
    use serde::Deserialize;

    const OKX: &str = include_str!("../../datasets/okx/books_btcusdt.jsonl");
    const KRAKEN: &str = include_str!("../../datasets/kraken/books_btcusd.jsonl");

    fn nd(
        symbol: &str,
        bids: Vec<(String, String)>,
        asks: Vec<(String, String)>,
        is_snapshot: bool,
    ) -> NormalizedDelta {
        NormalizedDelta {
            symbol: symbol.into(),
            bids,
            asks,
            update_id: 0,
            sequence: 0,
            source_orderbook_ts_us: 0,
            local_orderbook_ts_us: 0,
            source_orderbook_rtt_us: 0,
            checksum: None,
            orders: Vec::new(),
            is_snapshot,
        }
    }

    // ── OKX: wire levels are already strings ──────────────────────────────
    #[derive(Deserialize)]
    struct OkxFrame {
        #[serde(default)]
        action: Option<String>,
        #[serde(default)]
        data: Vec<OkxData>,
    }
    #[derive(Deserialize)]
    struct OkxData {
        #[serde(default)]
        bids: Vec<[String; 4]>,
        #[serde(default)]
        asks: Vec<[String; 4]>,
        #[serde(default)]
        checksum: Option<i64>,
    }

    #[test]
    fn okx_checksum_matches_real_frames() {
        let pair = TradingPair::new("BTC", "USDT");
        let mut book = OrderbookDelta::new(pair);
        let mut verified = 0;
        for line in OKX.lines() {
            let Ok(frame) = serde_json::from_str::<OkxFrame>(line.trim()) else {
                continue;
            };
            let is_snapshot = matches!(frame.action.as_deref(), Some("snapshot"));
            for d in &frame.data {
                let lv = |rows: &[[String; 4]]| -> Vec<(String, String)> {
                    rows.iter().map(|r| (r[0].clone(), r[1].clone())).collect()
                };
                let delta = nd("BTC-USDT", lv(&d.bids), lv(&d.asks), is_snapshot);
                book.process(&delta).unwrap();
                if let Some(ck) = d.checksum {
                    assert_eq!(
                        book_checksum(&ChecksumFmt::OkxTop25, &book),
                        ck,
                        "OKX checksum mismatch"
                    );
                    verified += 1;
                }
            }
        }
        assert!(
            verified > 30,
            "expected many verified frames, got {verified}"
        );
    }

    // ── Kraken: levels are precision-significant numbers; preserve the token ──
    fn de_num_str<'de, D: serde::Deserializer<'de>>(d: D) -> Result<String, D::Error> {
        let raw: Box<serde_json::value::RawValue> = Deserialize::deserialize(d)?;
        Ok(raw.get().to_string())
    }
    #[derive(Deserialize)]
    struct KFrame {
        #[serde(default)]
        channel: Option<String>,
        #[serde(rename = "type", default)]
        ty: Option<String>,
        #[serde(default)]
        data: Vec<KData>,
    }
    #[derive(Deserialize)]
    struct KData {
        #[serde(default)]
        bids: Vec<KLvl>,
        #[serde(default)]
        asks: Vec<KLvl>,
        #[serde(default)]
        checksum: Option<u64>,
    }
    #[derive(Deserialize)]
    struct KLvl {
        #[serde(deserialize_with = "de_num_str")]
        price: String,
        #[serde(deserialize_with = "de_num_str")]
        qty: String,
    }

    #[test]
    fn kraken_checksum_matches_real_frames() {
        let pair = TradingPair::new("BTC", "USD");
        // Kraken depth=10: hold the book at the subscribed depth.
        let mut book = OrderbookDelta::new(pair).with_max_depth(Some(10));
        let mut verified = 0;
        for line in KRAKEN.lines() {
            let Ok(frame) = serde_json::from_str::<KFrame>(line.trim()) else {
                continue;
            };
            if frame.channel.as_deref() != Some("book") {
                continue;
            }
            let is_snapshot = matches!(frame.ty.as_deref(), Some("snapshot"));
            for d in &frame.data {
                let lv = |rows: &[KLvl]| -> Vec<(String, String)> {
                    rows.iter()
                        .map(|r| (r.price.clone(), r.qty.clone()))
                        .collect()
                };
                let delta = nd("BTC/USD", lv(&d.bids), lv(&d.asks), is_snapshot);
                book.process(&delta).unwrap();
                if let Some(ck) = d.checksum {
                    assert_eq!(
                        book_checksum(&ChecksumFmt::KrakenTop10, &book),
                        ck as i64,
                        "Kraken checksum mismatch"
                    );
                    verified += 1;
                }
            }
        }
        assert!(
            verified > 100,
            "expected many verified frames, got {verified}"
        );
    }
}
