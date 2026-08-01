use aetelier_connect::clients::wss::WssDecoder;
use aetelier_types::orderbooks::L3Order;
use aetelier_types::trades::TradeSide;

use super::harness::{LegacyBook, LegacyDecoded, LegacyInput, LegacyTrade};

fn ms_to_us(ms: u64) -> u64 {
    if ms == 0 { 0 } else { ms * 1_000 }
}

fn ns_to_us(ns: u64) -> u64 {
    ns / 1_000
}

fn rfc3339_to_us(text: &str) -> u64 {
    chrono::DateTime::parse_from_rfc3339(text)
        .map(|t| t.timestamp_micros() as u64)
        .unwrap_or(0)
}

fn side_from(text: &str) -> Result<TradeSide, String> {
    TradeSide::from_str_loose(text).ok_or_else(|| format!("unknown taker side '{text}'"))
}

fn number(text: &str) -> Result<f64, String> {
    text.parse::<f64>()
        .map_err(|e| format!("unparseable number '{text}': {e}"))
}

fn pairs(levels: &[[String; 2]]) -> Vec<(String, String)> {
    levels
        .iter()
        .map(|l| (l[0].clone(), l[1].clone()))
        .collect()
}

pub fn binance(input: LegacyInput<'_>) -> Result<LegacyDecoded, String> {
    use aetelier_connect::sources::binance::decoder::BinanceDecoder;
    use aetelier_connect::sources::binance::events::BinanceWssEvent;
    use aetelier_connect::sources::binance::responses::orderbooks::BinanceDepthSnapshot;

    match input {
        LegacyInput::RestSeed { body, wire_symbol } => {
            let snapshot: BinanceDepthSnapshot = serde_json::from_str(body)
                .map_err(|e| format!("rest snapshot parse: {e}"))?;
            Ok(LegacyDecoded::mapped().with_delta(snapshot.to_normalized(wire_symbol)))
        }
        LegacyInput::Frame(raw) => {
            let decoded = BinanceDecoder::decode(raw).map_err(|e| e.to_string())?;
            let Some(event) = decoded else {
                return Ok(LegacyDecoded::mapped());
            };
            match event {
                BinanceWssEvent::DepthUpdate(u) => {
                    let mut book = LegacyBook::new(u.symbol.clone());
                    book.bids = pairs(&u.bids);
                    book.asks = pairs(&u.asks);
                    book.update_id = u.last_update_id;
                    book.sequence = u.first_update_id;
                    book.source_orderbook_ts_us = ms_to_us(u.event_time);
                    Ok(LegacyDecoded::mapped()
                        .with_delta(u.to_normalized())
                        .with_book(book))
                }
                BinanceWssEvent::TradeData(t) => {
                    Ok(LegacyDecoded::mapped().with_trade(LegacyTrade {
                        id: t.trade_id.to_string(),
                        price: number(&t.price)?,
                        amount: number(&t.quantity)?,
                        side: side_from(t.taker_side())?,
                        source_trade_ts_us: ms_to_us(t.trade_time),
                        sequence: Some(t.trade_id),
                    }))
                }
                BinanceWssEvent::DepthSnapshot(_) => Ok(LegacyDecoded::mapped()),
            }
        }
    }
}

pub fn bybit(input: LegacyInput<'_>) -> Result<LegacyDecoded, String> {
    use aetelier_connect::sources::bybit::decoder::BybitDecoder;
    use aetelier_connect::sources::bybit::events::BybitWssEvent;

    let LegacyInput::Frame(raw) = input else {
        return Err("bybit has no REST seed".into());
    };
    let Some(event) = BybitDecoder::decode(raw).map_err(|e| e.to_string())? else {
        return Ok(LegacyDecoded::mapped());
    };
    match event {
        BybitWssEvent::OrderbookData(resp) => {
            let mut book = LegacyBook::new(resp.data.symbol.clone());
            book.bids = resp
                .data
                .bids
                .iter()
                .map(|l| (l.0.clone(), l.1.clone()))
                .collect();
            book.asks = resp
                .data
                .asks
                .iter()
                .map(|l| (l.0.clone(), l.1.clone()))
                .collect();
            book.update_id = resp.data.update_id;
            book.sequence = resp.data.sequence;
            book.source_orderbook_ts_us =
                ms_to_us(resp.cts.unwrap_or(resp.orderbook_ts_ms));
            book.is_snapshot = resp.is_snapshot();
            let delta = resp
                .to_normalized()
                .ok_or_else(|| "bybit to_normalized returned None".to_string())?;
            Ok(LegacyDecoded::mapped().with_delta(delta).with_book(book))
        }
        BybitWssEvent::TradeData(fills) => {
            let mut decoded = LegacyDecoded::mapped();
            for t in fills {
                decoded = decoded.with_trade(LegacyTrade {
                    id: t.trade_id.clone(),
                    price: number(&t.price)?,
                    amount: number(&t.amount)?,
                    side: side_from(&t.side)?,
                    source_trade_ts_us: ms_to_us(t.trade_ts),
                    sequence: t.trade_id.parse::<u64>().ok(),
                });
            }
            Ok(decoded)
        }
        _ => Ok(LegacyDecoded::mapped()),
    }
}

pub fn kraken(input: LegacyInput<'_>) -> Result<LegacyDecoded, String> {
    use aetelier_connect::sources::kraken::decoder::KrakenDecoder;
    use aetelier_connect::sources::kraken::events::KrakenWssEvent;

    let LegacyInput::Frame(raw) = input else {
        return Err("kraken has no REST seed".into());
    };
    let Some(event) = KrakenDecoder::decode(raw).map_err(|e| e.to_string())? else {
        return Ok(LegacyDecoded::mapped());
    };
    match event {
        KrakenWssEvent::OrderbookData(resp) => {
            let Some(data) = resp.data.first() else {
                return Ok(LegacyDecoded::mapped());
            };
            let mut book = LegacyBook::new(data.symbol.clone());
            book.bids = data
                .bids
                .iter()
                .map(|l| (l.price.clone(), l.qty.clone()))
                .collect();
            book.asks = data
                .asks
                .iter()
                .map(|l| (l.price.clone(), l.qty.clone()))
                .collect();
            book.update_id = data.checksum;
            book.sequence = data.checksum;
            book.source_orderbook_ts_us = rfc3339_to_us(&data.timestamp);
            book.checksum = Some(data.checksum as i64);
            book.is_snapshot = resp.ty == "snapshot";
            let delta = resp
                .to_normalized()
                .ok_or_else(|| "kraken to_normalized returned None".to_string())?;
            Ok(LegacyDecoded::mapped().with_delta(delta).with_book(book))
        }
        KrakenWssEvent::TradeData(prints) => {
            let mut decoded = LegacyDecoded::mapped();
            for t in prints {
                decoded = decoded.with_trade(LegacyTrade {
                    id: t.trade_id.to_string(),
                    price: t.price,
                    amount: t.qty,
                    side: side_from(&t.side)?,
                    source_trade_ts_us: t.timestamp_us(),
                    sequence: Some(t.trade_id),
                });
            }
            Ok(decoded)
        }
    }
}

pub fn coinbase(input: LegacyInput<'_>) -> Result<LegacyDecoded, String> {
    use aetelier_connect::sources::coinbase::decoder::CoinbaseDecoder;
    use aetelier_connect::sources::coinbase::events::CoinbaseWssEvent;

    let LegacyInput::Frame(raw) = input else {
        return Err("coinbase has no REST seed".into());
    };
    let Some(event) = CoinbaseDecoder::decode(raw).map_err(|e| e.to_string())? else {
        return Ok(LegacyDecoded::mapped());
    };
    match event {
        CoinbaseWssEvent::OrderbookData(resp) => {
            let event_ts = rfc3339_to_us(&resp.timestamp);
            let mut decoded = LegacyDecoded::mapped();
            for l2 in &resp.events {
                let mut book = LegacyBook::new(l2.product_id.clone());
                for update in &l2.updates {
                    let level = (update.price_level.clone(), update.new_quantity.clone());
                    match update.side.as_str() {
                        "bid" => book.bids.push(level),
                        "offer" | "ask" => book.asks.push(level),
                        _ => {}
                    }
                }
                book.update_id = resp.sequence_num;
                book.sequence = resp.sequence_num;
                book.source_orderbook_ts_us = event_ts;
                book.is_snapshot = l2.ty == "snapshot";
                decoded = decoded.with_book(book);
            }
            if let Some(delta) = resp.to_normalized() {
                decoded = decoded.with_delta(delta);
            }
            Ok(decoded)
        }
        CoinbaseWssEvent::TradeData(prints) => {
            let mut decoded = LegacyDecoded::mapped();
            for t in prints.iter().rev() {
                decoded = decoded.with_trade(LegacyTrade {
                    id: t.trade_id.clone(),
                    price: number(&t.price)?,
                    amount: number(&t.size)?,
                    side: side_from(&t.side)?,
                    source_trade_ts_us: t.timestamp_us(),
                    sequence: t.trade_id.parse::<u64>().ok(),
                });
            }
            Ok(decoded)
        }
        _ => Ok(LegacyDecoded::mapped()),
    }
}

pub fn okx(input: LegacyInput<'_>) -> Result<LegacyDecoded, String> {
    use aetelier_connect::sources::okx::decoder::OkxDecoder;
    use aetelier_connect::sources::okx::events::OkxWssEvent;

    let LegacyInput::Frame(raw) = input else {
        return Err("okx has no REST seed".into());
    };
    let Some(event) = OkxDecoder::decode(raw).map_err(|e| e.to_string())? else {
        return Ok(LegacyDecoded::unmapped());
    };
    match event {
        OkxWssEvent::OrderbookData(resp) => {
            let is_snapshot = resp.is_snapshot();
            let symbol = resp.symbol().to_string();
            let mut decoded = LegacyDecoded::unmapped();
            for data in &resp.data {
                let mut book = LegacyBook::new(symbol.clone());
                book.bids = data
                    .bids
                    .iter()
                    .map(|l| (l.price_str().to_string(), l.size_str().to_string()))
                    .collect();
                book.asks = data
                    .asks
                    .iter()
                    .map(|l| (l.price_str().to_string(), l.size_str().to_string()))
                    .collect();
                book.update_id = data.seq_id.max(0) as u64;
                book.sequence = data.prev_seq_id.unwrap_or(data.seq_id).max(0) as u64;
                book.source_orderbook_ts_us = ms_to_us(data.ts_ms());
                book.checksum = data.checksum;
                book.is_snapshot = is_snapshot;
                decoded = decoded.with_book(book);
            }
            Ok(decoded)
        }
        OkxWssEvent::TradeData(prints) => {
            let mut decoded = LegacyDecoded::unmapped();
            for t in prints {
                decoded = decoded.with_trade(LegacyTrade {
                    id: t.trade_id.clone(),
                    price: number(&t.px)?,
                    amount: number(&t.sz)?,
                    side: side_from(&t.side)?,
                    source_trade_ts_us: ms_to_us(t.ts_ms()),
                    sequence: t.trade_id.parse::<u64>().ok(),
                });
            }
            Ok(decoded)
        }
    }
}

pub fn gateio(input: LegacyInput<'_>) -> Result<LegacyDecoded, String> {
    use aetelier_connect::sources::gateio::decoder::GateioDecoder;
    use aetelier_connect::sources::gateio::events::GateioWssEvent;

    let LegacyInput::Frame(raw) = input else {
        return Err("gateio has no REST seed".into());
    };
    let Some(event) = GateioDecoder::decode(raw).map_err(|e| e.to_string())? else {
        return Ok(LegacyDecoded::unmapped());
    };
    match event {
        GateioWssEvent::OrderbookData(resp) => {
            let data = &resp.result;
            let id = data.last_update_id.max(0) as u64;
            let mut book = LegacyBook::new(data.symbol.clone());
            book.bids = data
                .bids
                .iter()
                .map(|l| (l.price_str().to_string(), l.size_str().to_string()))
                .collect();
            book.asks = data
                .asks
                .iter()
                .map(|l| (l.price_str().to_string(), l.size_str().to_string()))
                .collect();
            book.update_id = id;
            book.sequence = id;
            book.source_orderbook_ts_us = ms_to_us(data.ts_ms);
            book.is_snapshot = true;
            Ok(LegacyDecoded::unmapped().with_book(book))
        }
        GateioWssEvent::TradeData(t) => {
            Ok(LegacyDecoded::unmapped().with_trade(LegacyTrade {
                id: t.id.to_string(),
                price: number(&t.price)?,
                amount: number(&t.amount)?,
                side: side_from(&t.side)?,
                source_trade_ts_us: ms_to_us(t.ts_ms()),
                sequence: Some(t.id),
            }))
        }
    }
}

pub fn bitget(input: LegacyInput<'_>) -> Result<LegacyDecoded, String> {
    use aetelier_connect::sources::bitget::decoder::BitgetDecoder;
    use aetelier_connect::sources::bitget::events::BitgetWssEvent;

    let LegacyInput::Frame(raw) = input else {
        return Err("bitget has no REST seed".into());
    };
    let Some(event) = BitgetDecoder::decode(raw).map_err(|e| e.to_string())? else {
        return Ok(LegacyDecoded::unmapped());
    };
    match event {
        BitgetWssEvent::Book(frame) => {
            let is_snapshot = frame.action == "snapshot";
            let symbol = frame.arg.inst_id.clone();
            let mut decoded = LegacyDecoded::unmapped();
            for data in &frame.data {
                let mut book = LegacyBook::new(symbol.clone());
                book.bids = pairs(&data.bids);
                book.asks = pairs(&data.asks);
                book.update_id = data
                    .seq
                    .unwrap_or_else(|| data.ts.parse::<u64>().unwrap_or(0));
                book.sequence = data.pseq.unwrap_or(0);
                book.source_orderbook_ts_us =
                    ms_to_us(data.ts.parse::<u64>().unwrap_or(0));
                book.is_snapshot = is_snapshot;
                decoded = decoded.with_book(book);
            }
            Ok(decoded)
        }
        BitgetWssEvent::Trade(frame) => {
            let mut decoded = LegacyDecoded::unmapped();
            for t in &frame.data {
                decoded = decoded.with_trade(LegacyTrade {
                    id: t.trade_id.clone(),
                    price: number(&t.price)?,
                    amount: number(&t.size)?,
                    side: side_from(&t.side)?,
                    source_trade_ts_us: ms_to_us(t.ts.parse::<u64>().unwrap_or(0)),
                    sequence: None,
                });
            }
            Ok(decoded)
        }
    }
}

pub fn poloniex(input: LegacyInput<'_>) -> Result<LegacyDecoded, String> {
    use aetelier_connect::sources::poloniex::decoder::PoloniexDecoder;
    use aetelier_connect::sources::poloniex::events::PoloniexWssEvent;

    let LegacyInput::Frame(raw) = input else {
        return Err("poloniex has no REST seed".into());
    };
    let Some(event) = PoloniexDecoder::decode(raw).map_err(|e| e.to_string())? else {
        return Ok(LegacyDecoded::unmapped());
    };
    match event {
        PoloniexWssEvent::Book(frame) => {
            let is_snapshot = frame.action == "snapshot";
            let mut decoded = LegacyDecoded::unmapped();
            for data in &frame.data {
                let mut book = LegacyBook::new(data.symbol.clone());
                book.bids = pairs(&data.bids);
                book.asks = pairs(&data.asks);
                book.update_id = data.id;
                book.sequence = data.last_id;
                book.source_orderbook_ts_us = ms_to_us(data.ts);
                book.is_snapshot = is_snapshot;
                decoded = decoded.with_book(book);
            }
            Ok(decoded)
        }
        PoloniexWssEvent::Trades(frame) => {
            let mut decoded = LegacyDecoded::unmapped();
            for t in &frame.data {
                let ts = if t.create_time > 0 {
                    t.create_time
                } else {
                    t.ts
                };
                decoded = decoded.with_trade(LegacyTrade {
                    id: t.id.clone(),
                    price: number(&t.price)?,
                    amount: number(&t.quantity)?,
                    side: side_from(&t.taker_side)?,
                    source_trade_ts_us: ms_to_us(ts),
                    sequence: t.id.parse::<u64>().ok(),
                });
            }
            Ok(decoded)
        }
    }
}

pub fn upbit(input: LegacyInput<'_>) -> Result<LegacyDecoded, String> {
    use aetelier_connect::sources::upbit::decoder::UpbitDecoder;
    use aetelier_connect::sources::upbit::events::UpbitWssEvent;

    let LegacyInput::Frame(raw) = input else {
        return Err("upbit has no REST seed".into());
    };
    let Some(event) = UpbitDecoder::decode(raw).map_err(|e| e.to_string())? else {
        return Ok(LegacyDecoded::unmapped());
    };
    match event {
        UpbitWssEvent::Orderbook(ob) => {
            let mut book = LegacyBook::new(ob.code.clone());
            for unit in &ob.orderbook_units {
                book.bids
                    .push((unit.bid_price.to_string(), unit.bid_size.to_string()));
                book.asks
                    .push((unit.ask_price.to_string(), unit.ask_size.to_string()));
            }
            book.update_id = ob.timestamp;
            book.sequence = 0;
            book.source_orderbook_ts_us = ms_to_us(ob.timestamp);
            book.is_snapshot = true;
            Ok(LegacyDecoded::unmapped().with_book(book))
        }
        UpbitWssEvent::Trade(t) => {
            let side = match t.ask_bid.as_str() {
                "BID" => TradeSide::Buy,
                "ASK" => TradeSide::Sell,
                other => return Err(format!("unknown upbit ask_bid '{other}'")),
            };
            Ok(LegacyDecoded::unmapped().with_trade(LegacyTrade {
                id: t.sequential_id.to_string(),
                price: t.trade_price,
                amount: t.trade_volume,
                side,
                source_trade_ts_us: ms_to_us(t.trade_timestamp),
                sequence: Some(t.sequential_id),
            }))
        }
    }
}

pub fn htx(input: LegacyInput<'_>) -> Result<LegacyDecoded, String> {
    use aetelier_connect::sources::htx::decoder::HtxDecoder;
    use aetelier_connect::sources::htx::events::HtxWssEvent;
    use aetelier_connect::sources::htx::responses::orderbooks::{HtxLevel, HtxMbpTick};

    fn levels(rows: &[HtxLevel]) -> Vec<(String, String)> {
        rows.iter()
            .map(|l| (l.0.to_string(), l.1.to_string()))
            .collect()
    }

    fn book_from(
        channel: &str,
        tick: &HtxMbpTick,
        ts: u64,
        is_snapshot: bool,
    ) -> LegacyBook {
        let symbol = channel.split('.').nth(1).unwrap_or_default();
        let mut book = LegacyBook::new(symbol);
        book.bids = levels(&tick.bids);
        book.asks = levels(&tick.asks);
        book.update_id = tick.seq_num;
        book.sequence = tick.prev_seq_num.unwrap_or(0);
        book.source_orderbook_ts_us = ms_to_us(ts);
        book.is_snapshot = is_snapshot;
        book
    }

    let LegacyInput::Frame(raw) = input else {
        return Err("htx has no REST seed".into());
    };
    let Some(event) = HtxDecoder::decode(raw).map_err(|e| e.to_string())? else {
        return Ok(LegacyDecoded::unmapped());
    };
    match event {
        HtxWssEvent::MbpUpdate(u) => {
            Ok(LegacyDecoded::unmapped()
                .with_book(book_from(&u.ch, &u.tick, u.ts, false)))
        }
        HtxWssEvent::MbpSnapshot(s) => {
            Ok(LegacyDecoded::unmapped()
                .with_book(book_from(&s.rep, &s.data, s.ts, true)))
        }
        HtxWssEvent::Trade(frame) => {
            let mut decoded = LegacyDecoded::unmapped();
            for d in frame.tick.data.iter().rev() {
                decoded = decoded.with_trade(LegacyTrade {
                    id: d.trade_id.to_string(),
                    price: d.price,
                    amount: d.amount,
                    side: side_from(&d.direction)?,
                    source_trade_ts_us: ms_to_us(d.ts),
                    sequence: Some(d.trade_id),
                });
            }
            Ok(decoded)
        }
    }
}

pub fn bitso(input: LegacyInput<'_>) -> Result<LegacyDecoded, String> {
    use aetelier_connect::sources::bitso::decoder::BitsoDecoder;
    use aetelier_connect::sources::bitso::events::BitsoWssEvent;

    let LegacyInput::Frame(raw) = input else {
        return Err("bitso rest seed is not part of the frame shim".into());
    };
    let Some(event) = BitsoDecoder::decode(raw).map_err(|e| e.to_string())? else {
        return Ok(LegacyDecoded::unmapped());
    };
    match event {
        BitsoWssEvent::DiffOrders(m) => {
            let mut book = LegacyBook::new(m.book.clone());
            let mut update_id = 0u64;
            for o in &m.payload {
                update_id = update_id.max(o.d);
                book.orders.push(L3Order {
                    order_id: o.o.clone(),
                    is_ask: o.t != 0,
                    price: o.r.clone().unwrap_or_default(),
                    size: o.a.clone().unwrap_or_default(),
                    removed: matches!(o.s.as_str(), "cancelled" | "completed"),
                });
            }
            book.update_id = update_id;
            book.sequence = m.sequence.unwrap_or(0);
            book.source_orderbook_ts_us = ms_to_us(update_id);
            Ok(LegacyDecoded::unmapped().with_book(book))
        }
        BitsoWssEvent::Trades(m) => {
            let mut decoded = LegacyDecoded::unmapped();
            for t in &m.payload {
                let side = match t.t {
                    0 => TradeSide::Buy,
                    1 => TradeSide::Sell,
                    other => return Err(format!("unknown bitso taker flag '{other}'")),
                };
                decoded = decoded.with_trade(LegacyTrade {
                    id: t.i.to_string(),
                    price: number(&t.r)?,
                    amount: number(&t.a)?,
                    side,
                    source_trade_ts_us: ms_to_us(t.x),
                    sequence: None,
                });
            }
            Ok(decoded)
        }
    }
}

pub fn kucoin(input: LegacyInput<'_>) -> Result<LegacyDecoded, String> {
    use aetelier_connect::sources::kucoin::decoder::KucoinDecoder;
    use aetelier_connect::sources::kucoin::events::KucoinWssEvent;

    let LegacyInput::Frame(raw) = input else {
        return Err("kucoin rest seed is not part of the frame shim".into());
    };
    let Some(event) = KucoinDecoder::decode(raw).map_err(|e| e.to_string())? else {
        return Ok(LegacyDecoded::unmapped());
    };
    match event {
        KucoinWssEvent::Level2(d) => {
            let mut book = LegacyBook::new(d.symbol.clone());
            book.bids = d
                .changes
                .bids
                .iter()
                .map(|c| (c.0.clone(), c.1.clone()))
                .collect();
            book.asks = d
                .changes
                .asks
                .iter()
                .map(|c| (c.0.clone(), c.1.clone()))
                .collect();
            book.update_id = d.sequence_end;
            book.sequence = d.sequence_start;
            book.source_orderbook_ts_us = ms_to_us(d.time);
            Ok(LegacyDecoded::unmapped().with_book(book))
        }
        KucoinWssEvent::Match(m) => {
            Ok(LegacyDecoded::unmapped().with_trade(LegacyTrade {
                id: m.trade_id.clone(),
                price: number(&m.price)?,
                amount: number(&m.size)?,
                side: side_from(&m.side)?,
                source_trade_ts_us: ns_to_us(
                    m.time
                        .parse::<u64>()
                        .map_err(|e| format!("kucoin match time '{}': {e}", m.time))?,
                ),
                sequence: None,
            }))
        }
    }
}
