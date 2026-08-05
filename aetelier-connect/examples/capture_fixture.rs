//! Live raw-frame capture for conformance fixtures.
//!
//! Connects to a venue over its REAL protocol — the adapter's own
//! `ProtocolHooks` supply the endpoint, subscribe frames, and heartbeat, the
//! same protocol the runtime uses — and records every inbound text frame,
//! verbatim, to a `.jsonl` fixture. This is the raw wire stream the
//! conformance harness later replays through `replay_frame`; it is NOT the
//! reconstructed/parquet output a collector agent produces.
//!
//! Usage:
//!   cargo run --example capture_fixture -- <venue> <wire_symbol> <seconds> <out.jsonl>
//! Example:
//!   cargo run --example capture_fixture -- coinbase BTC-USD 180 datasets/coinbase/btcusd_book_trade.jsonl

use std::io::Write as _;
use std::sync::Arc;
use std::time::Duration;

use aetelier_connect::framework::protocol::{FramePayload, Heartbeat, ProtocolHooks};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::Mutex;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;

/// The concrete protocol hooks for a venue (the same the adapter's `spawn`
/// constructs). Add a venue's arm as its conformance cycle needs a capture.
fn hooks_for(venue: &str, symbol: &str) -> Option<Box<dyn ProtocolHooks>> {
    use aetelier_connect::framework::adapters;
    match venue {
        "binance" => Some(Box::new(adapters::binance::BinanceHooks)),
        "okx" => Some(Box::new(adapters::okx::OkxHooks)),
        "kraken" => Some(Box::new(adapters::kraken::KrakenHooks)),
        // Coinbase runs split sockets in production: "coinbase" captures the
        // book socket (level2 + heartbeats, the sequence-tracked one);
        // "coinbase-trades" captures the market_trades socket.
        "coinbase" => Some(Box::new(adapters::coinbase::CoinbaseHooks::level2())),
        "coinbase-trades" => Some(Box::new(adapters::coinbase::CoinbaseHooks::trades())),
        "bybit" => Some(Box::new(adapters::bybit::BybitHooks)),
        "gateio" => Some(Box::new(adapters::gateio::GateioHooks)),
        "bitget" => Some(Box::new(adapters::bitget::BitgetHooks)),
        "bitso" => Some(Box::new(adapters::bitso::BitsoHooks)),
        "poloniex" => Some(Box::new(adapters::poloniex::PoloniexHooks)),
        "upbit" => Some(Box::new(adapters::upbit::UpbitHooks)),
        // HTX seeds via an in-band REQ built per symbol in prepare(); gzip
        // binary frames are inflated by frame_codec below.
        "htx" => Some(Box::new(adapters::htx::HtxHooks::new(vec![
            symbol.to_string(),
        ]))),
        // KuCoin's prepare() does the bullet-token POST and overrides the
        // endpoint + heartbeat.
        "kucoin" => Some(Box::new(adapters::kucoin::KucoinHooks::with_connect_id(
            "aetelier-capture".to_string(),
        ))),
        _ => None,
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 5 {
        eprintln!("usage: capture_fixture <venue> <wire_symbol> <seconds> <out.jsonl>");
        std::process::exit(2);
    }
    let venue = &args[1];
    let symbol = args[2].clone();
    let seconds: u64 = args[3].parse().expect("seconds must be an integer");
    let out_path = &args[4];

    let hooks = hooks_for(venue, &symbol)
        .unwrap_or_else(|| panic!("no capture hooks for venue '{venue}'"));

    // Prepare exactly as the transport does: KuCoin's bullet-token POST
    // supplies the endpoint + heartbeat overrides; HTX's in-band REQ seed
    // arrives as extra_frames sent after subscribe.
    let prepared = hooks.prepare().await.expect("prepare");
    let url = prepared
        .endpoint_override
        .clone()
        .unwrap_or_else(|| hooks.endpoint());
    eprintln!("connecting to {venue} at {url} for {seconds}s ({symbol})");

    let (ws, _) = connect_async(&url).await.expect("connect");
    let (write, mut read) = ws.split();
    let write = Arc::new(Mutex::new(write));

    // Subscribe with the adapter's own frames immediately.
    for frame in hooks.subscribe_frames(
        std::slice::from_ref(&symbol),
        &aetelier_connect::framework::protocol::DeclaredSet::all(),
    ) {
        write
            .lock()
            .await
            .send(frame)
            .await
            .expect("send subscribe");
    }

    // Extra frames (HTX's in-band REQ seed) are sent AFTER a short delay so the
    // seed reply lands mid-stream: by then deltas have buffered, so the reply's
    // seqNum is current and the following deltas chain from it. Sent at t=0 the
    // reply lags the delta stream and can't bridge (the offline-reconstruction
    // gap this fixes). Harmless for venues with no extra frames.
    if !prepared.extra_frames.is_empty() {
        let w = write.clone();
        let extra = prepared.extra_frames.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(6)).await;
            for frame in extra {
                if w.lock().await.send(frame).await.is_err() {
                    break;
                }
            }
        });
    }

    // Heartbeat per the adapter's policy (prepare override wins).
    let heartbeat = prepared
        .heartbeat_override
        .clone()
        .unwrap_or_else(|| hooks.heartbeat());
    match heartbeat {
        Heartbeat::None => {}
        Heartbeat::WsPong { every } => {
            let w = write.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(every).await;
                    if w.lock()
                        .await
                        .send(Message::Pong(Vec::new().into()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });
        }
        Heartbeat::Text { every, payload } => {
            let w = write.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(every).await;
                    let text = payload();
                    if w.lock()
                        .await
                        .send(Message::Text(text.into()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });
        }
        Heartbeat::Resubscribe { every, frame } => {
            let w = write.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(every).await;
                    if w.lock().await.send(frame.clone()).await.is_err() {
                        break;
                    }
                }
            });
        }
    }

    // Inflate frames exactly as the transport does (text passthrough,
    // binary-plaintext for Upbit, gzip for HTX) so the fixture is the
    // inflated text `replay_frame` decodes.
    let codec = hooks.frame_codec();
    let mut file = std::fs::File::create(out_path).expect("create fixture");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
    let mut frames = 0u64;

    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => break,
            msg = read.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    match codec.inflate(FramePayload::Text(&text)) {
                        Ok(t) => {
                            // Answer application-level control frames exactly
                            // as the transport does (HTX's `{"ping":ts}` needs
                            // a `{"pong":ts}` echo or the server closes the
                            // socket after ~1 minute).
                            if let aetelier_connect::framework::protocol::ControlAction::Reply(reply) =
                                hooks.on_inbound_control(&t)
                            {
                                let _ = write.lock().await.send(reply).await;
                            }
                            writeln!(file, "{}", t.replace(['\n', '\r'], " "))
                                .expect("write frame");
                            frames += 1;
                        }
                        Err(e) => eprintln!("inflate(text) error: {e}"),
                    }
                }
                Some(Ok(Message::Binary(b))) => {
                    match codec.inflate(FramePayload::Binary(&b)) {
                        Ok(t) => {
                            if let aetelier_connect::framework::protocol::ControlAction::Reply(reply) =
                                hooks.on_inbound_control(&t)
                            {
                                let _ = write.lock().await.send(reply).await;
                            }
                            writeln!(file, "{}", t.replace(['\n', '\r'], " "))
                                .expect("write frame");
                            frames += 1;
                        }
                        Err(e) => eprintln!("inflate(binary) error: {e}"),
                    }
                }
                // Reply to server pings so the connection stays open.
                Some(Ok(Message::Ping(p))) => {
                    let _ = write.lock().await.send(Message::Pong(p)).await;
                }
                Some(Ok(Message::Close(_))) | None => {
                    eprintln!("stream closed by server");
                    break;
                }
                Some(Err(e)) => {
                    eprintln!("read error: {e}");
                    break;
                }
                _ => {}
            }
        }
    }

    file.flush().expect("flush fixture");
    eprintln!("captured {frames} frames -> {out_path}");
}
