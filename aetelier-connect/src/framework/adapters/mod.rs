//! Per-venue adapters — the only venue-specific code behind the framework.
//!
//! Each adapter wires three small impls against the generic seam:
//! - [`ProtocolHooks`](crate::framework::protocol::ProtocolHooks) — the
//!   transport axis (subscribe framing / heartbeat / control / codec / bootstrap).
//! - [`Normalizer`](crate::framework::model::Normalizer) — decode → `DomainEvent`.
//! - [`ExchangeAdapter`](crate::framework::registry::ExchangeAdapter) — the
//!   registry entry (profile + reconstruction-model + `spawn`).
//!
//! Where available, the decoder + wire response types are reused from
//! `crate::sources::<venue>`, so an adapter is a thin composition.
//!
//! Per-venue transport shapes:
//! - **binance** — combined-stream URL, WS-`Pong` heartbeat, `SeqDelta`
//!   (RangeInclusive) seeded by REST.
//! - **okx** — `{op,args:[{channel,instId}]}` framing, app-level `"ping"` text
//!   heartbeat, `ChecksumDelta` reconstruction.
//! - **upbit** — top-level JSON-array subscribe, `FullRefresh`, `QuoteFirst`
//!   (`KRW-BTC`) codec.
//! - **htx** — `FrameCodec::Gzip`, server-echo ping (`ControlAction::Reply`),
//!   in-band REQ seed (`prepare`→`extra_frames`), `SeqDelta{ExactPrev}`.
//! - **kucoin** — bullet-token `prepare` bootstrap (dynamic endpoint +
//!   server-dictated ping cadence).
//! - **bitso** — `{action,book,type}` subscribe, `L3` order-id-keyed book.

pub mod binance;
pub mod bitget;
pub mod bitso;
pub mod bybit;
pub mod coinbase;
pub mod gateio;
pub mod htx;
pub mod hyperliquid;
pub mod kraken;
pub mod kucoin;
pub mod okx;
pub mod poloniex;
pub mod upbit;

#[cfg(test)]
mod ack_code_tests {
    use super::{binance::BinanceHooks, bybit::BybitHooks, okx::OkxHooks};
    use crate::framework::protocol::{AckOutcome, ProtocolHooks};

    fn code_of(outcome: AckOutcome) -> Option<i64> {
        match outcome {
            AckOutcome::Rejected { code, .. } => code,
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    #[test]
    fn venue_codes_are_extracted_only_where_the_venue_publishes_one() {
        let okx = OkxHooks
            .classify_ack(r#"{"event":"error","code":"60012","msg":"Invalid request"}"#);
        assert_eq!(code_of(okx), Some(60012), "okx publishes a string code");

        let binance = BinanceHooks
            .classify_ack(r#"{"error":{"code":-1121,"msg":"Invalid symbol"},"id":1}"#);
        assert_eq!(
            code_of(binance),
            Some(-1121),
            "binance publishes a numeric code"
        );

        let bybit = BybitHooks
            .classify_ack(r#"{"success":false,"ret_msg":"bad topic","op":"subscribe"}"#);
        assert_eq!(code_of(bybit), None, "bybit v5 subscribe carries no code");
    }

    #[test]
    fn a_missing_code_field_never_fabricates_one() {
        let okx = OkxHooks.classify_ack(r#"{"event":"error","msg":"no code here"}"#);
        assert_eq!(code_of(okx), None);

        let binance =
            BinanceHooks.classify_ack(r#"{"error":{"msg":"no code here"},"id":1}"#);
        assert_eq!(code_of(binance), None);
    }

    #[test]
    fn an_unparseable_venue_code_is_absent_rather_than_guessed() {
        let okx = OkxHooks.classify_ack(r#"{"event":"error","code":"E12","msg":"x"}"#);
        assert_eq!(code_of(okx), None);
    }
}
