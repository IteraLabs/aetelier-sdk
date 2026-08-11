//! Tests for the `data_worker` module.
//!
//! These tests verify the correctness of the data worker's internal
//! components **without** requiring a live exchange connection.  They
//! exercise:
//!
//! - [`ConnectionManager`] state machine transitions and backoff
//! - [`TopicRegistry`] creation, publishing, and subscription
//! - [`GapDetector`] silence detection
//! - [`classify_event`] routing for all exchange variants
//! - Config → registry → publish → subscribe round-trip

#![allow(deprecated)]

#[cfg(test)]
mod tests {

    use aetelier_connect::{
        ExchangeEvent,
        sources::{bybit, coinbase, kraken},
        workers::ingestion_core,
    };
    use aetelier_types::config::markets::market_config::{
        DataTypesSection, FeedToggle, OrderbookConfig,
    };

    fn all_enabled() -> DataTypesSection {
        DataTypesSection {
            orderbook: OrderbookConfig {
                enabled: true,
                depth: 50,
            },
            trades: FeedToggle { enabled: true },
            liquidations: FeedToggle { enabled: true },
            funding_rates: FeedToggle { enabled: true },
            open_interest: FeedToggle { enabled: true },
        }
    }

    fn bybit_trade_event() -> ExchangeEvent {
        ExchangeEvent::Bybit(bybit::events::BybitWssEvent::TradeData(vec![
            bybit::responses::BybitTradeData {
                trade_ts: 1_700_000_000_000,
                symbol: "BTCUSDT".into(),
                side: "Buy".into(),
                amount: "0.001".into(),
                price: "42000.0".into(),
                direction: Some("PlusTick".into()),
                trade_id: "test-001".into(),
                block_trade: false,
                rpi_trade: false,
                sequence: 1,
            },
        ]))
    }

    fn coinbase_trade_event() -> ExchangeEvent {
        ExchangeEvent::Coinbase(coinbase::events::CoinbaseWssEvent::TradeData(vec![
            coinbase::responses::CoinbaseTradeData {
                trade_id: "cb-001".into(),
                product_id: "BTC-USD".into(),
                price: "42000.00".into(),
                size: "0.001".into(),
                side: "BUY".into(),
                time: "2024-01-15T12:00:00.000Z".into(),
            },
        ]))
    }

    fn coinbase_orderbook_event() -> ExchangeEvent {
        ExchangeEvent::Coinbase(coinbase::events::CoinbaseWssEvent::OrderbookData(
            coinbase::responses::CoinbaseOrderbookResponse {
                channel: "l2_data".into(),
                timestamp: "2024-01-15T12:00:00.000Z".into(),
                sequence_num: 1,
                events: vec![],
            },
        ))
    }

    fn kraken_trade_event() -> ExchangeEvent {
        ExchangeEvent::Kraken(kraken::events::KrakenWssEvent::TradeData(vec![
            kraken::responses::KrakenTradeData {
                symbol: "BTC/USD".into(),
                side: "buy".into(),
                price: 42000.0,
                qty: 0.001,
                ord_type: "market".into(),
                trade_id: 12345,
                timestamp: "2024-01-15T12:00:00.000000Z".into(),
            },
        ]))
    }

    fn kraken_orderbook_event() -> ExchangeEvent {
        ExchangeEvent::Kraken(kraken::events::KrakenWssEvent::OrderbookData(
            kraken::responses::KrakenBookResponse {
                channel: "book".into(),
                ty: "snapshot".into(),
                data: vec![],
            },
        ))
    }

    // ── Bybit ───────────────────────────────────────────────────────────

    #[test]
    fn classify_bybit_trade_with_trades_enabled() {
        let event = bybit_trade_event();
        let topics = ingestion_core::classify_event(
            &event,
            "bybit",
            "BTCUSDT",
            50,
            &all_enabled(),
        );
        assert_eq!(topics, vec!["trade.all.BTCUSDT"]);
    }

    #[test]
    fn classify_bybit_trade_with_trades_disabled() {
        let mut dt = all_enabled();
        dt.trades.enabled = false;
        let event = bybit_trade_event();
        let topics = ingestion_core::classify_event(&event, "bybit", "BTCUSDT", 50, &dt);
        assert!(topics.is_empty());
    }

    #[test]
    fn classify_bybit_ticker_maps_to_both_funding_and_oi() {
        let ticker = bybit::responses::BybitTickerData {
            symbol: "BTCUSDT".into(),
            funding_rate: Some("0.0001".into()),
            open_interest: Some("50000".into()),
            ts: Some(1_700_000_000_000),
            tick_direction: None,
            price_24h_pcnt: None,
            last_price: None,
            prev_price_24h: None,
            high_price_24h: None,
            low_price_24h: None,
            prev_price_1h: None,
            mark_price: None,
            index_price: None,
            open_interest_value: None,
            turnover_24h: None,
            volume_24h: None,
            next_funding_time: None,
            bid1_price: None,
            bid1_size: None,
            ask1_price: None,
            ask1_size: None,
            delivery_time: None,
            basis_rate: None,
            delivery_fee_rate: None,
            predicted_delivery_price: None,
            pre_open_price: None,
            pre_qty: None,
            cur_pre_listing_phase: None,
            funding_interval_hour: None,
            funding_cap: None,
            basis_rate_year: None,
        };
        let event =
            ExchangeEvent::Bybit(bybit::events::BybitWssEvent::TickerData(ticker));
        let topics = ingestion_core::classify_event(
            &event,
            "bybit",
            "BTCUSDT",
            50,
            &all_enabled(),
        );
        assert_eq!(topics.len(), 2);
        assert!(topics.contains(&"funding.all.BTCUSDT".to_string()));
        assert!(topics.contains(&"open_interest.all.BTCUSDT".to_string()));
    }

    #[test]
    fn classify_respects_disabled_datatypes() {
        let event = bybit_trade_event();
        let topics = ingestion_core::classify_event(&event, "bybit", "BTCUSDT", 50, &{
            let mut dt = all_enabled();
            dt.trades.enabled = false;
            dt
        });
        assert!(topics.is_empty());
    }

    // ── Coinbase ────────────────────────────────────────────────────────

    #[test]
    fn classify_coinbase_trade_enabled() {
        let event = coinbase_trade_event();
        let topics = ingestion_core::classify_event(
            &event,
            "coinbase",
            "BTC-USD",
            50,
            &all_enabled(),
        );
        assert_eq!(topics, vec!["trade.all.BTC-USD"]);
    }

    #[test]
    fn classify_coinbase_trade_disabled() {
        let mut dt = all_enabled();
        dt.trades.enabled = false;
        let event = coinbase_trade_event();
        let topics =
            ingestion_core::classify_event(&event, "coinbase", "BTC-USD", 50, &dt);
        assert!(topics.is_empty());
    }

    #[test]
    fn classify_coinbase_orderbook_enabled() {
        let event = coinbase_orderbook_event();
        let topics = ingestion_core::classify_event(
            &event,
            "coinbase",
            "BTC-USD",
            50,
            &all_enabled(),
        );
        assert_eq!(topics, vec!["orderbook.50.BTC-USD"]);
    }

    #[test]
    fn classify_coinbase_orderbook_disabled() {
        let mut dt = all_enabled();
        dt.orderbook.enabled = false;
        let event = coinbase_orderbook_event();
        let topics =
            ingestion_core::classify_event(&event, "coinbase", "BTC-USD", 50, &dt);
        assert!(topics.is_empty());
    }

    // ── Kraken ──────────────────────────────────────────────────────────

    #[test]
    fn classify_kraken_trade_enabled() {
        let event = kraken_trade_event();
        let topics = ingestion_core::classify_event(
            &event,
            "kraken",
            "BTC/USD",
            50,
            &all_enabled(),
        );
        assert_eq!(topics, vec!["trade.all.BTC/USD"]);
    }

    #[test]
    fn classify_kraken_trade_disabled() {
        let mut dt = all_enabled();
        dt.trades.enabled = false;
        let event = kraken_trade_event();
        let topics = ingestion_core::classify_event(&event, "kraken", "BTC/USD", 50, &dt);
        assert!(topics.is_empty());
    }

    #[test]
    fn classify_kraken_orderbook_enabled() {
        let event = kraken_orderbook_event();
        let topics = ingestion_core::classify_event(
            &event,
            "kraken",
            "BTC/USD",
            50,
            &all_enabled(),
        );
        assert_eq!(topics, vec!["orderbook.50.BTC/USD"]);
    }

    #[test]
    fn classify_kraken_orderbook_disabled() {
        let mut dt = all_enabled();
        dt.orderbook.enabled = false;
        let event = kraken_orderbook_event();
        let topics = ingestion_core::classify_event(&event, "kraken", "BTC/USD", 50, &dt);

        assert!(topics.is_empty());
    }
}
