#[cfg(test)]
mod tests {
    use aetelier_telemetry::attributes::*;

    #[test]
    fn test_topic_category_orderbook() {
        assert_eq!(topic_category("orderbook.50.BTCUSDT"), "orderbook");
        assert_eq!(topic_category("orderbook.25.ETHUSDT"), "orderbook");
    }

    #[test]
    fn test_topic_category_trade() {
        assert_eq!(topic_category("trade.all.BTCUSDT"), "trade");
        assert_eq!(topic_category("publicTrade.BTCUSDT"), "trade");
    }

    #[test]
    fn test_topic_category_others() {
        assert_eq!(topic_category("liquidation.all.BTCUSDT"), "liquidation");
        assert_eq!(topic_category("funding.all.ETHUSDT"), "funding");
        assert_eq!(topic_category("open_interest.all.BTCUSDT"), "open_interest");
        assert_eq!(topic_category("something_else"), "unknown");
    }

    #[test]
    fn test_event_attributes_count() {
        let attrs = event_attributes(
            "bybit",
            "BTCUSDT",
            "bybit:BTCUSDT:0",
            "orderbook.50.BTCUSDT",
        );
        assert_eq!(attrs.len(), 5);
    }

    #[test]
    fn test_worker_attributes_count() {
        let attrs = worker_attributes("bybit", "BTCUSDT", "perpetual", "bybit:BTCUSDT:0");
        assert_eq!(attrs.len(), 4);
    }

    #[test]
    fn test_event_attributes_values() {
        let attrs = event_attributes(
            "kraken",
            "ETHUSDT",
            "kraken:ETHUSDT:1",
            "trade.all.ETHUSDT",
        );
        let exchange_kv = attrs.iter().find(|kv| kv.key.as_str() == EXCHANGE).unwrap();
        assert_eq!(exchange_kv.value.as_str(), "kraken");
        let cat_kv = attrs
            .iter()
            .find(|kv| kv.key.as_str() == TOPIC_CATEGORY)
            .unwrap();
        assert_eq!(cat_kv.value.as_str(), "trade");
    }
}
