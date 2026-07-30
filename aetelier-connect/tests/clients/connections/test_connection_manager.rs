#[cfg(test)]
mod tests {

    use aetelier_connect::{
        ConnectionManager, ConnectionManagerConfig, ConnectionState, DisconnectReason,
        ReconnectAction,
    };
    use std::time::Duration;

    #[test]
    fn default_config_matches_spec() {
        let cfg = ConnectionManagerConfig::default();
        assert_eq!(cfg.initial_delay, Duration::from_millis(100));
        assert_eq!(cfg.max_delay, Duration::from_secs(10));
        assert!(cfg.max_attempts.is_none());
    }

    #[test]
    fn transitions_are_recorded() {
        let mut mgr = ConnectionManager::with_defaults("test:BTCUSDT");
        assert_eq!(mgr.state(), ConnectionState::Disconnected);
        assert!(mgr.transitions().is_empty());

        mgr.transition(ConnectionState::Connecting, "initial connect");
        assert_eq!(mgr.state(), ConnectionState::Connecting);
        assert_eq!(mgr.transitions().len(), 1);
        assert_eq!(mgr.transitions()[0].from, ConnectionState::Disconnected);
        assert_eq!(mgr.transitions()[0].to, ConnectionState::Connecting);

        mgr.transition(ConnectionState::Streaming, "first event");
        assert_eq!(mgr.state(), ConnectionState::Streaming);
        assert_eq!(mgr.transitions().len(), 2);
    }

    #[test]
    fn on_connected_resets_policy() {
        let mut mgr = ConnectionManager::with_defaults("test:BTCUSDT");

        // Simulate a failure cycle
        let reason = DisconnectReason::TransportError {
            source: "test".into(),
        };
        let _action = mgr.on_disconnect(&reason);
        assert!(mgr.consecutive_failures() > 0);

        // Reset
        mgr.on_connected();
        assert_eq!(mgr.consecutive_failures(), 0);
    }

    #[test]
    fn non_retryable_disconnect_gives_up() {
        let mut mgr = ConnectionManager::with_defaults("test:BTCUSDT");
        let reason = DisconnectReason::ProtocolRejection {
            code: 1008,
            reason: "bad auth".into(),
        };
        let action = mgr.on_disconnect(&reason);
        assert!(matches!(action, ReconnectAction::GiveUp { .. }));
    }
}
