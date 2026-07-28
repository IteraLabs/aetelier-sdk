//! Unit tests for error enum construction and Display impls.
//!
//! Covers all 6 error types in `aetelier_types::errors`.

#[cfg(test)]
mod tests {
    use aetelier_types::errors::{
        LevelError, LoaderError, OrderError, OrderbookError, PersistError, TemporalError,
    };

    // ── LevelError ──────────────────────────────────────────────────────

    #[test]
    fn level_error_display() {
        assert_eq!(LevelError::LevelNotFound.to_string(), "Level not found");
        assert_eq!(
            LevelError::LevelInfoNotAvailable.to_string(),
            "Level info not available"
        );
        assert_eq!(
            LevelError::LevelDeletionFailed.to_string(),
            "Level deletion not successful"
        );
        assert_eq!(
            LevelError::LevelModificationFailed.to_string(),
            "Level modification not successful"
        );
        assert_eq!(
            LevelError::LevelInsertionFailed.to_string(),
            "Level insertion not successful"
        );
    }

    // ── OrderError ──────────────────────────────────────────────────────

    #[test]
    fn order_error_display() {
        assert_eq!(OrderError::OrderNotFound.to_string(), "Order not found");
        assert_eq!(
            OrderError::OrderInfoNotAvailable.to_string(),
            "Order info not available"
        );
        assert_eq!(
            OrderError::OrderDeletionFailed.to_string(),
            "Order deletion not successful"
        );
        assert_eq!(
            OrderError::OrderModificationFailed.to_string(),
            "Order modification not successful"
        );
        assert_eq!(
            OrderError::OrderInsertionFailed.to_string(),
            "Order insertion not successful"
        );
    }

    // ── OrderbookError ──────────────────────────────────────────────────

    #[test]
    fn orderbook_error_not_initialized() {
        let e = OrderbookError::NotInitialized;
        assert_eq!(
            e.to_string(),
            "Orderbook not initialized, waiting for snapshot"
        );
    }

    #[test]
    fn orderbook_error_sequence_gap() {
        let e = OrderbookError::SequenceGap {
            expected: 100,
            received: 105,
        };
        assert_eq!(e.to_string(), "Sequence gap: expected 100, received 105");
    }

    #[test]
    fn orderbook_error_symbol_mismatch() {
        let e = OrderbookError::SymbolMismatch {
            expected: "BTCUSDT".into(),
            received: "ETHUSDT".into(),
        };
        assert_eq!(
            e.to_string(),
            "Symbol mismatch: expected BTCUSDT, received ETHUSDT"
        );
    }

    #[test]
    fn orderbook_error_parse() {
        let e = OrderbookError::ParseError("bad decimal".into());
        assert_eq!(e.to_string(), "Parse error: bad decimal");
    }

    #[test]
    fn orderbook_error_contents() {
        let e = OrderbookError::ContentsError("empty book".into());
        assert_eq!(e.to_string(), "Orderbook contents error: empty book");
    }

    #[test]
    fn orderbook_error_eq_and_clone() {
        let a = OrderbookError::NotInitialized;
        let b = a.clone();
        assert_eq!(a, b);

        let c = OrderbookError::SequenceGap {
            expected: 1,
            received: 2,
        };
        let d = OrderbookError::SequenceGap {
            expected: 1,
            received: 3,
        };
        assert_ne!(c, d);
    }

    // ── PersistError ────────────────────────────────────────────────────

    #[test]
    fn persist_error_unsupported_format() {
        let e = PersistError::UnsupportedFormat("txt".into());
        assert_eq!(e.to_string(), "Unsupported format: txt");
    }

    #[test]
    fn persist_error_parse() {
        let e = PersistError::Parse("bad row".into());
        assert_eq!(e.to_string(), "Parse error: bad row");
    }

    #[test]
    fn persist_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "no file");
        let e: PersistError = io_err.into();
        let msg = e.to_string();
        assert!(msg.starts_with("IO error:"), "got: {msg}");
    }

    #[test]
    fn persist_error_from_json() {
        // Trigger a serde_json error by parsing invalid JSON
        let json_err = serde_json::from_str::<serde_json::Value>("{{bad}}").unwrap_err();
        let e: PersistError = json_err.into();
        let msg = e.to_string();
        assert!(msg.starts_with("JSON error:"), "got: {msg}");
    }

    // ── LoaderError ─────────────────────────────────────────────────────

    #[test]
    fn loader_error_display() {
        assert_eq!(
            LoaderError::EmptyData.to_string(),
            "Empty data: no timestamps to process"
        );
        assert_eq!(
            LoaderError::IoError("disk full".into()).to_string(),
            "I/O error: disk full"
        );
    }

    // ── TemporalError ───────────────────────────────────────────────────

    #[test]
    fn temporal_error_empty_data() {
        assert_eq!(
            TemporalError::EmptyData.to_string(),
            "Empty data: no timestamps to process"
        );
    }

    #[test]
    fn temporal_error_non_monotonic() {
        let e = TemporalError::NonMonotonic {
            index: 5,
            prev: 100,
            curr: 99,
        };
        assert_eq!(
            e.to_string(),
            "Non-monotonic timestamps at index 5: prev=100, curr=99"
        );
    }

    #[test]
    fn temporal_error_insufficient_data() {
        let e = TemporalError::InsufficientData("need 2+ points".into());
        assert_eq!(e.to_string(), "Insufficient data: need 2+ points");
    }
}
