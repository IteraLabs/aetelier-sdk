#[cfg(test)]
mod tests {

    use aetelier_types::temporal;

    #[test]
    fn test_from_nanos_to_seconds() {
        let ts_us: u64 = 1_500_000_000; // 1.5 seconds
        let ts_us = temporal::from_nanos(ts_us, temporal::TimeResolution::Seconds);
        assert!((ts_us - 1.5).abs() < 1e-13);
    }

    #[test]
    fn test_from_nanos_to_millis() {
        let ts_us: u64 = 1_500_000; // 1.5 milliseconds
        let ts_ms = temporal::from_nanos(ts_us, temporal::TimeResolution::Milliseconds);
        assert!((ts_ms - 1.5).abs() < 1e-10);
    }

    #[test]
    fn test_from_nanos_to_micros() {
        let ts_us: u64 = 1_500; // 1.5 microseconds
        let ts_us = temporal::from_nanos(ts_us, temporal::TimeResolution::Microseconds);
        assert!((ts_us - 1.5).abs() < 1e-7);
    }

    #[test]
    fn test_from_seconds_to_nanos() {
        let ts_s: u64 = 1_672;
        let ts_us = temporal::to_nanos(ts_s, temporal::TimeResolution::Seconds);
        assert_eq!(ts_us, ts_s * temporal::NS_PER_S);
    }

    #[test]
    fn test_from_millis_to_nanos() {
        let ts_ms: u64 = 1_672_304;
        let ts_us = temporal::to_nanos(ts_ms, temporal::TimeResolution::Milliseconds);
        assert_eq!(ts_us, ts_ms * temporal::NS_PER_MS);
    }

    #[test]
    fn test_from_micros_to_nanos() {
        let ts_us: u64 = 1_672_304_484;
        let ts_ns_out = temporal::to_nanos(ts_us, temporal::TimeResolution::Microseconds);
        assert_eq!(ts_ns_out, ts_us * temporal::NS_PER_US);
    }
}
