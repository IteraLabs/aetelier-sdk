use std::time::Duration;

use rand::Rng;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Success,
    RetryAfterBackoff,
    Fatal,
}

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub multiplier: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 8,
            base_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(120),
            multiplier: 2.0,
        }
    }
}

impl RetryPolicy {
    pub fn ceiling(&self, attempt: u32) -> Duration {
        let exponent = attempt.min(1024) as i32;
        let scaled = self.base_delay.as_secs_f64() * self.multiplier.powi(exponent);
        if !scaled.is_finite() || scaled >= self.max_delay.as_secs_f64() {
            return self.max_delay;
        }
        Duration::from_secs_f64(scaled)
    }

    pub fn jittered_with<R: Rng>(&self, attempt: u32, rng: &mut R) -> Duration {
        let ceiling = self.ceiling(attempt);
        let half = ceiling / 2;
        half + Duration::from_secs_f64(rng.random_range(0.0..=half.as_secs_f64()))
    }

    pub fn jittered(&self, attempt: u32) -> Duration {
        self.jittered_with(attempt, &mut rand::rng())
    }

    pub fn delay_for(&self, attempt: u32, retry_after: Option<Duration>) -> Duration {
        match retry_after {
            Some(hinted) => hinted.min(self.max_delay).max(self.jittered(attempt)),
            None => self.jittered(attempt),
        }
    }

    pub fn exhausted(&self, attempt: u32) -> bool {
        attempt >= self.max_retries
    }
}

pub fn classify_status(status: u16) -> Verdict {
    match status {
        200..=299 => Verdict::Success,
        408 | 425 | 429 => Verdict::RetryAfterBackoff,
        500..=599 => Verdict::RetryAfterBackoff,
        _ => Verdict::Fatal,
    }
}

pub fn is_retryable_transport(err: &reqwest::Error) -> bool {
    err.is_timeout()
        || err.is_connect()
        || err.is_request()
        || err.is_body()
        || err.is_decode()
}

pub fn parse_retry_after(raw: &str) -> Option<Duration> {
    raw.trim()
        .parse::<u64>()
        .ok()
        .map(|secs| Duration::from_secs(secs.min(600)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    fn policy() -> RetryPolicy {
        RetryPolicy {
            max_retries: 5,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            multiplier: 2.0,
        }
    }

    #[test]
    fn ceiling_grows_geometrically_and_saturates() {
        let p = policy();
        assert_eq!(p.ceiling(0), Duration::from_millis(100));
        assert_eq!(p.ceiling(1), Duration::from_millis(200));
        assert_eq!(p.ceiling(20), Duration::from_secs(10));
        assert_eq!(p.ceiling(u32::MAX), Duration::from_secs(10));
    }

    #[test]
    fn jitter_stays_between_half_ceiling_and_ceiling() {
        let p = policy();
        let mut rng = StdRng::seed_from_u64(42);
        for attempt in 0..6 {
            let ceiling = p.ceiling(attempt);
            for _ in 0..200 {
                let delay = p.jittered_with(attempt, &mut rng);
                assert!(delay >= ceiling / 2);
                assert!(delay <= ceiling);
            }
        }
    }

    #[test]
    fn statuses_classify_by_retryability() {
        assert_eq!(classify_status(200), Verdict::Success);
        assert_eq!(classify_status(206), Verdict::Success);
        assert_eq!(classify_status(429), Verdict::RetryAfterBackoff);
        assert_eq!(classify_status(503), Verdict::RetryAfterBackoff);
        assert_eq!(classify_status(403), Verdict::Fatal);
        assert_eq!(classify_status(404), Verdict::Fatal);
    }

    #[test]
    fn retry_after_is_parsed_capped_and_never_below_backoff() {
        let p = policy();
        assert_eq!(parse_retry_after("3"), Some(Duration::from_secs(3)));
        assert_eq!(parse_retry_after("99999"), Some(Duration::from_secs(600)));
        assert_eq!(parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT"), None);
        assert!(p.delay_for(4, Some(Duration::from_millis(1))) >= p.ceiling(4) / 2);
        assert_eq!(
            p.delay_for(0, Some(Duration::from_secs(5))),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn exhaustion_respects_max_retries() {
        let p = policy();
        assert!(!p.exhausted(4));
        assert!(p.exhausted(5));
    }
}
