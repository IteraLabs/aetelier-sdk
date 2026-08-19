use std::time::Duration;

use rand::Rng;

#[derive(Debug, Clone, Copy)]
pub struct Jitter {
    pub min: Duration,
    pub max: Duration,
}

impl Default for Jitter {
    fn default() -> Self {
        Self {
            min: Duration::from_millis(120),
            max: Duration::from_millis(650),
        }
    }
}

impl Jitter {
    pub fn new(min: Duration, max: Duration) -> Self {
        Self {
            min,
            max: max.max(min),
        }
    }

    pub fn sample_with<R: Rng>(&self, rng: &mut R) -> Duration {
        if self.max <= self.min {
            return self.min;
        }
        let span = (self.max - self.min).as_secs_f64();
        self.min + Duration::from_secs_f64(rng.random_range(0.0..=span))
    }

    pub fn sample(&self) -> Duration {
        self.sample_with(&mut rand::rng())
    }

    pub async fn wait(&self) {
        tokio::time::sleep(self.sample()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    #[test]
    fn samples_stay_within_bounds_and_vary() {
        let jitter = Jitter::new(Duration::from_millis(50), Duration::from_millis(250));
        let mut rng = StdRng::seed_from_u64(11);
        let samples: Vec<_> = (0..500).map(|_| jitter.sample_with(&mut rng)).collect();
        assert!(samples.iter().all(|d| *d >= Duration::from_millis(50)));
        assert!(samples.iter().all(|d| *d <= Duration::from_millis(250)));
        assert!(samples.iter().any(|d| *d != samples[0]));
    }

    #[test]
    fn inverted_bounds_are_clamped() {
        let jitter = Jitter::new(Duration::from_millis(400), Duration::from_millis(100));
        let mut rng = StdRng::seed_from_u64(5);
        assert_eq!(jitter.sample_with(&mut rng), Duration::from_millis(400));
    }
}
