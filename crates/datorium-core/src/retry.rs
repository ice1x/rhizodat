//! Retry policy: bounded attempts, an overall deadline, exponential backoff
//! with full jitter.
//!
//! Full jitter (`delay = uniform(0, min(max_delay, base * 2^attempt))`) matters
//! here: drevo's prepared fence makes concurrent writers fail at the same
//! moment, and without jitter they would retry in lockstep.

use std::fmt;
use std::time::Duration;

/// How long and how often to retry.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Retry {
    attempts: u32,
    deadline: Duration,
    base_delay: Duration,
    max_delay: Duration,
}

/// Retry settings that make no sense.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidRetry(&'static str);

impl fmt::Display for InvalidRetry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for InvalidRetry {}

impl Default for Retry {
    /// Five attempts within ten seconds, 50 ms to 2 s apart.
    fn default() -> Self {
        Self {
            attempts: 5,
            deadline: Duration::from_secs(10),
            base_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(2),
        }
    }
}

impl Retry {
    /// A policy.
    ///
    /// # Errors
    ///
    /// [`InvalidRetry`] for zero attempts, a zero deadline or `base_delay > max_delay`.
    pub fn new(
        attempts: u32,
        deadline: Duration,
        base_delay: Duration,
        max_delay: Duration,
    ) -> Result<Self, InvalidRetry> {
        if attempts < 1 {
            return Err(InvalidRetry("attempts must be >= 1"));
        }
        if deadline.is_zero() {
            return Err(InvalidRetry("deadline must be > 0"));
        }
        if base_delay > max_delay {
            return Err(InvalidRetry("need base_delay <= max_delay"));
        }
        Ok(Self {
            attempts,
            deadline,
            base_delay,
            max_delay,
        })
    }

    /// Attempts in total, the first included.
    #[must_use]
    pub const fn attempts(&self) -> u32 {
        self.attempts
    }

    /// The overall time budget.
    #[must_use]
    pub const fn deadline(&self) -> Duration {
        self.deadline
    }

    /// The sleep before retry number `attempt` (0 = first retry); `rand` is uniform in [0, 1).
    #[must_use]
    pub fn delay(&self, attempt: u32, rand: f64) -> Duration {
        let factor = 2_f64.powi(i32::try_from(attempt).unwrap_or(i32::MAX));
        let ceiling = self.max_delay.min(self.base_delay.mul_f64(factor.min(1e9)));
        ceiling.mul_f64(rand.clamp(0.0, 1.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn backoff_doubles_up_to_the_cap_with_full_jitter() {
        let r = Retry::new(5, ms(1000), ms(10), ms(50)).unwrap();
        let ceilings: Vec<_> = (0..5).map(|a| r.delay(a, 0.999_999)).collect();
        assert_eq!(
            ceilings.iter().map(Duration::as_millis).collect::<Vec<_>>(),
            [9, 19, 39, 49, 49]
        );
        assert_eq!(r.delay(3, 0.0), Duration::ZERO);
        assert_eq!(r.delay(u32::MAX, 0.5), ms(25));
    }

    #[test]
    fn nonsense_is_rejected() {
        assert!(Retry::new(0, ms(1), ms(0), ms(0)).is_err());
        assert!(Retry::new(1, ms(0), ms(0), ms(0)).is_err());
        assert!(Retry::new(1, ms(1), ms(2), ms(1)).is_err());
    }
}
