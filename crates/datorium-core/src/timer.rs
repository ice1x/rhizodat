//! Time for the coordinator: a monotonic clock and an async sleep.
//!
//! The core is runtime-agnostic; a binding passes a timer over its runtime.

use std::time::{Duration, Instant};

use async_trait::async_trait;

/// A monotonic clock and a way to wait.
#[async_trait]
pub trait Timer: Send + Sync {
    /// Time since an arbitrary fixed point; never goes back.
    fn monotonic(&self) -> Duration;

    /// Wait for `duration`.
    async fn sleep(&self, duration: Duration);
}

/// A timer that never waits: for tests and policies without backoff.
#[derive(Debug)]
pub struct NoWait(Instant);

impl Default for NoWait {
    fn default() -> Self {
        Self(Instant::now())
    }
}

#[async_trait]
impl Timer for NoWait {
    fn monotonic(&self) -> Duration {
        self.0.elapsed()
    }

    async fn sleep(&self, _duration: Duration) {}
}

/// The tokio runtime's clock and sleep.
#[cfg(feature = "tokio")]
#[derive(Debug)]
pub struct TokioTimer(tokio::time::Instant);

#[cfg(feature = "tokio")]
impl Default for TokioTimer {
    fn default() -> Self {
        Self(tokio::time::Instant::now())
    }
}

#[cfg(feature = "tokio")]
#[async_trait]
impl Timer for TokioTimer {
    fn monotonic(&self) -> Duration {
        self.0.elapsed()
    }

    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}
