//! Hybrid logical clock for transaction time (`dat_tx_time`).
//!
//! Follows Kulkarni et al., "Logical Physical Clocks" (2014). A timestamp is a
//! wall-clock millisecond plus a logical counter that breaks ties and absorbs
//! clock steps back. Timestamps from other hosts are merged with
//! [`HybridLogicalClock::update`]; wall clocks of different hosts are never
//! compared directly.
//!
//! Wire format: one non-negative 64-bit integer, `wall_ms << 16 | logical`, with
//! `wall_ms < 2^47`. It fits a signed BIGINT / Int64 column and sorts the same
//! way as the timestamps.

use std::fmt;
use std::sync::Mutex;

use crate::identity::system_ms;

const LOGICAL_BITS: u32 = 16;
const MAX_WALL: u64 = (1 << 47) - 1;

/// One HLC timestamp. Ordered by wall time, then by the logical counter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct HlcTimestamp {
    wall_ms: u64,
    logical: u16,
}

/// Why the clock refused to issue a timestamp.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClockError {
    /// A received timestamp is further ahead of the local wall clock than allowed.
    ClockSkew {
        /// How far ahead, in milliseconds.
        ahead_ms: u64,
        /// The configured tolerance.
        max_offset_ms: u64,
    },
    /// The logical counter is exhausted within one wall-clock millisecond.
    LogicalOverflow {
        /// The millisecond that ran out of logical values.
        wall_ms: u64,
    },
    /// A wall time outside `[0, 2^47)`, from the local clock or a decoded value.
    OutOfRange(i64),
}

impl fmt::Display for ClockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClockError::ClockSkew {
                ahead_ms,
                max_offset_ms,
            } => write!(
                f,
                "remote timestamp is {ahead_ms} ms ahead of the local wall clock \
                 (max_offset_ms={max_offset_ms})"
            ),
            ClockError::LogicalOverflow { wall_ms } => write!(
                f,
                "logical counter exhausted at wall_ms={wall_ms}; the wall clock is stuck \
                 or a peer is sending timestamps too fast"
            ),
            ClockError::OutOfRange(v) => write!(f, "HLC value {v} out of range"),
        }
    }
}

impl std::error::Error for ClockError {}

impl HlcTimestamp {
    /// A timestamp from its parts.
    ///
    /// # Errors
    ///
    /// [`ClockError::OutOfRange`] when `wall_ms >= 2^47`.
    pub fn new(wall_ms: u64, logical: u16) -> Result<Self, ClockError> {
        if wall_ms > MAX_WALL {
            return Err(ClockError::OutOfRange(
                i64::try_from(wall_ms).unwrap_or(i64::MAX),
            ));
        }
        Ok(Self { wall_ms, logical })
    }

    /// The wall-clock millisecond.
    #[must_use]
    pub const fn wall_ms(self) -> u64 {
        self.wall_ms
    }

    /// The logical counter within the millisecond.
    #[must_use]
    pub const fn logical(self) -> u16 {
        self.logical
    }

    /// The wire form, `wall_ms << 16 | logical`.
    #[must_use]
    pub fn encode(self) -> i64 {
        let raw = (self.wall_ms << LOGICAL_BITS) | u64::from(self.logical);
        // wall_ms < 2^47 keeps the encoding below 2^63.
        i64::try_from(raw).unwrap_or(i64::MAX)
    }

    /// The timestamp of a wire value.
    ///
    /// # Errors
    ///
    /// [`ClockError::OutOfRange`] for a negative value.
    pub fn decode(raw: i64) -> Result<Self, ClockError> {
        let bits = u64::try_from(raw).map_err(|_| ClockError::OutOfRange(raw))?;
        let logical = u16::try_from(bits & 0xFFFF).unwrap_or(u16::MAX);
        Self::new(bits >> LOGICAL_BITS, logical)
    }
}

type WallFn = dyn Fn() -> i64 + Send + Sync;

/// Thread-safe HLC: [`now`](Self::now) for local and send events,
/// [`update`](Self::update) for receive events.
pub struct HybridLogicalClock {
    wall_ms: Box<WallFn>,
    max_offset_ms: u64,
    last: Mutex<Option<HlcTimestamp>>,
}

impl fmt::Debug for HybridLogicalClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HybridLogicalClock")
            .field("max_offset_ms", &self.max_offset_ms)
            .field("last", &self.last())
            .finish_non_exhaustive()
    }
}

impl Default for HybridLogicalClock {
    fn default() -> Self {
        Self::new()
    }
}

impl HybridLogicalClock {
    /// The default tolerance for a peer's clock running ahead.
    pub const DEFAULT_MAX_OFFSET_MS: u64 = 500;

    /// A clock over the system wall clock with the default tolerance.
    #[must_use]
    pub fn new() -> Self {
        Self::with_wall(system_ms, Self::DEFAULT_MAX_OFFSET_MS)
    }

    /// A clock over an injected wall clock returning Unix milliseconds.
    pub fn with_wall(
        wall_ms: impl Fn() -> i64 + Send + Sync + 'static,
        max_offset_ms: u64,
    ) -> Self {
        Self {
            wall_ms: Box::new(wall_ms),
            max_offset_ms,
            last: Mutex::new(None),
        }
    }

    /// The latest timestamp issued, or `None` before the first event.
    #[must_use]
    pub fn last(&self) -> Option<HlcTimestamp> {
        *self.lock()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<HlcTimestamp>> {
        self.last
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn physical(&self) -> Result<u64, ClockError> {
        let raw = (self.wall_ms)();
        u64::try_from(raw)
            .ok()
            .filter(|ms| *ms <= MAX_WALL)
            .ok_or(ClockError::OutOfRange(raw))
    }

    /// A timestamp for a local or send event.
    ///
    /// # Errors
    ///
    /// [`ClockError::LogicalOverflow`] when the counter is exhausted, and
    /// [`ClockError::OutOfRange`] for a wall clock outside `[0, 2^47)`.
    pub fn now(&self) -> Result<HlcTimestamp, ClockError> {
        let mut last = self.lock();
        let pt = self.physical()?;
        let ts = match *last {
            Some(prev) if pt <= prev.wall_ms => tick(prev.wall_ms, u32::from(prev.logical) + 1)?,
            _ => HlcTimestamp {
                wall_ms: pt,
                logical: 0,
            },
        };
        *last = Some(ts);
        Ok(ts)
    }

    /// Merge a timestamp received from another host.
    ///
    /// # Errors
    ///
    /// [`ClockError::ClockSkew`] (state unchanged) when `remote` is more than
    /// `max_offset_ms` ahead of the local wall clock, and
    /// [`ClockError::LogicalOverflow`] when the counter is exhausted.
    pub fn update(&self, remote: HlcTimestamp) -> Result<HlcTimestamp, ClockError> {
        let mut last = self.lock();
        let pt = self.physical()?;
        let ahead = remote.wall_ms.saturating_sub(pt);
        if ahead > self.max_offset_ms {
            return Err(ClockError::ClockSkew {
                ahead_ms: ahead,
                max_offset_ms: self.max_offset_ms,
            });
        }
        let prev = last.unwrap_or(HlcTimestamp {
            wall_ms: 0,
            logical: 0,
        });
        let wall = prev.wall_ms.max(remote.wall_ms).max(pt);
        let (pl, rl) = (u32::from(prev.logical), u32::from(remote.logical));
        let logical = if wall == prev.wall_ms && wall == remote.wall_ms {
            pl.max(rl) + 1
        } else if wall == prev.wall_ms {
            pl + 1
        } else if wall == remote.wall_ms {
            rl + 1
        } else {
            0
        };
        let ts = tick(wall, logical)?;
        *last = Some(ts);
        Ok(ts)
    }
}

fn tick(wall_ms: u64, logical: u32) -> Result<HlcTimestamp, ClockError> {
    let logical = u16::try_from(logical).map_err(|_| ClockError::LogicalOverflow { wall_ms })?;
    Ok(HlcTimestamp { wall_ms, logical })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoding_sorts_like_the_timestamps() {
        let a = HlcTimestamp::new(10, u16::MAX).unwrap();
        let b = HlcTimestamp::new(11, 0).unwrap();
        assert!(a < b && a.encode() < b.encode());
        assert_eq!(HlcTimestamp::decode(b.encode()), Ok(b));
    }

    #[test]
    fn out_of_range_values_are_rejected() {
        assert!(HlcTimestamp::new(1 << 47, 0).is_err());
        assert!(HlcTimestamp::decode(-1).is_err());
        assert_eq!(
            HlcTimestamp::new(MAX_WALL, u16::MAX).unwrap().encode(),
            i64::MAX
        );
        let clock = HybridLogicalClock::with_wall(|| -5, 0);
        assert_eq!(clock.now(), Err(ClockError::OutOfRange(-5)));
        assert_eq!(clock.last(), None);
    }

    #[test]
    fn system_clock_is_monotonic() {
        let clock = HybridLogicalClock::new();
        let stamps: Vec<_> = (0..1000).map(|_| clock.now().unwrap()).collect();
        assert!(stamps.windows(2).all(|w| w[0] < w[1]));
    }
}
