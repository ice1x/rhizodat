"""Hybrid logical clock for transaction time (``dat_tx_time``).

Follows Kulkarni et al., "Logical Physical Clocks" (2014). A timestamp is a
wall-clock millisecond plus a logical counter that breaks ties and absorbs
clock steps back. Timestamps from other hosts are merged with ``update``;
wall clocks of different hosts are never compared directly.

Wire format: one non-negative 64-bit integer, ``wall_ms << 16 | logical``,
with ``wall_ms < 2**47`` and ``logical < 2**16``. It fits a signed BIGINT /
Int64 column and sorts the same way as the timestamps.
"""

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

__all__ = [
    "ClockSkewError",
    "HLCTimestamp",
    "HybridLogicalClock",
    "LogicalOverflowError",
]

_WALL_BITS = 47
_LOGICAL_BITS = 16
_MAX_WALL = (1 << _WALL_BITS) - 1
_MAX_LOGICAL = (1 << _LOGICAL_BITS) - 1


class ClockSkewError(Exception):
    """A received timestamp is further ahead of the local wall clock than allowed."""


class LogicalOverflowError(Exception):
    """The logical counter is exhausted within one wall-clock millisecond."""


@dataclass(frozen=True, order=True, slots=True)
class HLCTimestamp:
    wall_ms: int
    logical: int = 0

    def __post_init__(self) -> None:
        if not 0 <= self.wall_ms <= _MAX_WALL:
            raise ValueError(f"wall_ms {self.wall_ms} out of range [0, 2**47)")
        if not 0 <= self.logical <= _MAX_LOGICAL:
            raise ValueError(f"logical {self.logical} out of range [0, 2**16)")

    def encode(self) -> int:
        return (self.wall_ms << _LOGICAL_BITS) | self.logical

    @classmethod
    def decode(cls, raw: int) -> "HLCTimestamp":
        if not 0 <= raw < 1 << (_WALL_BITS + _LOGICAL_BITS):
            raise ValueError(f"encoded timestamp {raw} out of range [0, 2**63)")
        return cls(raw >> _LOGICAL_BITS, raw & _MAX_LOGICAL)


def _system_ms() -> int:
    return time.time_ns() // 1_000_000


class HybridLogicalClock:
    """Thread-safe HLC. ``now`` for local and send events, ``update`` for receive."""

    def __init__(
        self,
        *,
        wall_ms: Callable[[], int] = _system_ms,
        max_offset_ms: int = 500,
    ) -> None:
        if max_offset_ms < 0:
            raise ValueError("max_offset_ms must be >= 0")
        self._wall_ms = wall_ms
        self._max_offset_ms = max_offset_ms
        self._lock = threading.Lock()
        self._last: HLCTimestamp | None = None

    @property
    def last(self) -> HLCTimestamp | None:
        """The latest timestamp issued, or ``None`` before the first event."""
        return self._last

    def now(self) -> HLCTimestamp:
        with self._lock:
            pt = self._wall_ms()
            last = self._last
            if last is None or pt > last.wall_ms:
                ts = HLCTimestamp(pt, 0)
            else:
                ts = self._tick(last.wall_ms, last.logical + 1)
            self._last = ts
            return ts

    def update(self, remote: HLCTimestamp) -> HLCTimestamp:
        with self._lock:
            pt = self._wall_ms()
            ahead = remote.wall_ms - pt
            if ahead > self._max_offset_ms:
                raise ClockSkewError(
                    f"remote timestamp is {ahead} ms ahead of the local wall clock "
                    f"(max_offset_ms={self._max_offset_ms})"
                )
            last = self._last or HLCTimestamp(0, 0)
            wall = max(last.wall_ms, remote.wall_ms, pt)
            if wall == last.wall_ms == remote.wall_ms:
                logical = max(last.logical, remote.logical) + 1
            elif wall == last.wall_ms:
                logical = last.logical + 1
            elif wall == remote.wall_ms:
                logical = remote.logical + 1
            else:
                logical = 0
            ts = self._tick(wall, logical)
            self._last = ts
            return ts

    @staticmethod
    def _tick(wall: int, logical: int) -> HLCTimestamp:
        if logical > _MAX_LOGICAL:
            raise LogicalOverflowError(
                f"logical counter exhausted at wall_ms={wall}; the wall clock is stuck "
                "or a peer is sending timestamps too fast"
            )
        return HLCTimestamp(wall, logical)
