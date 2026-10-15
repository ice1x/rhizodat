"""Hybrid logical clock (Kulkarni et al., 2014) for dat_tx_time."""

import itertools
import time

import pytest
from hypothesis import given
from hypothesis import strategies as st

from rhizodat.core.clock import (
    ClockSkewError,
    HLCTimestamp,
    HybridLogicalClock,
    LogicalOverflowError,
)

MAX_WALL = 2**47 - 1
MAX_LOGICAL = 2**16 - 1

walls = st.integers(min_value=0, max_value=MAX_WALL)
logicals = st.integers(min_value=0, max_value=MAX_LOGICAL)
timestamps = st.builds(HLCTimestamp, walls, logicals)


class FakeWall:
    def __init__(self, ms: int) -> None:
        self.ms = ms

    def __call__(self) -> int:
        return self.ms


def clock_at(ms: int, max_offset_ms: int = 500) -> tuple[HybridLogicalClock, FakeWall]:
    wall = FakeWall(ms)
    return HybridLogicalClock(wall_ms=wall, max_offset_ms=max_offset_ms), wall


# --- timestamp -------------------------------------------------------------


@given(timestamps)
def test_encoding_round_trips(ts: HLCTimestamp) -> None:
    assert HLCTimestamp.decode(ts.encode()) == ts


@given(timestamps, timestamps)
def test_encoding_preserves_order(a: HLCTimestamp, b: HLCTimestamp) -> None:
    assert (a < b) == (a.encode() < b.encode())


@given(timestamps)
def test_encoding_fits_a_signed_64_bit_column(ts: HLCTimestamp) -> None:
    assert 0 <= ts.encode() < 2**63


def test_encoding_layout_is_wall_then_logical() -> None:
    assert HLCTimestamp(1, 2).encode() == (1 << 16) | 2


@pytest.mark.parametrize(
    ("wall", "logical"), [(-1, 0), (MAX_WALL + 1, 0), (0, -1), (0, MAX_LOGICAL + 1)]
)
def test_out_of_range_components_are_rejected(wall: int, logical: int) -> None:
    with pytest.raises(ValueError, match="out of range"):
        HLCTimestamp(wall, logical)


@pytest.mark.parametrize("raw", [-1, 2**63])
def test_out_of_range_encoding_is_rejected(raw: int) -> None:
    with pytest.raises(ValueError, match="out of range"):
        HLCTimestamp.decode(raw)


# --- local events ----------------------------------------------------------


def test_now_follows_the_wall_clock() -> None:
    clock, wall = clock_at(1_000)
    assert clock.now() == HLCTimestamp(1_000, 0)
    wall.ms = 1_005
    assert clock.now() == HLCTimestamp(1_005, 0)


def test_now_ticks_logically_within_one_millisecond() -> None:
    clock, _ = clock_at(1_000)
    assert [clock.now() for _ in range(3)] == [
        HLCTimestamp(1_000, 0),
        HLCTimestamp(1_000, 1),
        HLCTimestamp(1_000, 2),
    ]


def test_now_never_goes_back_with_the_wall_clock() -> None:
    clock, wall = clock_at(2_000)
    first = clock.now()
    wall.ms = 1_000
    second = clock.now()
    assert second > first
    assert second == HLCTimestamp(2_000, 1)


def test_logical_overflow_raises_instead_of_wrapping() -> None:
    clock, _ = clock_at(1_000)
    assert clock.update(HLCTimestamp(1_000, MAX_LOGICAL - 1)) == HLCTimestamp(1_000, MAX_LOGICAL)
    with pytest.raises(LogicalOverflowError):
        clock.now()
    assert clock.last == HLCTimestamp(1_000, MAX_LOGICAL)


@given(st.lists(st.integers(min_value=0, max_value=MAX_WALL), min_size=1, max_size=50))
def test_now_is_strictly_monotonic_for_any_wall_clock(readings: list[int]) -> None:
    wall = FakeWall(readings[0])
    clock = HybridLogicalClock(wall_ms=wall)
    seen = []
    for ms in readings:
        wall.ms = ms
        seen.append(clock.now())
    assert all(a < b for a, b in itertools.pairwise(seen))


# --- receive events --------------------------------------------------------


def test_update_with_an_older_remote_ticks_locally() -> None:
    clock, _ = clock_at(1_000)
    clock.now()
    assert clock.update(HLCTimestamp(900, 7)) == HLCTimestamp(1_000, 1)


def test_update_adopts_a_remote_that_is_ahead_within_the_offset() -> None:
    clock, _ = clock_at(1_000)
    assert clock.update(HLCTimestamp(1_300, 4)) == HLCTimestamp(1_300, 5)
    assert clock.now() == HLCTimestamp(1_300, 6)


def test_update_with_equal_walls_takes_the_larger_logical() -> None:
    clock, _ = clock_at(1_000)
    clock.update(HLCTimestamp(1_000, 3))
    assert clock.update(HLCTimestamp(1_000, 9)) == HLCTimestamp(1_000, 10)


def test_update_when_the_wall_clock_passed_both_resets_logical() -> None:
    clock, wall = clock_at(1_000)
    clock.update(HLCTimestamp(1_000, 9))
    wall.ms = 2_000
    assert clock.update(HLCTimestamp(1_500, 3)) == HLCTimestamp(2_000, 0)


def test_update_rejects_a_remote_too_far_ahead_and_keeps_state() -> None:
    clock, _ = clock_at(1_000, max_offset_ms=500)
    before = clock.now()
    with pytest.raises(ClockSkewError, match="501 ms ahead"):
        clock.update(HLCTimestamp(1_501, 0))
    assert clock.last == before


def test_last_is_none_before_any_event() -> None:
    clock, _ = clock_at(1_000)
    assert clock.last is None


def test_negative_max_offset_is_rejected() -> None:
    with pytest.raises(ValueError, match="max_offset_ms"):
        HybridLogicalClock(max_offset_ms=-1)


@given(
    st.integers(min_value=0, max_value=10**12),
    st.lists(
        st.tuples(st.booleans(), st.integers(0, 400), logicals.map(lambda x: x % 1000)),
        max_size=40,
    ),
)
def test_every_event_is_after_the_last_and_after_any_received(
    start: int, events: list[tuple[bool, int, int]]
) -> None:
    clock, wall = clock_at(start)
    last = None
    for is_receive, remote_delta, remote_logical in events:
        if is_receive:
            remote = HLCTimestamp(wall.ms + remote_delta, remote_logical)
            ts = clock.update(remote)
            assert ts > remote
        else:
            ts = clock.now()
        if last is not None:
            assert ts > last
        assert ts.wall_ms >= wall.ms
        last = ts


def test_system_wall_clock_is_used_by_default() -> None:
    before = time.time_ns() // 1_000_000
    ts = HybridLogicalClock().now()
    after = time.time_ns() // 1_000_000
    assert before <= ts.wall_ms <= after
