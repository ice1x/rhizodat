"""The retry policy: its bounds are checked, and the backoff is full jitter
under an exponential ceiling that stops at ``max_delay_s``."""

import pytest
from hypothesis import given
from hypothesis import strategies as st

from rhizodat.core.retry import Retry

delays = st.floats(min_value=0, max_value=60, allow_nan=False)


@st.composite
def policies(draw: st.DrawFn) -> Retry:
    base = draw(delays)
    return Retry(
        attempts=draw(st.integers(1, 20)),
        deadline_s=draw(st.floats(min_value=0.001, max_value=600)),
        base_delay_s=base,
        max_delay_s=draw(st.floats(min_value=base, max_value=120)),
    )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"attempts": 0}, "attempts"),
        ({"deadline_s": 0}, "deadline_s"),
        ({"deadline_s": -1.0}, "deadline_s"),
        ({"base_delay_s": -0.1}, "base_delay_s"),
        ({"base_delay_s": 3.0, "max_delay_s": 2.0}, "base_delay_s"),
    ],
)
def test_a_policy_out_of_bounds_is_refused(kwargs: dict[str, float], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        Retry(**kwargs)  # type: ignore[arg-type]


@given(policies(), st.integers(0, 200), st.floats(min_value=0, max_value=1, exclude_max=True))
def test_a_delay_is_within_the_ceiling(retry: Retry, attempt: int, r: float) -> None:
    ceiling = min(retry.max_delay_s, retry.base_delay_s * 2.0**attempt)
    assert 0 <= retry.delay(attempt, lambda: r) <= ceiling <= retry.max_delay_s


@given(policies(), st.integers(0, 60))
def test_the_ceiling_never_shrinks_with_attempts(retry: Retry, attempt: int) -> None:
    def top(n: int) -> float:
        return retry.delay(n, lambda: 1.0)

    assert top(attempt) <= top(attempt + 1)


def test_the_ceiling_doubles_then_stops_at_the_maximum() -> None:
    retry = Retry(base_delay_s=0.5, max_delay_s=3.0)
    assert [retry.delay(n, lambda: 1.0) for n in range(5)] == [0.5, 1.0, 2.0, 3.0, 3.0]
    assert retry.delay(4, lambda: 0.0) == 0.0
