"""Retry policy: bounded attempts, an overall deadline, exponential backoff with full jitter.

Full jitter (``delay = uniform(0, min(max_delay, base * 2**attempt))``) matters
here: drevo's prepared fence makes concurrent writers fail at the same moment,
and without jitter they would retry in lockstep.
"""

from collections.abc import Callable
from dataclasses import dataclass

__all__ = ["Retry"]


@dataclass(frozen=True, slots=True)
class Retry:
    attempts: int = 5
    deadline_s: float = 10.0
    base_delay_s: float = 0.05
    max_delay_s: float = 2.0

    def __post_init__(self) -> None:
        if self.attempts < 1:
            raise ValueError("attempts must be >= 1")
        if self.deadline_s <= 0:
            raise ValueError("deadline_s must be > 0")
        if not 0 <= self.base_delay_s <= self.max_delay_s:
            raise ValueError("need 0 <= base_delay_s <= max_delay_s")

    def delay(self, attempt: int, rand: Callable[[], float]) -> float:
        """Sleep before retry number ``attempt`` (0 = first retry); ``rand`` is uniform [0, 1)."""
        ceiling = min(self.max_delay_s, self.base_delay_s * 2.0**attempt)
        return rand() * ceiling
