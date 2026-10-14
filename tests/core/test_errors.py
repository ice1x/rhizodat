"""Error classification: what a store's error means, and the store-full error."""

import errno

import pytest
from faker import Faker

from datorium import Aborted, StoreFull
from datorium.core.errors import Capacity, Classifies, ErrorKind, classify
from datorium.core.identity import new_id


class OnlyRetryable:
    """An adapter that predates ``classify``: only ``is_retryable``."""

    def is_retryable(self, error: BaseException) -> bool:
        return isinstance(error, TimeoutError)


class Classifying(OnlyRetryable):
    def classify(self, error: BaseException) -> ErrorKind:
        return ErrorKind.CONSTRAINT


def test_without_classify_is_retryable_decides() -> None:
    source = OnlyRetryable()
    assert not isinstance(source, Classifies)
    assert classify(source, TimeoutError()) is ErrorKind.TRANSIENT
    assert classify(source, ValueError()) is ErrorKind.PERMANENT


def test_an_adapter_classify_wins() -> None:
    assert isinstance(Classifying(), Classifies)
    assert classify(Classifying(), TimeoutError()) is ErrorKind.CONSTRAINT


@pytest.mark.parametrize("code", [errno.ENOSPC, errno.EDQUOT])
def test_no_space_left_on_a_device_is_a_full_store_for_any_adapter(code: int) -> None:
    assert classify(OnlyRetryable(), OSError(code, "No space left on device")) is (
        ErrorKind.STORE_FULL
    )


def test_a_full_device_is_found_behind_a_driver_error() -> None:
    outer = RuntimeError("write failed")
    outer.__cause__ = OSError(errno.ENOSPC, "No space left on device")
    assert classify(OnlyRetryable(), outer) is ErrorKind.STORE_FULL


def test_only_transient_errors_are_worth_a_retry() -> None:
    assert [k for k in ErrorKind if k.retryable] == [ErrorKind.TRANSIENT]


def test_store_full_is_an_abort_that_names_the_store(faker: Faker) -> None:
    txn, store = new_id(), faker.word()
    error = StoreFull(txn, store=store, reason="disk full", free=1024)
    assert isinstance(error, Aborted)
    assert (error.dat_txn, error.store, error.free, error.retryable) == (txn, store, 1024, False)
    assert store in str(error)
    assert "disk full" in str(error)


def test_capacity_is_a_capability() -> None:
    class Disk:
        def free_space(self) -> int | None:
            return 0

    assert isinstance(Disk(), Capacity)
    assert not isinstance(OnlyRetryable(), Capacity)
