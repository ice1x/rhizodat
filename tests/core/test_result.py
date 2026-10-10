"""Result of native store access: one pass over rows, plus the raw backend object."""

import sys
from collections.abc import Iterator

import pytest

from datorium.core.result import Result, ResultConsumedError

ROWS = [{"id": 1, "status": "new"}, {"id": 2, "status": "paid"}, {"id": 3, "status": "new"}]


def counting(rows: list[dict[str, object]], pulled: list[int]) -> Iterator[dict[str, object]]:
    for row in rows:
        pulled.append(1)
        yield row


def test_iterates_rows_once() -> None:
    result = Result(iter(ROWS))
    assert list(result) == ROWS
    with pytest.raises(ResultConsumedError):
        list(result)


def test_all_returns_every_remaining_row() -> None:
    result = Result(iter(ROWS))
    assert next(iter(result)) == ROWS[0]
    assert result.all() == ROWS[1:]


def test_first_returns_the_first_row_and_stops_reading() -> None:
    pulled: list[int] = []
    result = Result(counting(ROWS, pulled))
    assert result.first() == ROWS[0]
    assert len(pulled) == 1
    with pytest.raises(ResultConsumedError):
        result.all()


def test_first_of_an_empty_result_is_none() -> None:
    assert Result(iter([])).first() is None


def test_one_requires_exactly_one_row() -> None:
    assert Result(iter(ROWS[:1])).one() == ROWS[0]
    with pytest.raises(LookupError, match="no rows"):
        Result(iter([])).one()
    with pytest.raises(LookupError, match="more than one"):
        Result(iter(ROWS)).one()


def test_close_hook_runs_once_when_consumed_or_closed() -> None:
    closed: list[bool] = []
    result = Result(iter(ROWS), on_close=lambda: closed.append(True))
    result.all()
    result.close()
    assert closed == [True]

    closed.clear()
    early = Result(iter(ROWS), on_close=lambda: closed.append(True))
    early.close()
    assert closed == [True]
    with pytest.raises(ResultConsumedError):
        early.first()


def test_context_manager_closes() -> None:
    closed: list[bool] = []
    with Result(iter(ROWS), on_close=lambda: closed.append(True)) as result:
        assert result.first() == ROWS[0]
    assert closed == [True]


def test_raw_and_columns_are_kept() -> None:
    cursor = object()
    result = Result(iter(ROWS), columns=("id", "status"), raw=cursor)
    assert result.raw is cursor
    assert result.columns == ("id", "status")


def test_columns_default_to_none() -> None:
    assert Result(iter(ROWS)).columns is None


def test_to_arrow_uses_the_backend_provider_when_given() -> None:
    table = object()
    result = Result(iter(ROWS), arrow=lambda: table)
    assert result.to_arrow() is table
    with pytest.raises(ResultConsumedError):
        result.to_arrow()


def test_to_arrow_without_pyarrow_explains_how_to_install(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    with pytest.raises(ImportError, match="pyarrow"):
        Result(iter(ROWS)).to_arrow()


def test_to_arrow_builds_a_table_from_rows() -> None:
    pa = pytest.importorskip("pyarrow")
    table = Result(iter(ROWS), columns=("id", "status")).to_arrow()
    assert isinstance(table, pa.Table)
    assert table.column_names == ["id", "status"]
    assert table.num_rows == 3
