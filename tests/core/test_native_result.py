"""A ``Result`` over an Arrow IPC stream: what native stores hand back from ``execute``.

The core returns a query's full result as the bytes of one Arrow IPC stream;
:func:`rhizodat.core.native_result.from_ipc` wraps them in the Python facade's
:class:`~rhizodat.core.result.Result`, whose rows are the table's Python values and
whose ``to_arrow`` is the table itself (no second conversion).
"""

from typing import Any

import pytest
from faker import Faker

pa = pytest.importorskip("pyarrow")

from rhizodat.core.native_result import from_ipc  # noqa: E402
from rhizodat.core.result import Result, ResultConsumedError  # noqa: E402


def ipc(table: Any) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return bytes(sink.getvalue().to_pybytes())


def test_rows_are_the_tables_python_values(faker: Faker) -> None:
    names = [faker.name() for _ in range(3)]
    table = pa.table({"id": [1, 2, 3], "name": names})
    result = from_ipc(ipc(table))
    assert isinstance(result, Result)
    assert result.columns == ("id", "name")
    assert result.all() == [{"id": i + 1, "name": n} for i, n in enumerate(names)]


def test_to_arrow_is_the_stream_itself() -> None:
    table = pa.table({"qty": pa.array([1, None, 3], pa.int64())})
    assert from_ipc(ipc(table)).to_arrow().equals(table)


def test_a_stream_of_several_batches_is_one_table() -> None:
    table = pa.Table.from_batches([pa.record_batch({"n": [1, 2]}), pa.record_batch({"n": [3]})])
    result = from_ipc(ipc(table))
    assert [r["n"] for r in result.all()] == [1, 2, 3]


def test_an_empty_stream_has_columns_and_no_rows() -> None:
    table = pa.table({"sku": pa.array([], pa.string())})
    result = from_ipc(ipc(table))
    assert result.columns == ("sku",)
    assert result.first() is None


def test_a_result_is_read_once() -> None:
    result = from_ipc(ipc(pa.table({"n": [1]})))
    assert result.one() == {"n": 1}
    with pytest.raises(ResultConsumedError):
        result.to_arrow()


def test_the_raw_object_is_kept() -> None:
    marker = object()
    assert from_ipc(ipc(pa.table({"n": [1]})), raw=marker).raw is marker


def test_not_a_stream_is_refused() -> None:
    with pytest.raises(pa.ArrowInvalid):
        from_ipc(b"not arrow")
