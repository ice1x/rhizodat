"""A :class:`~rhizodat.core.result.Result` over an Arrow IPC stream.

Native stores (``rhizodat_native``) answer ``execute`` with a query's full result
as the bytes of one Arrow IPC stream, read here into a pyarrow table: the rows are
the table's Python values (``to_pylist``), ``columns`` its names and ``to_arrow``
the table itself, so a caller that wants Arrow pays for no second conversion.
"""

from typing import Any

from rhizodat.core.result import Result

__all__ = ["empty", "from_ipc"]


def from_ipc(data: bytes, *, raw: object = None) -> Result:
    """``data`` (an Arrow IPC stream) as a ``Result``; needs pyarrow."""
    import pyarrow as pa

    with pa.ipc.open_stream(pa.py_buffer(data)) as reader:
        table: Any = reader.read_all()
    return Result(
        iter(table.to_pylist()),
        columns=tuple(table.column_names),
        raw=raw,
        arrow=lambda: table,
    )


def empty() -> Result:
    """The result of a statement that returns no rows (DDL, an insert)."""
    return Result(iter(()), columns=())
