"""Result model returned by native store access.

A ``Result`` is a single-pass iterator over rows (mappings from column name to value),
because store cursors are. ``first``, ``one`` and ``all`` consume it; reading a
consumed result raises ``ResultConsumedError``. ``raw`` keeps the
backend-specific object (cursor, query result, scan) for anything the common
surface does not cover.

``to_arrow`` returns a ``pyarrow.Table``: from the backend when it produces
Arrow natively (an Iceberg scan), otherwise built from the rows. pyarrow is
not a core dependency.
"""

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from types import TracebackType
from typing import Any, Self

__all__ = ["Result", "ResultConsumedError", "Row"]

Row = Mapping[str, Any]


class ResultConsumedError(RuntimeError):
    """The result was already read to the end or closed."""


class Result:
    def __init__(
        self,
        rows: Iterable[Row],
        *,
        columns: Sequence[str] | None = None,
        raw: object = None,
        arrow: Callable[[], Any] | None = None,
        on_close: Callable[[], None] | None = None,
    ) -> None:
        self._rows: Iterator[Row] | None = iter(rows)
        self._columns = tuple(columns) if columns is not None else None
        self._arrow = arrow
        self._on_close = on_close
        self.raw = raw

    @property
    def columns(self) -> tuple[str, ...] | None:
        return self._columns

    def __iter__(self) -> Self:
        self._live()
        return self

    def __next__(self) -> Row:
        rows = self._live()
        try:
            return next(rows)
        except StopIteration:
            self.close()
            raise

    def first(self) -> Row | None:
        rows = self._live()
        try:
            return next(rows, None)
        finally:
            self.close()

    def one(self) -> Row:
        rows = self._live()
        try:
            row = next(rows, None)
            if row is None:
                raise LookupError("expected one row, got no rows")
            if next(rows, None) is not None:
                raise LookupError("expected one row, got more than one")
            return row
        finally:
            self.close()

    def all(self) -> list[Row]:
        rows = self._live()
        try:
            return list(rows)
        finally:
            self.close()

    def to_arrow(self) -> Any:  # noqa: ANN401 - pyarrow is optional, so no static type
        if self._arrow is not None:
            self._live()
            try:
                return self._arrow()
            finally:
                self.close()
        try:
            import pyarrow
        except ImportError as exc:
            raise ImportError("Result.to_arrow() needs pyarrow: pip install pyarrow") from exc
        rows = self.all()
        names = self._columns or (tuple(rows[0]) if rows else ())
        return pyarrow.table({name: [row[name] for row in rows] for name in names})

    def close(self) -> None:
        if self._rows is None and self._on_close is None:
            return
        self._rows = None
        hook, self._on_close = self._on_close, None
        if hook is not None:
            hook()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def _live(self) -> Iterator[Row]:
        if self._rows is None:
            raise ResultConsumedError("the result was already consumed or closed")
        return self._rows
