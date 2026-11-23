"""Teardown that rolls back only the prepared branches its own test opened.

The drevo and PostgreSQL test servers are shared with other test processes (another
pytest run, ``cargo test``) that prepare their own ``dat:`` gids at the same time. A
teardown that aborts every ``dat:`` gid aborts theirs too, and they then see a
heuristic outcome, so a test rolls back only what it opened.
"""

import contextlib
import uuid
from collections.abc import Iterable
from typing import Any, TypeVar

import psycopg

from rhizodat.core.coordinator import Participant, parse_gid
from rhizodat.core.journal import BranchRecord

S = TypeVar("S")


class OwnBranches:
    """The ``dat_txn`` of every branch a test opened on the stores it tracks."""

    def __init__(self) -> None:
        self.txns: set[uuid.UUID] = set()

    def track(self, store: S) -> S:
        """Record the ``dat_txn`` of each branch ``store`` opens from now on."""
        original = store.branch  # type: ignore[attr-defined]

        def tracked(dat_txn: uuid.UUID, *args: Any, **kw: Any) -> Any:
            self.txns.add(dat_txn)
            return original(dat_txn, *args, **kw)

        setattr(store, "branch", tracked)  # noqa: B010 - an instance attribute, not the class
        return store

    def note(self, dat_txn: uuid.UUID) -> None:
        """Claim a ``dat_txn`` whose branches were opened some other way."""
        self.txns.add(dat_txn)

    def prepared(self, store: str, participant: Participant) -> list[str]:
        """This test's gids that ``store`` still holds prepared."""
        own = []
        for gid in participant.prepared_gids():
            parsed = parse_gid(gid)
            if parsed is not None and parsed[1] == store and parsed[0] in self.txns:
                own.append(gid)
        return own

    def abort_prepared(self, store: str, participant: Participant) -> None:
        """Roll back this test's prepared gids on ``store``; best effort, one by one."""
        for gid in self.prepared(store, participant):
            with contextlib.suppress(Exception):
                participant.abort(BranchRecord(store, gid, True))


# Locks on relations of the given schemas, in this database.
_IN_SCHEMAS = """
    JOIN pg_class c ON c.oid = l.relation
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
      AND n.nspname = ANY(%s)
"""


def release_schemas(conn: psycopg.Connection[Any], schemas: Iterable[str]) -> None:
    """Free ``schemas`` for ``DROP SCHEMA``: terminate idle-in-transaction sessions and
    roll back prepared transactions that hold a lock in them, and nothing else.

    A prepared transaction keeps its locks with no backend (``pid`` is null); its
    ``transactionid`` lock ties them to ``pg_prepared_xacts``. ``conn`` must be in
    autocommit mode.
    """
    names = list(schemas)
    conn.execute("SET lock_timeout = '10s'")
    conn.execute(
        "SELECT pg_terminate_backend(a.pid) FROM pg_stat_activity a"
        " WHERE a.pid <> pg_backend_pid() AND a.state LIKE 'idle in transaction%%'"
        f" AND a.pid IN (SELECT l.pid FROM pg_locks l {_IN_SCHEMAS})",
        (names,),
    )
    rows = conn.execute(
        "SELECT DISTINCT p.gid FROM pg_prepared_xacts p"
        " JOIN pg_locks x ON x.locktype = 'transactionid' AND x.pid IS NULL"
        " AND x.transactionid = p.transaction"
        " WHERE p.database = current_database() AND x.virtualtransaction IN"
        f" (SELECT l.virtualtransaction FROM pg_locks l {_IN_SCHEMAS} AND l.pid IS NULL)",
        (names,),
    ).fetchall()
    for (gid,) in rows:
        with contextlib.suppress(psycopg.errors.UndefinedObject):  # resolved meanwhile
            conn.execute(f"ROLLBACK PREPARED '{gid}'")
