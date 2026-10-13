"""datorium-MCP over MCP (the ``mcp`` SDK, ``pip install 'datorium[mcp]'``).

``datorium-mcp`` reads its configuration from the environment (see
``datorium.mcp.config``) and serves the tools over stdio.
"""

import math
import os
import sys
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from datorium.mcp.config import ConfigError, config_from_env
from datorium.mcp.tools import DEFAULT_LIMIT, Tools

if TYPE_CHECKING:
    from mcp.server.mcpserver import MCPServer

__all__ = ["build_server", "main"]

INSTRUCTIONS = """\
datorium is one data layer over PostgreSQL, ClickHouse, drevo (a graph) and Apache Iceberg.
Call `stores` first: it names the configured stores and their kind. `query` runs a native
read in a store's own language (SQL for postgres and clickhouse, Cypher for drevo, a table
name with scan parameters for iceberg); the store refuses writes. Writes in datorium are
meta-transactions over several stores; `transaction` and `pending_transactions` show them
in the journal, including any a crashed process left for recovery. If `run_recovery` is
offered, it finishes or rolls back what crashed processes left: run it only when asked."""


def _guarded[**P, T](fn: Callable[P, T]) -> Callable[P, T]:
    """Report a store's or the tool's error to the client as a tool error with its text."""
    import functools

    from mcp.server.mcpserver.exceptions import ToolError

    @functools.wraps(fn)
    def run(*args: P.args, **kwargs: P.kwargs) -> T:
        try:
            return fn(*args, **kwargs)
        except KeyError as exc:
            raise ToolError(str(exc.args[0]) if exc.args else repr(exc)) from exc
        except Exception as exc:
            raise ToolError(f"{type(exc).__name__}: {exc}") from exc

    return run


def build_server(tools: Tools, *, recovery: bool = False) -> "MCPServer":
    """The MCP server over ``tools``; ``recovery`` also offers ``run_recovery``, the one
    tool that writes."""
    from mcp.server.mcpserver import MCPServer
    from mcp_types import ToolAnnotations

    import datorium

    server: MCPServer = MCPServer(
        "datorium", instructions=INSTRUCTIONS, version=datorium.__version__
    )
    read_only = ToolAnnotations(read_only_hint=True, open_world_hint=False)

    @server.tool(annotations=read_only)
    @_guarded
    def stores() -> list[dict[str, Any]]:
        """The configured stores: name, kind (postgres, clickhouse, drevo, iceberg), whether
        it commits in two phases, whether `query` can read it, and its DSN with secrets
        masked."""
        return tools.stores()

    @server.tool(annotations=read_only)
    @_guarded
    def query(
        store: str,
        query: str,
        params: dict[str, Any] | list[Any] | None = None,
        limit: int = DEFAULT_LIMIT,
    ) -> dict[str, Any]:
        """A native read on one store, which refuses any write. postgres: SQL with %s or
        %(name)s placeholders; clickhouse: SQL with {name:Type} placeholders; drevo:
        Cypher with $name parameters; iceberg: a table name (namespace.table), with scan
        parameters (row_filter, selected_fields, limit). Returns at most `limit` rows and
        whether more were left."""
        return tools.query(store, query, params, limit)

    @server.tool(annotations=read_only)
    @_guarded
    def transaction(dat_txn: str) -> dict[str, Any] | None:
        """One meta-transaction's journal record: its state (ACTIVE, PREPARED, COMMITTED,
        ABORTED, DONE), commit time, branches per store, and how long since it changed;
        null if the journal does not know it."""
        return tools.transaction(dat_txn)

    @server.tool(annotations=read_only)
    @_guarded
    def pending_transactions(older_than_s: float = 0, limit: int = DEFAULT_LIMIT) -> dict[str, Any]:
        """Unfinished meta-transactions (ACTIVE, PREPARED, COMMITTED) unchanged for at least
        `older_than_s` seconds, oldest first: in flight, or left by a crash for recovery."""
        return tools.pending_transactions(older_than_s, limit)

    if recovery:

        @server.tool(
            annotations=ToolAnnotations(
                read_only_hint=False,
                destructive_hint=True,
                idempotent_hint=True,
                open_world_hint=False,
            )
        )
        @_guarded
        def run_recovery(limit: int = DEFAULT_LIMIT) -> dict[str, list[str]]:
            """One recovery run over the journal and the stores, as a crashed process
            would need: rolls back prepared transactions the journal has no decision for
            (orphans), aborts meta-transactions left undecided past the stale cutoff, and
            finishes decided ones on every store. Never touches a transaction a live
            process may still be deciding. Returns the dat_txn of each, by outcome; `stuck`
            ones stay decided but unfinished for too long and need attention."""
            return tools.run_recovery(limit)

    return server


def main() -> None:
    """The ``datorium-mcp`` command: configure from the environment, serve over stdio."""
    try:
        config = config_from_env(os.environ)
    except ConfigError as exc:
        print(f"datorium-mcp: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
    tools = Tools.open(
        config.stores,
        journal=config.journal,
        max_rows=config.max_rows,
        stale_after_ms=math.ceil(config.stale_after_s * 1000),
    )
    try:
        build_server(tools, recovery=config.recovery).run()
    finally:
        tools.close()
