"""datorium-MCP over MCP (the ``mcp`` SDK, ``pip install 'datorium[mcp]'``).

``datorium-mcp`` reads its configuration from the environment (see
``datorium.mcp.config``) and serves the tools over stdio.
"""

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
in the journal, including any a crashed process left for recovery."""


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


def build_server(tools: Tools) -> "MCPServer":
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

    return server


def main() -> None:
    """The ``datorium-mcp`` command: configure from the environment, serve over stdio."""
    try:
        config = config_from_env(os.environ)
    except ConfigError as exc:
        print(f"datorium-mcp: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
    tools = Tools.open(config.stores, journal=config.journal, max_rows=config.max_rows)
    try:
        build_server(tools).run()
    finally:
        tools.close()
