"""rhizodat-MCP over MCP (the ``mcp`` SDK, ``pip install 'rhizodat[mcp]'``).

``rhizodat-mcp`` reads its configuration from the environment (see
``rhizodat.mcp.config``) and serves the tools over stdio.
"""

import os
import sys
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from rhizodat.mcp.config import ConfigError, config_from_env
from rhizodat.mcp.tools import DEFAULT_LIMIT, Tools

if TYPE_CHECKING:
    from mcp.server.mcpserver import MCPServer

__all__ = ["build_server", "main"]

INSTRUCTIONS = """\
rhizodat is one data layer over several stores (relational, analytical, graph, lakehouse,
or any other a plug-in adapter adds). Call `stores` first: it names the configured stores
and their kind. `query` runs a native read in a store's own language; the store refuses
writes. Entities (records whose fields may span several stores) are read and, if enabled,
written through the entity tools: `entity_types` lists them and how to write each field.
Each entity call is atomic across its stores; times are ISO 8601 with a time zone."""

QUERY_HELP = """\
A native read on one store, which refuses any write. Returns at most `limit` rows and
whether more were left."""


def query_description(languages: dict[str, str]) -> str:
    """The `query` tool's description: the native language of each configured kind."""
    lines = "".join(f"\n- {kind}: {help_}" for kind, help_ in sorted(languages.items()))
    return f"{QUERY_HELP}\nQuery languages by store kind:{lines}" if lines else QUERY_HELP


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
    """The MCP server over ``tools``."""
    from mcp.server.mcpserver import MCPServer
    from mcp_types import ToolAnnotations

    import rhizodat

    server: MCPServer = MCPServer(
        "rhizodat", instructions=INSTRUCTIONS, version=rhizodat.__version__
    )
    read_only = ToolAnnotations(read_only_hint=True, open_world_hint=False)

    @server.tool(annotations=read_only)
    @_guarded
    def stores() -> list[dict[str, Any]]:
        """The configured stores: name, kind (postgres, clickhouse, drevo, iceberg), whether
        it commits in two phases, whether `query` can read it, and its DSN with secrets
        masked."""
        return tools.stores()

    @server.tool(annotations=read_only, description=query_description(tools.query_languages()))
    @_guarded
    def query(
        store: str,
        query: str,
        params: dict[str, Any] | list[Any] | None = None,
        limit: int = DEFAULT_LIMIT,
    ) -> dict[str, Any]:
        return tools.query(store, query, params, limit)

    if tools.entity_type_names:
        _entity_tools(server, tools, read_only)
    return server


def _entity_tools(server: "MCPServer", tools: Tools, read_only: Any) -> None:  # noqa: ANN401
    from mcp_types import ToolAnnotations

    @server.tool(annotations=read_only)
    @_guarded
    def entity_types() -> list[dict[str, Any]]:
        """The entity types offered: each field and how to write it (references are the
        target's dat_id, decimals strings, bytes {"base64": ...}), the stores holding
        them, and whether delete is soft (restorable) or hard (erased for good)."""
        return tools.entity_types()

    @server.tool(annotations=read_only)
    @_guarded
    def get_entity(
        type: str,
        dat_id: str,
        as_of: str | None = None,
        known_at: str | None = None,
        include_deleted: bool = False,
    ) -> dict[str, Any]:
        """One entity by dat_id as {"entity": ...}; the entity is null when there is none.
        known_at: as rhizodat knew it then (default now). as_of: for a type with valid time
        (see entity_types), the version holding at that moment; otherwise the same as
        known_at. Times are ISO 8601 with a time zone. Soft-deleted ones only with
        include_deleted."""
        found = tools.get_entity(
            type, dat_id, as_of=as_of, known_at=known_at, include_deleted=include_deleted
        )
        return {"entity": found}

    @server.tool(annotations=read_only)
    @_guarded
    def entity_history(type: str, dat_id: str) -> list[dict[str, Any]]:
        """Every committed version of one entity, oldest first."""
        return tools.entity_history(type, dat_id)

    if not tools.writes:
        return
    write = ToolAnnotations(read_only_hint=False, destructive_hint=False, open_world_hint=False)
    erase = ToolAnnotations(read_only_hint=False, destructive_hint=True, open_world_hint=False)

    @server.tool(annotations=write)
    @_guarded
    def create_entity(type: str, values: dict[str, Any]) -> dict[str, Any]:
        """Create an entity from a value for every field (see entity_types); returns it with
        its new dat_id. Atomic across the stores holding its fields."""
        return tools.create_entity(type, values)

    @server.tool(annotations=write)
    @_guarded
    def update_entity(type: str, dat_id: str, values: dict[str, Any]) -> dict[str, Any]:
        """Change some fields of an entity (a new version; the old one stays in history)."""
        return tools.update_entity(type, dat_id, values)

    @server.tool(annotations=erase)
    @_guarded
    def delete_entity(type: str, dat_id: str) -> dict[str, Any]:
        """Delete an entity. For a type with soft delete it is hidden and restorable for a
        while; otherwise every version is erased for good."""
        return tools.delete_entity(type, dat_id)

    @server.tool(annotations=write)
    @_guarded
    def restore_entity(type: str, dat_id: str) -> dict[str, Any]:
        """Bring back a soft-deleted entity within its restore window."""
        return tools.restore_entity(type, dat_id)


def main() -> None:
    """The ``rhizodat-mcp`` command: configure from the environment, serve over stdio."""
    try:
        config = config_from_env(os.environ)
    except ConfigError as exc:
        print(f"rhizodat-mcp: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
    from rhizodat.mcp.entities import load_entity_types

    try:
        entities = load_entity_types(",".join(config.entities))
    except ValueError as exc:
        print(f"rhizodat-mcp: RHIZODAT_MCP_ENTITIES: {exc}", file=sys.stderr)
        raise SystemExit(2) from None
    tools = Tools.open(
        config.stores,
        journal=config.journal,
        max_rows=config.max_rows,
        entities=entities,
        writes=config.writes,
    )
    try:
        build_server(tools).run()
    finally:
        tools.close()
