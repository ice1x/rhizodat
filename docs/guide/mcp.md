# rhizodat-MCP (out of the box)

rhizodat ships with an MCP server, `rhizodat-mcp`, so that AI agents and tools
work with the stores through rhizodat rather than through four drivers. It is
part of the distribution, configured by the same DSNs, and built on the
library like the server.

**MCP is a client of rhizodat's API, isolated from its internals.** It calls
the Repository's CRUD (create, read with `as_of`, history, update, delete);
each call is one Unit of Work inside rhizodat, which alone deals with
transactions, the journal and recovery. No tool begins, commits or inspects a
meta-transaction, and none runs recovery: that is an operator process's job
(`Rhizodat.recovery()`).

✔ First version (Python, `rhizodat.mcp`, read-only). Tools, marked read-only
for clients:

| Tool | What it does |
|---|---|
| `stores` | the configured stores: name, kind, two-phase or not, whether `query` can read it, DSN with secrets masked |
| `query` | a native read (SQL, Cypher, an Iceberg scan) through the store's `read_only` access, at most `limit` rows (capped by `RHIZODAT_MCP_MAX_ROWS`) |

The store itself refuses writes in `query`, so a mistaken or injected query
cannot change data:
- PostgreSQL: one prepared statement in a `READ ONLY` transaction whose mode
  is fixed before the statement runs, always rolled back. Without both, the
  tests showed `SET TRANSACTION READ WRITE` or `COMMIT; INSERT …` getting
  through.
- drevo: a transaction that is always rolled back, since drevo has no
  read-only mode.
- ClickHouse: `readonly = 2`, which also refuses lifting it per query.
- Iceberg: a scan.

As defence in depth, the DSNs can still use read-only accounts.

Configuration is the environment, so secrets stay out of command lines:
`RHIZODAT_STORE_<NAME>=<dsn>` per store, `RHIZODAT_JOURNAL=<dsn>`, and
optionally `RHIZODAT_MCP_MAX_ROWS` (default 1000), `RHIZODAT_MCP_ENTITIES`
and `RHIZODAT_MCP_WRITES` (below). The former
`RHIZODAT_MCP_RECOVERY*` settings are refused with an error, so a stale
configuration does not pass silently. An MCP client starts it over stdio:

```json
{
  "mcpServers": {
    "rhizodat": {
      "command": "rhizodat-mcp",
      "env": {
        "RHIZODAT_STORE_ERP": "postgresql://reader:…@db:5432/erp",
        "RHIZODAT_STORE_LAKE": "iceberg+rest://catalog:8181/warehouse",
        "RHIZODAT_JOURNAL": "postgresql://reader:…@db:5432/erp"
      }
    }
  }
}
```

✔ Entity CRUD tools over the Repository. `RHIZODAT_MCP_ENTITIES` names the
application's `@entity` classes by module path (`tracker.models:Bug,…`; the
package must be importable by the server). Each tool is one Repository call,
so one Unit of Work over every store holding the entity's fields.

| Tool | What it does |
|---|---|
| `entity_types` | each offered type: its fields (with `valid_from` / `valid_to` for a valid-time type) and how to write each (a reference is the target's `dat_id`, a decimal a string, a date or time ISO 8601, bytes `{"base64": …}`), the stores holding them, and whether delete is soft (restorable, for how long) or hard |
| `get_entity` | `{"entity": …}` by `dat_id`, as known at `known_at` (default now); for a valid-time type the version holding at `as_of`; times ISO 8601 with a time zone; `null` when there is none; soft-deleted ones only with `include_deleted` |
| `entity_history` | every committed version, oldest first |
| `create_entity` | a new entity from a value for every field; returns it with its `dat_id` |
| `update_entity` | a new version with some fields changed; the others are kept |
| `delete_entity` | soft delete (hidden, restorable) for a type that has it, else erased for good; marked destructive |
| `restore_entity` | brings a soft-deleted entity back within its window |

The read tools are always offered with `RHIZODAT_MCP_ENTITIES`. The write
tools only appear with `RHIZODAT_MCP_WRITES=1`: off by default, since a
delete erases unless the type says otherwise. Values are checked against
each field's type before anything is written: unknown or missing fields,
service fields (`dat_id`, `dat_*_at`) and values that do not fit are refused,
with the field named. A result says `pending: true` when the decision is
durable but a store applied after it is still being finished.
