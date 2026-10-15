"""rhizodat-MCP: the MCP server shipped with rhizodat.

It lets an agent work with the stores through rhizodat instead of four
drivers. This first version is read-only:

- ``stores``: the configured stores, their kind and DSN (secrets masked);
- ``query``: a native read (SQL, Cypher, an Iceberg scan) through the store's
  ``read_only`` access, which the store itself enforces: a PostgreSQL
  ``READ ONLY`` transaction, a drevo transaction that is always rolled back,
  ClickHouse ``readonly = 2``, an Iceberg scan.

Transactions, the journal and recovery stay inside rhizodat. Writes come as
entity CRUD tools over the Repository, each call one Unit of Work.
The tools live in ``rhizodat.mcp.tools`` (no MCP dependency); ``server``
exposes them over MCP (``pip install 'rhizodat[mcp]'``, then ``rhizodat-mcp``).
"""
