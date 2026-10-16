# Getting started

## Install

```bash
pip install 'rhizodat[postgres,clickhouse,drevo,iceberg]'
```

Each store's driver is an extra: install only those you use. `rhizodat[ingest]`
adds file imports, `rhizodat[mcp]` the MCP server. Embedded drevo (`drevo:///`)
needs `drevo-py`, which is not on PyPI yet and is built from source.

## Run the stores

The repository's `compose.yaml` runs every store on 127.0.0.1 with empty,
throw-away data:

```bash
docker compose --profile clickhouse up -d --wait
```

## The first entity

An order whose money lives in PostgreSQL and whose links live in the graph.
Each write is one atomic meta-transaction over both stores; every version is
kept, so the order can be read as it was at any moment:

```python
--8<-- "examples/quickstart.py"
```

```bash
python examples/quickstart.py
```

Next: [more examples](examples.md), or how the
[meta-transactions](concepts/transactions.md) behind this work.
