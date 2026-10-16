# Package structure

```
rhizodat/
├── core/
│   ├── identity.py      # dat_id, UUIDv7
│   ├── clock.py         # hybrid logical clock
│   ├── adapter.py       # adapter registry, plug-ins, capability protocols
│   ├── dsn.py           # connection strings (schemes from the adapters)
│   ├── mapping.py       # @entity, store mapping
│   ├── parts.py         # entity parts: versions, PartStore / PartBranch
│   ├── repository.py    # entity CRUD, versions, soft delete
│   ├── unit_of_work.py
│   ├── coordinator.py   # commit protocol, recovery
│   ├── journal.py
│   ├── rows.py          # one-phase payloads: row batches (ClickHouse)
│   ├── staged.py        # one-phase payloads: staged files (Iceberg)
│   └── result.py
├── adapters/
│   ├── postgres.py
│   ├── drevo.py
│   ├── clickhouse.py
│   └── iceberg.py
├── mcp/                 # rhizodat-MCP: tools, configuration, server
├── testing/             # the adapter and parts contract suites (rhizodat[testing]), memory stores
└── migrations/
```

Store drivers are optional extras: `rhizodat[postgres]`, `rhizodat[clickhouse]`,
`rhizodat[drevo]`, `rhizodat[iceberg]`; `rhizodat[mcp]` adds the MCP server.
`drevo-py` is not on PyPI yet and is installed from source.
