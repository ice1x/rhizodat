# Adapter contracts and plug-ins

## Adapter contracts

The core knows no particular database: every store, rhizodat's own four
included, is a plug-in adapter (`rhizodat.core.adapter`).

```python
@dataclass(frozen=True)
class Scheme:                        # one DSN scheme and its rules
    name: str                        # "postgresql", "drevo+bolt", …
    transport: str | None = None
    default_port: int | None = None
    host: "required" | "optional" | "none" = "optional"
    path: "required" | "optional" | "none" = "optional"

@dataclass(frozen=True)
class Adapter:                       # a store kind
    kind: str                        # "postgres", "drevo", …
    schemes: tuple[Scheme, ...]
    open_store: Callable[[name, dsn], Store] | None = None        # None: journal only (file)
    open_journal: Callable[[dsn, wall_ms], Journal] | None = None  # shared ACID stores only
    journal_location: Callable[[dsn], tuple | None] | None = None  # compared with fence_location
    query_help: str | None = None    # its native query language, for MCP clients

class Store:                         # what open_store returns
    name: str
    two_phase: bool
    def branch(self, dat_txn) -> Branch: ...
    def participant(self) -> Participant: ...
    def execute(self, query, params=None) -> Result: ...  # native access, outside Units of Work
    def close(self) -> None: ...

# optional capabilities, checked with isinstance
class ReadOnlyQueries:  def read_only(self, query, params=None) -> Result: ...  # the store refuses writes
class AppliedLocation:  applied_location: tuple | None   # two stores in one place are refused
class Fenced:           fence_location: tuple | None     # the journal must not live there
class PartStore:        ...  # entity parts: install_part, read_part, part_history, expired_parts
class PartBranch:       ...  # on a branch: write_part, erase_part, read_part (see Repository)

class Branch:                        # one store's part of one Unit of Work
    store: str
    two_phase: bool                  # True for PostgreSQL and drevo
    def stage(self, dat_txn, dat_tx_time) -> bytes | None: ...  # slow, lock-free; payload for the journal
    def prepare(self, gid: str) -> None: ...   # lock-holding; called last
    def commit_one_phase(self) -> None: ...    # the only branch: native commit
    def rollback(self) -> None: ...            # before the decision
    def is_retryable(self, error: Exception) -> bool: ...
    def classify(self, error: Exception) -> ErrorKind: ...  # optional (Classifies)

class Participant:                   # the store after the decision and for recovery
    def finish(self, branch, dat_txn, dat_tx_time) -> None: ...  # idempotent; unknown gid = done
    def is_applied(self, branch, dat_txn) -> bool: ...           # resolves ambiguous outcomes
    def abort(self, branch) -> None: ...                         # roll back a prepared gid
    def prepared_gids(self) -> list[str]: ...
    def is_retryable(self, error: Exception) -> bool: ...
    def classify(self, error: Exception) -> ErrorKind: ...  # optional (Classifies)

class Capacity:          ...  # optional, on a store: free_space() -> bytes | None

class Result:                        # single pass over rows (mappings)
    columns: tuple[str, ...] | None
    def __iter__(self): ...
    def first(self): ...             # first row or None; consumes
    def one(self): ...               # exactly one row, else LookupError
    def all(self): ...               # remaining rows
    def to_arrow(self): ...          # backend's Arrow if native, else built (needs pyarrow)
    def close(self): ...             # also a context manager
    raw: object                      # backend-specific data
```

A result follows the store cursor: it is read once, and reading it again
raises `ResultConsumedError`.

## Plug-in adapters

A package adds a database by declaring one entry point; rhizodat itself is not
changed:

```toml
[project.entry-points."rhizodat.adapters"]
mysql = "rhizodat_mysql:ADAPTER"
```

```python
ADAPTER = Adapter(
    kind="mysql",
    schemes=(Scheme("mysql", default_port=3306, host="required"),),
    open_store=lambda name, dsn: MySqlStore(dsn, name=name),
    open_journal=lambda dsn, wall_ms: MySqlJournal(dsn, wall_ms=wall_ms),
    query_help="SQL with %s placeholders",
)
```

`register_adapter(ADAPTER)` does the same inside one process. rhizodat's own
adapters are loaded the same way, first. A kind or scheme registered twice is
refused; a plug-in that fails to load is skipped, and the error is named when a
DSN needs a scheme nobody registered. Adapters are imported on first use, and
drivers only when a store opens.

An adapter keeps rhizodat's guarantees only if its store fits one of the two
branch models: real two-phase commit (prepare, then commit or roll back a
prepared transaction by gid, listing the prepared ones), or one phase with an
idempotent apply of the journal payload after the decision (`is_applied`
answers whether it is visible). A store that can do neither cannot join a
meta-transaction with others.

**The adapter contract** (`rhizodat.testing.contract`, `rhizodat[testing]`) is
how an adapter shows it keeps those promises on the real store. The author
writes a harness (open an isolated store, write one record by key in a branch,
count committed records by key) and subclasses `AdapterContract`:

```python
class TestMySqlContract(AdapterContract):
    @pytest.fixture
    def harness(self) -> Iterator[MySqlHarness]: ...
```

It checks:
- nothing is visible or applied before `finish`;
- `finish` commits once however often it runs;
- `abort` discards and is idempotent, and an aborted branch never becomes
  visible;
- a single branch commits natively, and rollback before the decision discards;
- plain errors are not retryable; errors are classified (`PERMANENT`, and
  `STORE_FULL` for a device out of space), and the store's own full-store error
  is `STORE_FULL` when the harness names one (`store_full_error()`);
- `free_space()`, where the store has it, is bytes or `None`;
- the commit protocol, crashed at every step: a meta-transaction over the
  adapter's store and an in-memory two-phase store, the process "dies" (open
  transactions die with it, prepared ones and staged data stay), and one
  `Recovery` run must end all-or-nothing with nothing left prepared.

rhizodat's own adapters pass it in `tests/contract/` (PostgreSQL, ClickHouse,
drevo embedded and over Bolt, Iceberg on SQLite and REST).

The guide for adapter authors, with the criteria a store must meet and a
table of supported, candidate and non-viable databases, is
[the adapter guide](../adapters/index.md).

**A dry run first: `rhizodat adapter check`.** Before the contract, the CLI
reports what an adapter implements, without writing anything:

```bash
rhizodat adapter check rhizodat_mysql:ADAPTER --dsn mysql://localhost/test
rhizodat adapter check postgres --dsn postgresql://localhost/app --json
```

Without `--dsn` it checks the declaration only (kind, DSN schemes, what it
opens). With one it opens the store and checks:
- the methods rhizodat calls on the store, a branch (opened and rolled back)
  and the participant (`prepared_gids()` listed);
- that plain errors are not retryable and a device out of space is
  `STORE_FULL`;
- every optional capability: `ReadOnlyQueries`, `AppliedLocation`, `Fenced`,
  `Classifies`, `Capacity` (with the bytes it reports), `PartStore` /
  `PartBranch`.

Each check is required or optional and passed, failed or not run. The
summary counts them, with a completeness percentage. The DSN's password and
secret parameters are masked. Exit status: 0 when every required check that
ran passed, 1 otherwise, 2 when the adapter cannot be loaded.

**In Rust** the core knows no store either (`rhizodat_core::adapter`): each
adapter crate exports `ADAPTER` (kind, `Scheme`s with the same host / path
rules, whether it holds the journal, its query language) and parses its own
DSNs with `ADAPTER.parse`; an application builds a `Registry` from the crates
it links (`Registry::new().with(rhizodat_postgres::ADAPTER)?…`), which refuses
the same conflicts as Python. `postgres_schema` lives in `rhizodat-postgres`.
The conformance vectors run in `crates/rhizodat-conformance`, which links every
adapter. Opening stores by DSN from Rust comes with a Rust facade (the
bindings).

## Connection strings

| DSN | Store | Driver | Default port |
|---|---|---|---|
| `postgresql://…` (alias `postgres://`) | PostgreSQL | `psycopg` | 5432 |
| `clickhouse://…` | ClickHouse | `clickhouse-connect` | 8123 |
| `drevo:///path/graph.drevo`, `drevo+mem://` (tests only) | drevo, embedded | `drevo-py` | — |
| `drevo+bolt://host:7687` | drevo, remote | `neo4j` driver | 7687 |
| `iceberg+rest://…`, `iceberg+sql://…`, `iceberg+glue://…` | Iceberg | `pyiceberg` | — |
| `file:///path` | development journal only | — | — |

- PostgreSQL: `schema=<name>` puts a store (its `dat_applied` and
  `search_path`) or the journal in that schema, so stores configured only by
  DSN, such as rhizodat-MCP's, can share a database. It is removed before the
  DSN reaches the driver; a `schema=` argument as well must name the same
  schema. A `search_path` set through `options=` is not seen by the
  `dat_applied` check: use `schema=`.
- Iceberg: `iceberg+rest://host:8181/<warehouse>` (`secure=true` for HTTPS);
  `iceberg+sql://user:pass@host:5432/db?warehouse=s3://…` keeps the catalog in
  PostgreSQL, `iceberg+sql:///abs/catalog.db?warehouse=file:///…` in SQLite.
  Other query parameters become pyiceberg catalog properties (`s3.endpoint`,
  `credential`, …).
- The schemes and their rules come from the registered adapters (see
  plug-in adapters above); these are rhizodat's own.
- Parsing never imports a driver. Scheme matching ignores case; user,
  password and path are percent-decoded; a repeated query parameter is an error.
- `drevo:///` and `file:///` take a local absolute path and no host;
  `clickhouse://`, `drevo+bolt://` and `iceberg+rest://` require a host.
- Passwords and query parameters whose name contains `pass`, `secret`, `token`,
  `key` or `credential` are masked in every printed form and in errors; the
  original string is handed only to the driver.
