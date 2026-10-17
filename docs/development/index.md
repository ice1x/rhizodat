# Development

`compose.yaml` runs the stores the tests need (ports on 127.0.0.1 only, data in
tmpfs, so every `up` starts empty):

```bash
docker compose up -d --wait   # PostgreSQL (2PC enabled) and a disposable drevo
docker compose --profile '*' up -d --wait   # every store, plus toxiproxy
uv sync --group drevo         # optional: embedded drevo-py from the pinned commit (Rust)
uv sync --group native        # optional: the core in Rust (crates/rhizodat-py, maturin)
uv run pytest
docker compose down
```

The tests read store DSNs from the environment, else from `.env.test`. A DSN from
`.env.test` whose store is not running skips that store's tests with a hint; a
DSN set in the environment is used as is, so a store that is down fails the run
(CI exports `.env.test` for that reason). Point the tests at other stores with
`RHIZODAT_TEST_PG_DSN`, `RHIZODAT_TEST_DREVO_BOLT`,
`RHIZODAT_TEST_CLICKHOUSE_DSN`, `RHIZODAT_TEST_ICEBERG_REST_DSN` and
`RHIZODAT_TEST_TOXIPROXY`. The drevo server must be
**disposable**: 2PC raises the prepared fence and blocks other writers.

```bash
uv sync                  # dev environment
uv run ruff check .      # lint
uv run ruff format .     # format
uv run mypy              # types (strict)
```

The Rust core is a Cargo workspace at the repository root (stable Rust, MSRV
1.94); its tests include the vectors in `conformance/`, and the adapter crates'
tests use the compose PostgreSQL, drevo, ClickHouse and Iceberg REST catalog
(skipped with a hint when a store is not running, unless its
`RHIZODAT_TEST_*` variable is set):

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

`crates/rhizodat-py` is the Python module `rhizodat_native` (PyO3): UUIDv7,
the hybrid logical clock, gids and migration files from the Rust core.
`uv sync --group native` builds and installs it (`uv sync` alone removes it
again: list both groups, e.g. `--group drevo --group native`); the package
then makes `new_id` with it, and `RHIZODAT_NATIVE=0` turns it off.
`tests/conformance/test_native_vectors.py` runs the vectors through it.
