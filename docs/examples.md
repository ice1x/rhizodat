# Examples

The scripts in [`examples/`](https://github.com/ice1x/rhizodat/tree/main/examples)
run as they are against the compose stack (`docker compose --profile clickhouse
up -d --wait`), and the test suite runs every one of them, so what is shown here
works. Point them at other stores with `RHIZODAT_PG`, `RHIZODAT_CLICKHOUSE`,
`RHIZODAT_GRAPH` and `RHIZODAT_JOURNAL`.

## One entity across two stores

```python
--8<-- "examples/quickstart.py"
```

## A Unit of Work with native statements

```python
--8<-- "examples/transactions.py"
```

## Valid time and transaction time

```python
--8<-- "examples/bitemporal.py"
```

## Schema migrations

`examples/migrations/0001_tasks.toml`:

```toml
--8<-- "examples/migrations/0001_tasks.toml"
```

```python
--8<-- "examples/migrate.py"
```

## Importing a CSV with its schema inferred

```python
--8<-- "examples/import_csv.py"
```
