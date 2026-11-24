# Go (C ABI)

The rhizodat core has a C ABI, `librhizodat_ffi` (`crates/rhizodat-ffi`), and a
Go package over it, `github.com/ice1x/rhizodat/go`, linked statically through
cgo. PostgreSQL, ClickHouse, drevo and Iceberg (REST and Glue catalogs)
stores, the PostgreSQL and drevo journals and meta-transactions over any of
them work, and entity repositories over any of these stores, with
references and valid time. It commits through the same Rust path as the native
Python facade (`crates/rhizodat-units`).

## Build

```bash
cargo build --release -p rhizodat-ffi
cd go && go test ./...
```

The package links `target/release/librhizodat_ffi.a`; for a library built
elsewhere, set `CGO_LDFLAGS` to it.

The reference for how a Go program uses it is the cloud-operations scenario,
[`go/cloudops_test.go`](https://github.com/ice1x/rhizodat/blob/main/go/cloudops_test.go):
an inventory in PostgreSQL, the dependencies in drevo, telemetry in ClickHouse
and the raw API requests in Iceberg, changed together.

## A meta-transaction

Go code does not run inside the commit: it fills a unit of work, and the core
commits it — every store's writes, or none.

```go
inventory, _ := rhizodat.OpenPostgres(dsn, "inventory", "ops")
billing, _ := rhizodat.OpenPostgres(dsn, "billing", "billing")
journal, _ := rhizodat.OpenPostgresJournal(dsn, "rhizodat") // the schema must exist
coordinator, _ := rhizodat.NewCoordinator(journal)

done, err := coordinator.Transactional(3, func(u *rhizodat.Unit) error {
	if err := u.Insert(inventory, "resource", []string{"name", "size"},
		[][]any{{"web1", 2}}); err != nil {
		return err
	}
	return u.Insert(billing, "charge", []string{"resource", "cents"},
		[][]any{{"web1", 1200}})
})
// done.DatTxn is on every row as dat_txn; a retryable abort was re-run.
```

`Commit` fails with `*rhizodat.AbortedError` (nothing written; `Retryable`
says whether a re-run may help), `*rhizodat.InDoubtError` (recovery resolves
it; do not re-run), or `*rhizodat.Error` for a unit of work the core refuses
(two stores without a journal, a reserved column). `Transactional` re-runs the
whole function while the abort is retryable.

The other stores join the same unit of work: `u.InsertClickHouse(ch, table,
columns, rows)` (one-phase, applied after the decision from the journal) and
`u.Run(graph, cypher, rhizodat.Params{...})` for drevo (two-phase; the
statement gets `$dat_txn` and `$dat_tx_time`). A drevo journal
(`OpenDrevoJournal`) must live in a graph that holds no store's data.

Iceberg: `lake.CreateTable("ops.api_request", []rhizodat.Column{...})`
(or `RunOperations` with the migration step's operations; the native core has
`create_namespace` and `create_table`), `u.Append(lake, table, columns, rows)`
in a unit of work (columns matched by name and cast to the table's types at
the commit; a table needs `dat_txn` and `dat_tx_time` columns), and
`lake.Scan(table, rhizodat.ScanOptions{Columns, Limit, SnapshotID, Filter})`
with a pyiceberg row filter (`"status >= 500"`).

`store.Execute(sql, params...)` runs one statement outside any
meta-transaction, with `$1..$n` bound by the statement's types; it returns
`*rhizodat.Rows`. Dates and times bind through text
(`$1::text::timestamptz`), as on the native Python facade.
`ClickHouseStore.Execute(sql, rhizodat.Params{...})` binds `{name:Type}`
placeholders server-side; its rows travel as Arrow, so cast a `UUID` column
(`toString(id)`) and use `Date32` for dates (a `Date` arrives as its day
number). `DrevoStore.Execute(cypher, params)` returns columns in name order.

## Entities

An entity type is a struct that embeds `rhizodat.Entity` and maps each stored
field to a store by name:

```go
type Issue struct {
	rhizodat.Entity
	Title  string   `rhizodat:"title,store=tracker"`
	Points int      `rhizodat:"points,store=tracker"`
	Labels []string `rhizodat:"labels,store=search"`
}

tracker, _ := rhizodat.OpenPostgres(dsn, "tracker", "tracker")
search, _ := rhizodat.OpenPostgres(dsn, "search", "search")
issues, _ := rhizodat.NewRepository[Issue](coordinator,
	[]rhizodat.PartStore{tracker, search},
	rhizodat.EntityOptions{Type: "tracker.Issue", SoftDelete: 30 * 24 * time.Hour})
_ = issues.Install()

issue := &Issue{Title: "Crash on save", Points: 3}
_, err := issues.Add(issue) // one meta-transaction over both stores
// issue.DatID, DatCreatedAt and DatUpdatedAt are stamped
issue.Points = 5
_, err = issues.Update(issue)

now, _ := issues.Get(issue.DatID, rhizodat.ReadOptions{})
then, _ := issues.Get(issue.DatID, rhizodat.ReadOptions{AsOf: &yesterday})
versions, _ := issues.History(issue.DatID) // oldest first
```

As in Python, each write adds a version of every part in one
meta-transaction (re-run while it aborts retryably), and a read assembles the
parts of one version, as known at `AsOf` / `KnownAt` (a type without valid
time has one moment of knowledge: give one). `Add` fails with
`ErrEntityExists`, `Update` and `Delete` with `ErrEntityNotFound`. `Delete`
erases every version, unless the type has a `SoftDelete` window: then it hides
the entity, `Restore` brings it back within the window
(`ErrNotRestorable` after), and `PurgeExpired` erases it once the window is
over. `issues.In(unit)` is the repository inside your own unit of work: its
reads see the unit's writes, and they commit with the rest of it.

Tables and columns are the Python facade's (a type `tracker.Issue` lives in
`tracker_issue`; `string` is `text`, integers `bigint`, floats `double
precision`, `time.Time` `timestamptz`, `rhizodat.UUID` / `Decimal` / `Date`
`uuid` / `numeric` / `date`, `[]byte` `bytea`, lists and maps `jsonb`), and so
are drevo's nodes (one per version, labelled `tracker_issue`; strings,
numbers and booleans as properties, a `UUID` as its text, anything else as
tagged JSON, a field named `title` as `dat_title`), so a type declared alike
on both facades shares its versions. ClickHouse keeps a part in a table
named the same way (strings, integers, floats, booleans, `UUID`, `time.Time`
and `Date` natively, anything else as tagged JSON in a `String`), applied
after the decision like its rows, and Iceberg in `dat.tracker_issue` (the
store's `PartsNamespace`, `dat` by default; `[]byte` is `binary`, a `UUID` its
text, `Decimal` and lists tagged JSON text). An entity may span every kind of
store:
`NewRepository[Service](coordinator, []rhizodat.PartStore{inventory, graph},
...)` writes the PostgreSQL row and the drevo node of each version in one
meta-transaction. A pointer field is
nullable, and an empty `UUID`, `Decimal` or `Date` is stored as null. A field
without a `rhizodat` tag is not stored.

### References and valid time

A field of type `rhizodat.Ref[Person]` (or a list or map of them) refers to
other entities by id, as Python's `Ref["Person"]`: stored as the id, and in
drevo every version links the entities it names (`-[:assignee]->` an anchor
node). `rhizodat.RefTo(person)` makes one; the zero `Ref` is null.

A type that embeds `rhizodat.Validity` is bitemporal (Python's
`valid_time=True`): `ValidFrom` / `ValidTo` are the period a version holds
for (a zero end is open), every part stores them, and `Get` with `AsOf`
returns, among the versions known at `KnownAt`, the latest one whose period
holds at `AsOf`:

```go
type Contract struct {
	rhizodat.Entity
	rhizodat.Validity
	Price int `rhizodat:"price,store=billing"`
}
march := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
c, _ := contracts.Get(id, rhizodat.ReadOptions{AsOf: &march})                     // the price that held then
c, _ = contracts.Get(id, rhizodat.ReadOptions{AsOf: &march, KnownAt: &lastWeek}) // as it was known
```

A type declared alike on both facades is one type: the Go test
`TestATypeIsSharedWithThePythonFacade` writes an issue from Go that the
Python facade reads, and updates one the Python facade wrote.

## Entity parts

Under a repository, each store's share of an entity is a **part**, and every
write adds a version of it (see [repositories](repository.md)). The Go
package also reaches the parts of any store directly:

```go
spec := rhizodat.PartSpec{Type: "tracker.Issue", Table: "tracker_issue",
	Columns: []rhizodat.PartColumn{{Name: "title", Type: "text"}, {Name: "points", Type: "bigint"}}}
_ = pg.InstallPart(spec)
coordinator.Transactional(3, func(u *rhizodat.Unit) error {
	return u.WritePart(pg, spec, id, map[string]any{"title": "Crash", "points": 3},
		nil /* created: this is the first version */, false /* deleted */)
})
latest, _ := pg.ReadPart(spec, id, nil)       // nil: whatever is known now
before, _ := pg.ReadPart(spec, id, &knownAt) // as known at a transaction time
history, _ := pg.PartHistory(spec, id)       // every version, oldest first
```

`u.OwnPart` is the unit of work's own latest version (before the commit),
`u.ErasePart` removes every version at the commit, `PartVersions` scans every
entity's versions and `ExpiredParts` finds soft deletes past a time. Column
types are PostgreSQL's (`text`, `boolean`, `bigint`, `double precision`,
`numeric`, `timestamptz`, `date`, `uuid`, `bytea`, and `jsonb` for anything
else, lists and maps included).

## Values

| Go | rhizodat |
|---|---|
| `nil`, `bool`, `string` | themselves |
| any integer type | `int64` |
| `float32`, `float64` | `float64` (NaN, infinities kept) |
| `rhizodat.UUID`, `rhizodat.Decimal`, `rhizodat.Date` | uuid, exact decimal, date |
| `time.Time`, `rhizodat.NaiveDateTime` | datetime with / without offset |
| `[]byte` | bytes |
| slices | lists (`[]any` back) |
| `rhizodat.Map`, `map[string]any` | maps (`rhizodat.Map` back, in order) |

## The C ABI

The header is `crates/rhizodat-ffi/include/rhizodat.h`, generated by cbindgen
(a test fails when it is stale). Every function:

- returns a `RhizodatStatus` (`OK`, `INVALID_ARGUMENT`, `FAILED`, `PANIC`) and
  writes its results to out-pointers only on `OK`;
- reports an error as text through its last argument, `char **error`, when it
  is not null;
- hands out strings the caller frees with `rhizodat_string_free`;
- never lets a panic cross the boundary: it becomes `PANIC`;
- passes values as the core's tagged JSON (`{"$": "uuid", "v": …}` for what
  JSON cannot carry), the format of the journal payloads;
- frees a handle (`RhizodatPostgresStore *`, …) with its `*_free` function.

`rhizodat_coordinator_commit` returns `ABORTED` or `IN_DOUBT` for those
outcomes and writes the outcome JSON (with the meta-transaction id) in every
case.

In Go a failure is an `*rhizodat.Error` with its `Status` and message, matched
with `errors.Is(err, rhizodat.ErrFailed)` and the other sentinels.
