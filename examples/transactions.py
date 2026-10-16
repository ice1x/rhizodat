"""A Unit of Work with native statements in two stores, re-run on transient errors.

A worker claims a task in PostgreSQL and marks itself busy in the graph: both
or neither. ``transactional`` re-runs the whole function with a new
meta-transaction when a store reports a transient failure.
"""

from _stores import GRAPH, JOURNAL, PG

from rhizodat import Retry, Rhizodat, UnitOfWork

with Rhizodat(stores={"pg": PG, "graph": GRAPH}, journal=JOURNAL) as dat:
    dat.store("pg").execute(
        "CREATE TABLE IF NOT EXISTS example_task (id serial PRIMARY KEY, claimed_by text)"
    )
    dat.store("pg").execute("INSERT INTO example_task (claimed_by) VALUES (NULL)")
    dat.store("graph").execute(
        "MERGE (w:ExampleWorker {title: 'ExampleWorker:ada', name: 'ada'}) SET w.busy = false"
    )

    @dat.transactional(retry=Retry(attempts=5, deadline_s=10))
    def claim(uow: UnitOfWork, worker: str) -> int:
        pg = uow.store("pg")
        row = pg.execute(
            "SELECT id FROM example_task WHERE claimed_by IS NULL ORDER BY id LIMIT 1"
        ).one()
        pg.execute("UPDATE example_task SET claimed_by = %s WHERE id = %s", (worker, row["id"]))
        uow.store("graph").execute(
            "MATCH (w:ExampleWorker {name: $w}) SET w.busy = true", {"w": worker}
        )
        return int(row["id"])

    outcome = claim("ada")  # Committed (or CommittedPending); raises Aborted on failure
    print(f"claimed task {outcome.value}")
