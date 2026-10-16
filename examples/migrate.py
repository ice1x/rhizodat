"""Schema migrations: one history for every store, kept in the journal.

The directory ``examples/migrations`` runs from code, or with
``rhizodat migrate examples/migrations``.
A second run applies nothing; ``schema_versions`` says what applied when.
"""

from pathlib import Path

from _stores import CLICKHOUSE, JOURNAL, PG

from rhizodat import Rhizodat

with Rhizodat(stores={"pg": PG, "events": CLICKHOUSE}, journal=JOURNAL) as dat:
    report = dat.migrate(Path(__file__).parent / "migrations")
    print(f"applied {report.applied}, already {report.already}")
    for v in dat.schema_versions():
        print(v.id, v.name, v.applied_at.isoformat())
