"""The Python facade's side of the Go binding's shared-type test.

Declares ``tracker.Issue`` as ``go/repository_test.go`` does, over the same two
PostgreSQL stores and journal, and either reads an issue (printing it as JSON)
or adds one (printing its id). Run by ``TestATypeIsSharedWithThePythonFacade``:

    python facade.py read|add DSN TRACKER_SCHEMA LABELS_SCHEMA JOURNAL_SCHEMA [ID]
"""

import json
import sys
import uuid
from datetime import datetime
from decimal import Decimal

from rhizodat import Rhizodat, entity
from rhizodat.adapters.postgres import PostgresJournal, PostgresStore


@entity(
    stores={
        "tracker": ["title", "points", "closed", "due"],
        "labels": ["labels", "estimate", "extra"],
    },
    name="tracker.Issue",
)
class Issue:
    title: str
    points: int
    closed: bool
    due: datetime | None
    labels: list[str] | None
    estimate: Decimal | None
    extra: dict[str, str] | None


def main() -> None:
    action, dsn, tracker, labels, journal = sys.argv[1:6]
    dat = Rhizodat(
        stores={
            "tracker": PostgresStore(dsn, name="tracker", schema=tracker),
            "labels": PostgresStore(dsn, name="labels", schema=labels),
        },
        journal=PostgresJournal(dsn, schema=journal),
    )
    issues = dat.repository(Issue)
    try:
        if action == "read":
            issue = issues.get(uuid.UUID(sys.argv[6]))
            if issue is None:
                print("null")
                return
            print(
                json.dumps(
                    {
                        "title": issue.title,
                        "points": issue.points,
                        "closed": issue.closed,
                        "due": issue.due.isoformat() if issue.due else None,
                        "labels": issue.labels,
                        "estimate": str(issue.estimate) if issue.estimate is not None else None,
                        "extra": issue.extra,
                        "created": issue.dat_created_at.isoformat(),
                    }
                )
            )
        else:
            outcome = issues.add(
                Issue(
                    title="From Python",
                    points=8,
                    closed=True,
                    due=None,
                    labels=["py"],
                    estimate=Decimal("1.25"),
                    extra={"by": "python"},
                )
            )
            print(outcome.value.dat_id)
    finally:
        dat.close()


if __name__ == "__main__":
    main()
