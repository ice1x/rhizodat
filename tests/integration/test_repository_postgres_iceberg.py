"""ERP on the Repository: invoice headers in PostgreSQL, their amounts in an Iceberg lake.

Each invoice's bookkeeping fields live in PostgreSQL (2PC) and its figures in
Iceberg (a data file staged before the decision, committed as a snapshot
after it), so every write is a meta-transaction over both. A crash after the
decision leaves the Iceberg part behind until recovery; reads stay consistent
meanwhile. A hard delete rewrites the lake's data files.
"""

# @entity adds dat_id and the service times at runtime; mypy cannot see them.
# mypy: disable-error-code="attr-defined"

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from faker import Faker

from fakes import Crash
from iceberg_parts import iceberg_parts_store
from rhizodat import CommittedPending, Retry, Rhizodat, entity
from rhizodat.adapters.iceberg import IcebergStore
from rhizodat.adapters.postgres import PostgresJournal, PostgresStore
from rhizodat.core.clock import HybridLogicalClock
from rhizodat.core.repository import NotRestorable

DAY_MS = 86_400_000


@entity(
    stores={
        "books": ["number", "customer", "status"],
        "lake": ["amount", "currency", "booked_at", "lines"],
    },
    name="erp.Invoice",
    soft_delete=timedelta(days=30),
)
class Invoice:
    number: str
    customer: str
    status: str
    amount: Decimal
    currency: str
    booked_at: datetime | None
    lines: list[str]


@entity(stores={"books": ["number"], "lake": ["amount"]}, name="erp.Quote")
class Quote:
    number: str
    amount: Decimal


class Wall:
    def __init__(self) -> None:
        self.ms = int(datetime(2026, 10, 15, 8, 0, tzinfo=UTC).timestamp() * 1000)

    def __call__(self) -> int:
        return self.ms

    def now(self) -> datetime:
        return datetime.fromtimestamp(self.ms / 1000, UTC)


@dataclass
class Office:
    dat: Rhizodat
    lake: IcebergStore
    wall: Wall

    def recover(self) -> object:
        self.wall.ms += 60_000
        return self.dat.recovery(wall_ms=self.wall).run_once()

    def lake_rows(self, table: str, dat_id: object) -> int:
        rows = self.lake.execute(
            f"{self.lake.parts_namespace}.{table}", {"row_filter": f"dat_id == '{dat_id}'"}
        )
        return len(rows.all())


@pytest.fixture
def office(pg_dsn: str, pg_schema: str, tmp_path: Path) -> Iterator[Office]:
    wall = Wall()
    pg = PostgresStore(pg_dsn, name="books", schema=pg_schema)
    lake_dsn = f"iceberg+sql://{tmp_path / 'catalog.db'}?warehouse=file://{tmp_path / 'wh'}"
    no_wait = Retry(attempts=2, deadline_s=1.0, base_delay_s=0.0, max_delay_s=0.0)
    with iceberg_parts_store(lake_dsn, name="lake") as lake:
        dat = Rhizodat(
            stores={"books": pg, "lake": lake},
            journal=PostgresJournal(pg_dsn, schema=pg_schema, wall_ms=wall),
            clock=HybridLogicalClock(wall_ms=wall),
            finish=no_wait,
            sleep=lambda s: None,
        )
        yield Office(dat, lake, wall)
        dat.close()


def new_invoice(faker: Faker) -> Invoice:
    return Invoice(
        number=faker.bothify("INV-#####"),
        customer=faker.company(),
        status="draft",
        amount=Decimal(faker.pydecimal(left_digits=4, right_digits=2, positive=True)),
        currency=faker.currency_code(),
        booked_at=None,
        lines=[faker.bs() for _ in range(2)],
    )


def test_an_invoice_is_drafted_and_booked_across_both_stores(office: Office, faker: Faker) -> None:
    invoices = office.dat.repository(Invoice)
    invoice = new_invoice(faker)
    invoices.add(invoice)
    drafted = office.wall.now()

    office.wall.ms += 60_000
    invoice.status, invoice.booked_at = "booked", office.wall.now()
    invoice.lines = [*invoice.lines, faker.bs()]
    invoices.update(invoice)

    current = invoices.get(invoice.dat_id)
    assert current == invoice
    assert current is not None
    assert (current.dat_created_at, current.dat_updated_at) == (drafted, office.wall.now())
    at_draft = invoices.get(invoice.dat_id, as_of=drafted)
    assert at_draft is not None
    assert (at_draft.status, at_draft.booked_at, len(at_draft.lines)) == ("draft", None, 2)
    assert [i.status for i in invoices.history(invoice.dat_id)] == ["draft", "booked"]
    assert office.lake_rows("erp_invoice", invoice.dat_id) == 2


def test_a_quote_is_erased_from_both_stores(office: Office, faker: Faker) -> None:
    quotes = office.dat.repository(Quote)
    quote = Quote(number=faker.bothify("Q-####"), amount=Decimal("120.00"))
    other = Quote(number=faker.bothify("Q-####"), amount=Decimal("80.50"))
    quotes.add(quote)
    quotes.add(other)
    quote.amount += 1
    quotes.update(quote)

    quotes.delete(quote.dat_id)

    assert quotes.get(quote.dat_id) is None
    assert quotes.history(quote.dat_id) == []
    assert office.lake_rows("erp_quote", quote.dat_id) == 0
    assert quotes.get(other.dat_id) == other
    with pytest.raises(NotRestorable):
        quotes.restore(quote.dat_id)


def test_a_voided_invoice_is_restorable_for_its_window_then_purged(
    office: Office, faker: Faker
) -> None:
    invoices = office.dat.repository(Invoice)
    duplicate, mistake = new_invoice(faker), new_invoice(faker)
    invoices.add(duplicate)
    invoices.add(mistake)
    invoices.delete(duplicate.dat_id)
    invoices.delete(mistake.dat_id)

    office.wall.ms += DAY_MS
    assert invoices.restore(mistake.dat_id).value == mistake
    assert invoices.get(duplicate.dat_id) is None

    office.wall.ms += 29 * DAY_MS  # the type's own window (30 days)
    assert invoices.purge_expired() == 0
    office.wall.ms += 1
    assert invoices.purge_expired() == 1
    assert invoices.get(duplicate.dat_id, include_deleted=True) is None
    assert office.lake_rows("erp_invoice", duplicate.dat_id) == 0
    assert invoices.get(mistake.dat_id) == mistake


def test_figures_left_behind_by_a_crash_are_rolled_forward(office: Office, faker: Faker) -> None:
    invoices = office.dat.repository(Invoice)
    invoice = new_invoice(faker)
    invoices.add(invoice)
    participant = office.lake.participant()
    original = participant.finish

    def die(*a: object, **k: object) -> None:
        raise Crash()

    participant.finish = die  # type: ignore[method-assign]
    office.wall.ms += 60_000
    invoice.status, invoice.amount = "booked", invoice.amount * 2
    with pytest.raises(Crash):
        invoices.update(invoice)
    participant.finish = original  # type: ignore[method-assign]

    # PostgreSQL has the new version, the lake not yet: the read falls back to the
    # last version both stores agree on.
    seen = invoices.get(invoice.dat_id)
    assert seen is not None
    assert (seen.status, seen.amount) == ("draft", invoice.amount / 2)

    office.recover()
    booked = invoices.get(invoice.dat_id)
    assert booked is not None
    assert (booked.status, booked.amount) == ("booked", invoice.amount)
    assert office.lake_rows("erp_invoice", invoice.dat_id) == 2


def test_a_catalog_down_after_the_decision_is_pending(office: Office, faker: Faker) -> None:
    invoices = office.dat.repository(Invoice)
    participant = office.lake.participant()
    original = participant.finish

    def unavailable(*a: object, **k: object) -> None:
        raise ConnectionError("catalog unreachable")

    participant.finish = unavailable  # type: ignore[method-assign]
    invoice = new_invoice(faker)
    outcome = invoices.add(invoice)
    participant.finish = original  # type: ignore[method-assign]

    assert isinstance(outcome, CommittedPending)
    assert invoices.get(invoice.dat_id) is None  # not every part is there yet
    office.recover()
    assert invoices.get(invoice.dat_id) == invoice
