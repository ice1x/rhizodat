"""Scenario: a CBT journal over PostgreSQL, ClickHouse and drevo, on either facade.

A thought record is one entity in three stores: the text in PostgreSQL, the mood
ratings in ClickHouse (analytics), the cognitive distortions it shows as edges in
the drevo graph, where the distortion catalogue lives. Every write is one
meta-transaction over the three. The scenario runs end to end the way an
application would: migrate the schema, write a week of entries, reframe one,
review what the journal looked like before the reframe, aggregate the mood lift
natively in ClickHouse, find the recurring distortions natively in the graph,
and withdraw an entry (soft delete) and bring it back.

The same scenario runs on the Python facade and on the native facade (the core
in Rust under the same API), so both must agree.
"""

# @entity adds dat_id and the service times at runtime; mypy cannot see them.
# mypy: disable-error-code="attr-defined,call-arg"

import contextlib
import time
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime

import psycopg
import pytest
from faker import Faker

from rhizodat import Ref, Rhizodat, entity
from rhizodat.adapters.drevo import ANCHOR, DrevoStore
from rhizodat.core.parts import Part
from rhizodat.migrations import Migration, Step


@entity(stores={"graph": ["name", "definition"]}, name="cbt.Distortion")
class Distortion:
    name: str
    definition: str


@entity(
    stores={
        "journal": ["situation", "automatic_thought", "reframe"],
        "moods": ["mood_before", "mood_after"],
        "graph": ["distortions"],
    },
    name="cbt.Entry",
    soft_delete=True,
)
class Entry:
    situation: str
    automatic_thought: str
    reframe: str | None
    mood_before: int  # 0..10
    mood_after: int  # 0 until reframed
    distortions: list[Ref["Distortion"]]


ENTRY_GRAPH = Part.of(Entry, "graph").table
DISTORTION_GRAPH = Part.of(Distortion, "graph").table
MOODS = Part.of(Entry, "moods").table

SCHEMA = [
    Migration.of(
        1,
        "prompts",
        [
            Step(
                "journal",
                "sql",
                "CREATE TABLE cbt_prompt (position int PRIMARY KEY, text text NOT NULL)",
            ),
            Step(
                "journal",
                "sql",
                "INSERT INTO cbt_prompt VALUES (1, 'What happened?'),"
                " (2, 'What went through your mind?'), (3, 'How do you feel, 0-10?')",
            ),
            Step(
                "moods",
                "sql",
                "CREATE TABLE cbt_checkin (day Date, mood UInt8) ENGINE = MergeTree ORDER BY day",
            ),
        ],
        description="the journal's prompts and the daily check-in table",
    )
]

CATALOGUE = {
    "catastrophizing": "expecting the worst outcome",
    "mind reading": "assuming what others think",
    "all-or-nothing": "seeing only extremes",
}


@pytest.fixture
def journal_schema(pg_dsn: str) -> Iterator[str]:
    name = f"dat_journal_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{name}"')
    yield name
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA "{name}" CASCADE')


@pytest.fixture(autouse=True)
def _clean_graph(drevo_bolt_dsn: str) -> Iterator[None]:
    """The graph is shared: remove this run's versions and the anchors left bare."""
    yield
    cleaner = DrevoStore(drevo_bolt_dsn, name="cleaner")
    try:
        with contextlib.suppress(Exception):
            for label in (ENTRY_GRAPH, DISTORTION_GRAPH):
                cleaner.execute(f"MATCH (v:{label}) DETACH DELETE v")
            cleaner.execute(f"MATCH (a:{ANCHOR}) WHERE NOT (a)<--() DELETE a")
    finally:
        cleaner.close()


@pytest.fixture(params=["python", "native"])
def dat(
    request: pytest.FixtureRequest,
    pg_dsn: str,
    pg_schema: str,
    journal_schema: str,
    clickhouse_dsn: str,
    drevo_bolt_dsn: str,
) -> Iterator[Rhizodat]:
    if request.param == "python":
        from rhizodat.adapters.clickhouse import ClickHouseStore
        from rhizodat.adapters.postgres import PostgresJournal, PostgresStore

        moods = ClickHouseStore(clickhouse_dsn, name="moods")
        moods.install()
        d = Rhizodat(
            stores={
                "journal": PostgresStore(pg_dsn, name="journal", schema=pg_schema),
                "moods": moods,
                "graph": DrevoStore(drevo_bolt_dsn, name="graph"),
            },
            journal=PostgresJournal(pg_dsn, schema=journal_schema),
        )
    else:
        native = pytest.importorskip("rhizodat_native")
        d = Rhizodat(
            stores={
                "journal": native.PostgresStore(pg_dsn, "journal", pg_schema),
                "moods": native.ClickHouseStore(clickhouse_dsn, "moods"),
                "graph": native.DrevoStore(drevo_bolt_dsn, "graph"),
            },
            journal=native.PostgresJournal(pg_dsn, journal_schema),
        )
    d.migrate(SCHEMA)
    yield d
    d.close()


def catalogue(dat: Rhizodat) -> dict[str, Distortion]:
    distortions = dat.repository(Distortion)
    known = {}
    for name, definition in CATALOGUE.items():
        known[name] = Distortion(name=name, definition=definition)
        distortions.add(known[name])
    return known


def a_week(dat: Rhizodat, faker: Faker, known: dict[str, Distortion]) -> list[Entry]:
    """Seven entries; catastrophizing shows up in five, the others in one each."""
    entries = dat.repository(Entry)
    shows = [
        ["catastrophizing"],
        ["catastrophizing", "mind reading"],
        ["catastrophizing"],
        ["all-or-nothing"],
        ["catastrophizing"],
        [],
        ["catastrophizing"],
    ]
    week = []
    for day in shows:
        entry = Entry(
            situation=faker.sentence(),
            automatic_thought=faker.sentence(),
            reframe=None,
            mood_before=faker.random_int(2, 5),
            mood_after=0,
            distortions=[Ref.to(known[d]) for d in day],
        )
        entries.add(entry)
        week.append(entry)
    return week


def test_the_schema_is_migrated_once_and_the_history_says_so(dat: Rhizodat) -> None:
    assert [m.id for m in dat.schema_versions()] == [1]
    report = dat.migrate(SCHEMA)
    assert (report.applied, report.already) == ((), (1,))


def test_an_entry_is_written_across_the_three_stores_and_read_back(
    dat: Rhizodat, faker: Faker
) -> None:
    known = catalogue(dat)
    entries = dat.repository(Entry)
    entry = Entry(
        situation=faker.sentence(),
        automatic_thought=faker.sentence(),
        reframe=None,
        mood_before=3,
        mood_after=0,
        distortions=[Ref.to(known["catastrophizing"]), Ref.to(known["mind reading"])],
    )
    entries.add(entry)

    read = entries.get(entry.dat_id)
    assert read == entry
    assert read is not None
    assert [r.dat_id for r in read.distortions] == [
        known["catastrophizing"].dat_id,
        known["mind reading"].dat_id,
    ]
    assert dat.repository(Distortion).get(known["mind reading"].dat_id) == known["mind reading"]


def test_a_reframe_is_a_new_version_and_the_review_sees_the_journal_as_it_was(
    dat: Rhizodat, faker: Faker
) -> None:
    known = catalogue(dat)
    entries = dat.repository(Entry)
    entry = a_week(dat, faker, known)[1]
    before_session = datetime.now(UTC)
    time.sleep(0.01)

    entry.reframe = faker.sentence()
    entry.mood_after = 7
    entries.update(entry)

    now = entries.get(entry.dat_id)
    assert now is not None
    assert (now.reframe, now.mood_after) == (entry.reframe, 7)
    as_the_therapist_saw_it = entries.get(entry.dat_id, as_of=before_session)
    assert as_the_therapist_saw_it is not None
    assert (as_the_therapist_saw_it.reframe, as_the_therapist_saw_it.mood_after) == (None, 0)
    assert [v.mood_after for v in entries.history(entry.dat_id)] == [0, 7]


def test_the_mood_lift_is_aggregated_natively_in_clickhouse(dat: Rhizodat, faker: Faker) -> None:
    known = catalogue(dat)
    entries = dat.repository(Entry)
    week = a_week(dat, faker, known)
    for entry, lift in zip(week[:3], (4, 2, 3), strict=True):
        entry.reframe = faker.sentence()
        entry.mood_after = entry.mood_before + lift
        entries.update(entry)

    # The latest version of each entry (ClickHouse keeps every version), reframed ones only.
    row = (
        dat.store("moods")
        .execute(
            "SELECT avg(lift) AS lift_avg, count() AS n FROM ("
            "  SELECT argMax(mood_after - mood_before, dat_tx_time) AS lift,"
            "         argMax(mood_after, dat_tx_time) AS after"
            f"  FROM {MOODS} GROUP BY dat_id) WHERE after > 0"
        )
        .one()
    )
    assert (float(row["lift_avg"]), int(row["n"])) == (3.0, 3)


def test_the_recurring_distortion_is_found_natively_in_the_graph(
    dat: Rhizodat, faker: Faker
) -> None:
    known = catalogue(dat)
    a_week(dat, faker, known)

    rows = (
        dat.store("graph")
        .execute(
            f"MATCH (v:{ENTRY_GRAPH})-[:distortions]->(a:{ANCHOR})"
            " RETURN a.dat_id AS d, count(DISTINCT v.dat_id) AS n ORDER BY n DESC, d"
        )
        .all()
    )
    by_name = {name: str(d.dat_id) for name, d in known.items()}
    assert [(r["d"], int(r["n"])) for r in rows] == [
        (by_name["catastrophizing"], 5),
        *sorted([(by_name["all-or-nothing"], 1), (by_name["mind reading"], 1)]),
    ]


def test_a_withdrawn_entry_is_gone_from_reads_and_comes_back_on_restore(
    dat: Rhizodat, faker: Faker
) -> None:
    known = catalogue(dat)
    entries = dat.repository(Entry)
    entry = a_week(dat, faker, known)[5]

    entries.delete(entry.dat_id)
    assert entries.get(entry.dat_id) is None
    kept = entries.get(entry.dat_id, include_deleted=True)
    assert kept is not None
    assert kept.dat_deleted_at is not None

    entries.restore(entry.dat_id)
    back = entries.get(entry.dat_id)
    assert back is not None
    assert (back.situation, back.dat_deleted_at) == (entry.situation, None)
    assert len(entries.history(entry.dat_id)) == 3  # written, withdrawn, restored


def test_both_facades_read_each_others_journal(
    pg_dsn: str,
    pg_schema: str,
    journal_schema: str,
    clickhouse_dsn: str,
    drevo_bolt_dsn: str,
    faker: Faker,
) -> None:
    """An entry written on the Python facade is read, reframed and queried on the native one."""
    native = pytest.importorskip("rhizodat_native")
    from rhizodat.adapters.clickhouse import ClickHouseStore
    from rhizodat.adapters.postgres import PostgresJournal, PostgresStore

    moods = ClickHouseStore(clickhouse_dsn, name="moods")
    moods.install()
    facades: list[Callable[[], Rhizodat]] = [
        lambda: Rhizodat(
            stores={
                "journal": PostgresStore(pg_dsn, name="journal", schema=pg_schema),
                "moods": moods,
                "graph": DrevoStore(drevo_bolt_dsn, name="graph"),
            },
            journal=PostgresJournal(pg_dsn, schema=journal_schema),
        ),
        lambda: Rhizodat(
            stores={
                "journal": native.PostgresStore(pg_dsn, "journal", pg_schema),
                "moods": native.ClickHouseStore(clickhouse_dsn, "moods"),
                "graph": native.DrevoStore(drevo_bolt_dsn, "graph"),
            },
            journal=native.PostgresJournal(pg_dsn, journal_schema),
        ),
    ]
    with facades[0]() as python_side:
        python_side.migrate(SCHEMA)
        known = catalogue(python_side)
        entry = a_week(python_side, faker, known)[0]
    with facades[1]() as native_side:
        assert [m.id for m in native_side.schema_versions()] == [1]
        entries = native_side.repository(Entry)
        read = entries.get(entry.dat_id)
        assert read == entry
        assert read is not None
        read.reframe, read.mood_after = faker.sentence(), 8
        entries.update(read)
    with facades[0]() as python_side:
        versions = python_side.repository(Entry).history(entry.dat_id)
        assert [v.mood_after for v in versions] == [0, 8]
