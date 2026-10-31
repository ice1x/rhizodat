"""Entity parts and repositories on the native core, over drevo (and PostgreSQL).

``rhizodat_native.DrevoPartSpec`` names a drevo store's share of an entity type;
a native ``DrevoStore`` reads committed versions and a ``DrevoUnit`` buffers part
writes that its commit runs inside the meta-transaction. A native facade's
repositories then hold a type in drevo alone, or split across PostgreSQL and
drevo under a PostgreSQL journal.

The nodes are the Python adapter's (same properties, ``dat_title``, reference
anchors), so either facade reads the other's versions. Types carry a tag unique
to the run: the compose drevo is shared, so their labels never meet another
test's. Install the module with ``uv sync --group native`` (needs a Rust
toolchain).
"""

# @entity adds dat_id and the service times at runtime; mypy cannot see them.
# mypy: disable-error-code="attr-defined,union-attr"

import contextlib
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import psycopg
import pytest
from faker import Faker

from rhizodat import Ref, Rhizodat, UnitOfWork, entity
from rhizodat.adapters.drevo import ANCHOR
from rhizodat.adapters.drevo import DrevoStore as PyDrevoStore
from rhizodat.adapters.postgres import PostgresJournal as PyPostgresJournal
from rhizodat.adapters.postgres import PostgresStore as PyPostgresStore
from rhizodat.core.parts import Part
from rhizodat.core.repository import EntityExists

native = pytest.importorskip("rhizodat_native")

TAG = f"n{uuid.uuid4().hex[:8]}"


@entity(stores={"graph": ["name", "title", "score", "mentor"]}, name=f"{TAG}.Character")
class Character:
    name: str
    title: str | None  # drevo keeps it as dat_title
    score: float
    mentor: Ref["Character"] | None


@entity(
    stores={"pg": ["heading", "position"], "graph": ["pov", "appears", "tags"]},
    name=f"{TAG}.Chapter",
    soft_delete=True,
)
class Chapter:
    heading: str
    position: int
    pov: Ref["Character"] | None
    appears: list[Ref["Character"]]
    tags: list[str]


LABELS = (Part.of(Character, "graph").table, Part.of(Chapter, "graph").table)


@pytest.fixture(autouse=True)
def _clean(drevo_bolt_dsn: str) -> Iterator[None]:
    """Remove this run's versions and the anchors left without them."""
    yield
    store = PyDrevoStore(drevo_bolt_dsn, name="cleaner")
    try:
        with contextlib.suppress(Exception):
            for label in LABELS:
                store.execute(f"MATCH (v:{label}) DETACH DELETE v")
            store.execute(f"MATCH (a:{ANCHOR}) WHERE NOT (a)<--() DELETE a")
    finally:
        store.close()


@pytest.fixture
def journal_schema(pg_dsn: str) -> Iterator[str]:
    name = f"dat_journal_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{name}"')
    yield name
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA "{name}" CASCADE')


@pytest.fixture
def graph_only(drevo_bolt_dsn: str) -> Iterator[Rhizodat]:
    d = Rhizodat(stores={"graph": native.DrevoStore(drevo_bolt_dsn, "graph")})
    yield d
    d.close()


@pytest.fixture
def book(
    pg_dsn: str, pg_schema: str, journal_schema: str, drevo_bolt_dsn: str
) -> Iterator[Rhizodat]:
    d = Rhizodat(
        stores={
            "pg": native.PostgresStore(pg_dsn, "pg", pg_schema),
            "graph": native.DrevoStore(drevo_bolt_dsn, "graph"),
        },
        journal=native.PostgresJournal(pg_dsn, journal_schema),
    )
    yield d
    d.close()


@pytest.fixture
def python_book(
    pg_dsn: str, pg_schema: str, journal_schema: str, drevo_bolt_dsn: str
) -> Iterator[Rhizodat]:
    """The Python facade over the same schema and graph."""
    d = Rhizodat(
        stores={
            "pg": PyPostgresStore(pg_dsn, name="pg", schema=pg_schema),
            "graph": PyDrevoStore(drevo_bolt_dsn, name="graph"),
        },
        journal=PyPostgresJournal(pg_dsn, schema=journal_schema),
    )
    yield d
    d.close()


def tick() -> datetime:
    """A moment strictly between two commits (HLC times are milliseconds)."""
    time.sleep(0.005)
    at = datetime.now(UTC)
    time.sleep(0.005)
    return at


def character(faker: Faker, mentor: Character | None = None) -> Character:
    return Character(
        name=faker.name(),
        title=faker.prefix(),
        score=faker.pyfloat(min_value=0, max_value=10),
        mentor=Ref(mentor.dat_id) if mentor is not None else None,
    )


def graph_count(dsn: str, cypher: str, params: dict[str, Any]) -> int:
    store = PyDrevoStore(dsn, name="reader")
    try:
        return int(store.execute(cypher, params).one()["n"])
    finally:
        store.close()


# --- Docker-free ------------------------------------------------------------------------


def test_a_drevo_spec_takes_plain_names_and_known_kinds() -> None:
    with pytest.raises(native.InvalidUnitOfWork, match="label"):
        native.DrevoPartSpec("t.T", "bad label", [])
    with pytest.raises(native.InvalidUnitOfWork, match="field kind"):
        native.DrevoPartSpec("t.T", "L", [("a", "map", False)])
    with pytest.raises(native.InvalidUnitOfWork, match="service property"):
        native.DrevoPartSpec("t.T", "L", [("dat_id", "uuid", False)])
    from rhizodat.core.native_parts import part_spec

    store = native.DrevoStore("drevo+bolt://127.0.0.1:1", "graph")
    spec = part_spec(native, Part.of(Character, "graph"), store)
    assert (spec.label, spec.fields) == (LABELS[0], ["name", "title", "score", "mentor"])


def test_a_drevo_units_own_writes_read_back(faker: Faker) -> None:
    from rhizodat.core.native_parts import part_spec

    store = native.DrevoStore("drevo+bolt://127.0.0.1:1", "graph")
    unit = store.unit()
    spec = part_spec(native, Part.of(Chapter, "graph"), store)
    dat_id, pov = uuid.uuid4(), uuid.uuid4()
    values = {"pov": pov, "appears": [pov], "tags": faker.words(2)}
    unit.write_part(spec, dat_id, values, created=7)
    assert unit.own_part(spec, dat_id) == (
        True,
        {"created": 7, "deleted": False, "values": values},
    )
    with pytest.raises(native.InvalidUnitOfWork, match="are not fields"):
        unit.write_part(spec, dat_id, {"heading": "pg's"}, created=None)
    unit.erase_part(spec, dat_id)
    assert unit.own_part(spec, dat_id) == (True, None)


# --- against drevo (and PostgreSQL) ------------------------------------------------------


def test_a_graph_only_type_lives_in_drevo_with_its_history(
    graph_only: Rhizodat, faker: Faker, drevo_bolt_dsn: str
) -> None:
    characters = graph_only.repository(Character)
    mentor = character(faker)
    characters.add(mentor)
    pupil = character(faker, mentor)
    characters.add(pupil)
    first_name = pupil.name
    with pytest.raises(EntityExists):
        characters.add(pupil)
    between = tick()
    pupil.name = faker.name()
    characters.update(pupil)

    stored = characters.get(pupil.dat_id)
    assert (stored.name, stored.title, stored.score) == (pupil.name, pupil.title, pupil.score)
    assert stored.mentor == Ref(mentor.dat_id)
    assert characters.get(pupil.dat_id, known_at=between).name == first_name
    assert [c.name for c in characters.history(pupil.dat_id)] == [first_name, pupil.name]
    # Stored like the Python adapter: the title as dat_title, the mentor as an edge.
    assert (
        graph_count(
            drevo_bolt_dsn,
            f"MATCH (v:{LABELS[0]} {{dat_id: $id}})-[:mentor]->(a:{ANCHOR} {{dat_id: $m}})"
            " WHERE v.dat_title IS NOT NULL RETURN count(v) AS n",
            {"id": str(pupil.dat_id), "m": str(mentor.dat_id)},
        )
        == 2
    )

    characters.delete(pupil.dat_id)  # no soft delete: erased, with its anchors' edges
    assert characters.history(pupil.dat_id) == []
    assert (
        graph_count(
            drevo_bolt_dsn,
            f"MATCH (a:{ANCHOR} {{dat_id: $m}}) RETURN count(a) AS n",
            {"m": str(mentor.dat_id)},
        )
        == 0
    )


def test_a_chapter_splits_across_postgresql_and_drevo(book: Rhizodat, faker: Faker) -> None:
    cast = [character(faker) for _ in range(2)]
    chapter = Chapter(
        heading=faker.sentence(),
        position=1,
        pov=Ref(cast[0].dat_id),
        appears=[Ref(c.dat_id) for c in cast],
        tags=faker.words(3),
    )

    @book.transactional
    def write(uow: UnitOfWork) -> None:
        for c in cast:
            uow.repository(Character).add(c)
        uow.repository(Chapter).add(chapter)

    outcome = write()
    assert outcome.dat_tx_time is not None
    stored = book.repository(Chapter).get(chapter.dat_id)
    assert (stored.heading, stored.position, stored.tags) == (
        chapter.heading,
        1,
        chapter.tags,
    )
    assert stored.appears == chapter.appears
    chapters = book.repository(Chapter)
    chapters.delete(chapter.dat_id)
    assert chapters.get(chapter.dat_id) is None
    assert chapters.get(chapter.dat_id, include_deleted=True) is not None


def test_a_failed_unit_of_work_leaves_neither_store_written(book: Rhizodat, faker: Faker) -> None:
    chapter = Chapter(heading="x", position=1, pov=None, appears=[], tags=[])

    @book.transactional
    def broken(uow: UnitOfWork) -> None:
        uow.repository(Chapter).add(chapter)
        raise RuntimeError("the work fails")

    with pytest.raises(RuntimeError):
        broken()
    assert book.repository(Chapter).history(chapter.dat_id) == []


def test_the_facades_read_each_others_split_versions(
    book: Rhizodat, python_book: Rhizodat, faker: Faker
) -> None:
    hero = character(faker)
    python_book.repository(Character).add(hero)
    written_by_python = Chapter(
        heading=faker.sentence(),
        position=1,
        pov=Ref(hero.dat_id),
        appears=[Ref(hero.dat_id)],
        tags=["draft"],
    )
    python_book.repository(Chapter).add(written_by_python)
    seen = book.repository(Chapter).get(written_by_python.dat_id)
    assert (seen.heading, seen.pov, seen.appears, seen.tags) == (
        written_by_python.heading,
        written_by_python.pov,
        written_by_python.appears,
        ["draft"],
    )
    assert book.repository(Character).get(hero.dat_id).title == hero.title

    written_natively = Chapter(
        heading=faker.sentence(), position=2, pov=None, appears=[Ref(hero.dat_id)], tags=[]
    )
    book.repository(Chapter).add(written_natively)
    seen = python_book.repository(Chapter).get(written_natively.dat_id)
    assert (seen.heading, seen.position, seen.appears, seen.pov) == (
        written_natively.heading,
        2,
        written_natively.appears,
        None,
    )
