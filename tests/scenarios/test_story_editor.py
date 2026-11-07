"""Scenario: a story editor over PostgreSQL, the drevo graph and an Iceberg lake.

A chapter is one entity in three stores: its text in PostgreSQL, its cast (point of
view, who appears) as edges in the drevo graph, where the characters live, and its
figures (word count, revision) in an Iceberg lake. The scenario runs end to end
the way an application would: migrate the schema, draft chapters with their
cast, revise a chapter as a new version (an interrupted revision leaves the
draft), read the first draft as of its moment, ask the graph where a character
appears and confirm against the latest versions, scan the lake for the long
chapters with a row filter, cut a chapter and bring it back, and work the same
manuscript from both facades in turn.

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

from rhizodat import Ref, Rhizodat, UnitOfWork, entity
from rhizodat.adapters.drevo import ANCHOR, DrevoStore
from rhizodat.adapters.iceberg import IcebergStore
from rhizodat.core.parts import Part
from rhizodat.migrations import Migration, Step


@entity(stores={"graph": ["name", "role", "mentor"]}, name="story.Character")
class Character:
    name: str
    role: str  # hero | mentor | foil
    mentor: Ref["Character"] | None


@entity(
    stores={
        "text": ["heading", "body", "position"],
        "graph": ["pov", "appears"],
        "lake": ["words", "revision"],
    },
    name="story.Chapter",
    soft_delete=True,
)
class Chapter:
    heading: str
    body: str
    position: int
    pov: Ref["Character"] | None
    appears: list[Ref["Character"]]
    words: int
    revision: int


CHAPTER_GRAPH = Part.of(Chapter, "graph").table
CHARACTER_GRAPH = Part.of(Character, "graph").table
CHAPTERS_LAKE = Part.of(Chapter, "lake").table

SCHEMA = [
    Migration.of(
        1,
        "style",
        [
            Step("text", "sql", "CREATE TABLE story_style (rule text PRIMARY KEY)"),
            Step("text", "sql", "INSERT INTO story_style VALUES ('one point of view per chapter')"),
        ],
        description="the style sheet",
    )
]


@pytest.fixture
def journal_schema(pg_dsn: str) -> Iterator[str]:
    name = f"dat_journal_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{name}"')
    yield name
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA "{name}" CASCADE')


@pytest.fixture
def parts_namespace(iceberg_rest_dsn: str) -> Iterator[str]:
    """The lake outlives the test: a parts namespace of its own, purged afterwards."""
    name = f"dat_story_{uuid.uuid4().hex[:10]}"
    yield name
    cleaner = IcebergStore(iceberg_rest_dsn, name="cleaner")
    try:
        with contextlib.suppress(Exception):
            for ident in cleaner.catalog.list_tables(name):
                cleaner.catalog.purge_table(ident)
            cleaner.catalog.drop_namespace(name)
    finally:
        cleaner.close()


@pytest.fixture(autouse=True)
def _clean_graph(drevo_bolt_dsn: str) -> Iterator[None]:
    """The graph is shared: remove this run's versions and the anchors left bare."""
    yield
    cleaner = DrevoStore(drevo_bolt_dsn, name="cleaner")
    try:
        with contextlib.suppress(Exception):
            for label in (CHAPTER_GRAPH, CHARACTER_GRAPH):
                cleaner.execute(f"MATCH (v:{label}) DETACH DELETE v")
            cleaner.execute(f"MATCH (a:{ANCHOR}) WHERE NOT (a)<--() DELETE a")
    finally:
        cleaner.close()


def python_facade(
    pg_dsn: str, pg_schema: str, journal_schema: str, bolt: str, lake_dsn: str, namespace: str
) -> Rhizodat:
    from rhizodat.adapters.postgres import PostgresJournal, PostgresStore

    return Rhizodat(
        stores={
            "text": PostgresStore(pg_dsn, name="text", schema=pg_schema),
            "graph": DrevoStore(bolt, name="graph"),
            "lake": IcebergStore(lake_dsn, name="lake", parts_namespace=namespace),
        },
        journal=PostgresJournal(pg_dsn, schema=journal_schema),
    )


def native_facade(
    pg_dsn: str, pg_schema: str, journal_schema: str, bolt: str, lake_dsn: str, namespace: str
) -> Rhizodat:
    native = pytest.importorskip("rhizodat_native")
    return Rhizodat(
        stores={
            "text": native.PostgresStore(pg_dsn, "text", pg_schema),
            "graph": native.DrevoStore(bolt, "graph"),
            "lake": native.IcebergStore(lake_dsn, "lake", parts_namespace=namespace),
        },
        journal=native.PostgresJournal(pg_dsn, journal_schema),
    )


FACADES: dict[str, Callable[..., Rhizodat]] = {"python": python_facade, "native": native_facade}


@pytest.fixture(params=list(FACADES))
def dat(
    request: pytest.FixtureRequest,
    pg_dsn: str,
    pg_schema: str,
    journal_schema: str,
    drevo_bolt_dsn: str,
    iceberg_rest_dsn: str,
    parts_namespace: str,
) -> Iterator[Rhizodat]:
    d = FACADES[request.param](
        pg_dsn, pg_schema, journal_schema, drevo_bolt_dsn, iceberg_rest_dsn, parts_namespace
    )
    d.migrate(SCHEMA)
    yield d
    d.close()


def cast(dat: Rhizodat, faker: Faker) -> dict[str, Character]:
    characters = dat.repository(Character)
    mentor = Character(name=faker.name(), role="mentor", mentor=None)
    characters.add(mentor)
    hero = Character(name=faker.name(), role="hero", mentor=Ref.to(mentor))
    characters.add(hero)
    foil = Character(name=faker.name(), role="foil", mentor=None)
    characters.add(foil)
    return {"mentor": mentor, "hero": hero, "foil": foil}


def draft(dat: Rhizodat, faker: Faker, who: dict[str, Character]) -> list[Chapter]:
    """Three chapters: the hero alone, the hero with the mentor, the foil's chapter."""
    chapters = dat.repository(Chapter)
    plan = [
        ("hero", ["hero"], 300),
        ("hero", ["hero", "mentor"], 900),
        ("foil", ["foil"], 1200),
    ]
    drafted = []
    for position, (pov, appears, words) in enumerate(plan, start=1):
        chapter = Chapter(
            heading=faker.catch_phrase(),
            body=faker.paragraph(nb_sentences=5),
            position=position,
            pov=Ref.to(who[pov]),
            appears=[Ref.to(who[c]) for c in appears],
            words=words,
            revision=1,
        )
        chapters.add(chapter)
        drafted.append(chapter)
    return drafted


def appears_in(dat: Rhizodat, character: Character) -> set[uuid.UUID]:
    """The graph names every chapter version the character appeared in; the latest
    version of each chapter confirms (a cut chapter does not)."""
    rows = (
        dat.store("graph")
        .execute(
            f"MATCH (v:{CHAPTER_GRAPH})-[:appears]->(a:{ANCHOR} {{dat_id: $c}})"
            " RETURN DISTINCT v.dat_id AS d",
            {"c": str(character.dat_id)},
        )
        .all()
    )
    chapters = dat.repository(Chapter)
    confirmed = set()
    for row in rows:
        latest = chapters.get(uuid.UUID(str(row["d"])))
        if latest and any(r.dat_id == character.dat_id for r in latest.appears):
            confirmed.add(latest.dat_id)
    return confirmed


def long_chapters(dat: Rhizodat, namespace: str, *, at_least: int) -> dict[uuid.UUID, int]:
    """Native lake scan: the latest version of each chapter with that many words."""
    rows = dat.store("lake").execute(
        f"{namespace}.{CHAPTERS_LAKE}", {"selected_fields": ["dat_id", "words", "dat_tx_time"]}
    )
    latest: dict[uuid.UUID, tuple[int, int]] = {}
    for r in rows.all():
        dat_id = uuid.UUID(str(r["dat_id"]))
        if dat_id not in latest or latest[dat_id][0] < int(r["dat_tx_time"]):
            latest[dat_id] = (int(r["dat_tx_time"]), int(r["words"]))
    return {d: words for d, (_, words) in latest.items() if words >= at_least}


def test_the_schema_is_migrated_once_and_the_history_says_so(dat: Rhizodat) -> None:
    assert [m.id for m in dat.schema_versions()] == [1]
    report = dat.migrate(SCHEMA)
    assert (report.applied, report.already) == ((), (1,))


def test_chapters_are_drafted_across_the_three_stores_and_read_back(
    dat: Rhizodat, faker: Faker
) -> None:
    who = cast(dat, faker)
    drafted = draft(dat, faker, who)
    chapters = dat.repository(Chapter)
    second = chapters.get(drafted[1].dat_id)
    assert second == drafted[1]
    assert second is not None
    assert second.pov is not None
    assert second.pov.dat_id == who["hero"].dat_id
    assert [r.dat_id for r in second.appears] == [who["hero"].dat_id, who["mentor"].dat_id]
    hero = dat.repository(Character).get(who["hero"].dat_id)
    assert hero is not None
    assert hero.mentor is not None
    assert hero.mentor.dat_id == who["mentor"].dat_id


def test_a_revision_is_a_new_version_and_the_first_draft_stays_readable(
    dat: Rhizodat, parts_namespace: str, faker: Faker
) -> None:
    who = cast(dat, faker)
    drafted = draft(dat, faker, who)
    chapters = dat.repository(Chapter)
    first = drafted[0]
    drafted_at = datetime.now(UTC)
    time.sleep(0.01)

    def revise(*, fail: bool) -> None:
        @dat.transactional
        def work(uow: UnitOfWork) -> None:
            first.body, first.words, first.revision = faker.paragraph(nb_sentences=12), 1500, 2
            first.appears = [Ref.to(who["hero"]), Ref.to(who["foil"])]
            uow.repository(Chapter).update(first)
            if fail:
                raise RuntimeError("the editor crashed")

        work()

    with pytest.raises(RuntimeError, match="crashed"):
        revise(fail=True)
    still_draft = chapters.get(first.dat_id)
    assert still_draft is not None
    assert (still_draft.revision, still_draft.words) == (1, 300)

    revise(fail=False)
    now = chapters.get(first.dat_id)
    assert now is not None
    assert (now.revision, now.words, len(now.appears)) == (2, 1500, 2)
    as_drafted = chapters.get(first.dat_id, as_of=drafted_at)
    assert as_drafted is not None
    assert (as_drafted.revision, as_drafted.words, len(as_drafted.appears)) == (1, 300, 1)
    assert [c.revision for c in chapters.history(first.dat_id)] == [1, 2]
    assert appears_in(dat, who["foil"]) == {first.dat_id, drafted[2].dat_id}
    assert long_chapters(dat, parts_namespace, at_least=1000) == {
        first.dat_id: 1500,
        drafted[2].dat_id: 1200,
    }


def test_a_cut_chapter_leaves_the_cast_and_the_lake_and_comes_back(
    dat: Rhizodat, parts_namespace: str, faker: Faker
) -> None:
    who = cast(dat, faker)
    drafted = draft(dat, faker, who)
    chapters = dat.repository(Chapter)
    assert appears_in(dat, who["mentor"]) == {drafted[1].dat_id}

    chapters.delete(drafted[1].dat_id)
    assert chapters.get(drafted[1].dat_id) is None
    assert appears_in(dat, who["mentor"]) == set()
    assert appears_in(dat, who["hero"]) == {drafted[0].dat_id}

    chapters.restore(drafted[1].dat_id)
    assert appears_in(dat, who["mentor"]) == {drafted[1].dat_id}
    assert long_chapters(dat, parts_namespace, at_least=900) == {
        drafted[1].dat_id: 900,
        drafted[2].dat_id: 1200,
    }
    assert len(chapters.history(drafted[1].dat_id)) == 3  # drafted, cut, restored


def test_both_facades_edit_the_same_manuscript(
    pg_dsn: str,
    pg_schema: str,
    journal_schema: str,
    drevo_bolt_dsn: str,
    iceberg_rest_dsn: str,
    parts_namespace: str,
    faker: Faker,
) -> None:
    """Drafted on the Python facade, revised natively, read back in Python."""
    args = (pg_dsn, pg_schema, journal_schema, drevo_bolt_dsn, iceberg_rest_dsn, parts_namespace)
    with python_facade(*args) as python_side:
        python_side.migrate(SCHEMA)
        who = cast(python_side, faker)
        drafted = draft(python_side, faker, who)
    with native_facade(*args) as native_side:
        assert [m.id for m in native_side.schema_versions()] == [1]
        chapters = native_side.repository(Chapter)
        third = chapters.get(drafted[2].dat_id)
        assert third == drafted[2]
        assert third is not None
        third.appears, third.words, third.revision = [*third.appears, Ref.to(who["hero"])], 1400, 2
        chapters.update(third)
        assert appears_in(native_side, who["hero"]) == {c.dat_id for c in drafted}
    with python_facade(*args) as python_side:
        assert appears_in(python_side, who["hero"]) == {c.dat_id for c in drafted}
        assert long_chapters(python_side, parts_namespace, at_least=1000) == {
            drafted[2].dat_id: 1400
        }
        assert [c.revision for c in python_side.repository(Chapter).history(third.dat_id)] == [
            1,
            2,
        ]
