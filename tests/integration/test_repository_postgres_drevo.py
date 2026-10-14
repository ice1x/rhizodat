"""Story editor on the Repository: chapter text in PostgreSQL, the cast and who appears
where in a drevo graph.

A chapter's text lives in PostgreSQL and its links (point of view, characters who
appear) in drevo, where references are edges; characters live in drevo only.
Both stores commit with two-phase commit under a PostgreSQL journal.
"""

# @entity adds dat_id and the service times at runtime; mypy cannot see them.
# mypy: disable-error-code="attr-defined"

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pytest
from faker import Faker

from datorium import Datorium, Ref, entity
from datorium.adapters.drevo import ANCHOR, DrevoStore
from datorium.adapters.postgres import PostgresJournal, PostgresStore
from datorium.core.clock import HybridLogicalClock
from datorium.core.parts import Part
from datorium.core.repository import NotRestorable
from drevo_parts import drevo_parts_dsn
from fakes import Crash

DAY_MS = 86_400_000


@entity(stores={"graph": ["name", "title", "mentor"]}, name="story.Character", soft_delete=True)
class Character:
    name: str
    title: str | None  # "Lady", "Captain": drevo keeps it as dat_title
    mentor: Ref["Character"] | None


@entity(
    stores={"text": ["heading", "body", "position"], "graph": ["pov", "appears"]},
    name="story.Chapter",
)
class Chapter:
    heading: str
    body: str
    position: int
    pov: Ref["Character"] | None
    appears: list[Ref["Character"]]


LABELS = (Part.of(Character, "graph").table, Part.of(Chapter, "graph").table)


class Wall:
    def __init__(self) -> None:
        self.ms = int(datetime(2026, 10, 14, 9, 0, tzinfo=UTC).timestamp() * 1000)

    def __call__(self) -> int:
        return self.ms

    def now(self) -> datetime:
        return datetime.fromtimestamp(self.ms / 1000, UTC)


@dataclass
class Book:
    dat: Datorium
    graph: DrevoStore
    wall: Wall

    def recover(self) -> object:
        self.wall.ms += 60_000
        return self.dat.recovery(wall_ms=self.wall).run_once()

    def clear(self) -> None:
        for label in LABELS:
            self.graph.execute(f"MATCH (v:{label}) DETACH DELETE v")
        self.graph.execute(f"MATCH (a:{ANCHOR}) WHERE NOT (a)<--() DELETE a")

    def chapters_with(self, character: Character) -> list[str]:
        """Graph query: the chapters (latest versions) a character appears in."""
        rows = self.graph.execute(
            f"MATCH (v:{LABELS[1]})-[:appears]->(a:{ANCHOR} {{dat_id: $c}}) RETURN v.dat_id AS d",
            {"c": str(character.dat_id)},
        ).all()
        return sorted({r["d"] for r in rows})


@pytest.fixture(params=["file", "bolt"])
def book(
    request: pytest.FixtureRequest, tmp_path: Path, pg_dsn: str, pg_schema: str
) -> Iterator[Book]:
    wall = Wall()
    graph = DrevoStore(drevo_parts_dsn(request, tmp_path), name="graph")
    dat = Datorium(
        stores={"text": PostgresStore(pg_dsn, name="text", schema=pg_schema), "graph": graph},
        journal=PostgresJournal(pg_dsn, schema=pg_schema, wall_ms=wall),
        clock=HybridLogicalClock(wall_ms=wall),
    )
    b = Book(dat, graph, wall)
    b.clear()
    yield b
    b.clear()
    dat.close()


def cast(book: Book, faker: Faker) -> tuple[Character, Character]:
    characters = book.dat.repository(Character)
    mentor = Character(name=faker.name(), title="Captain", mentor=None)
    characters.add(mentor)
    hero = Character(name=faker.name(), title=None, mentor=Ref.to(mentor))
    characters.add(hero)
    return mentor, hero


def test_a_chapter_is_drafted_across_text_and_graph(book: Book, faker: Faker) -> None:
    mentor, hero = cast(book, faker)
    chapters = book.dat.repository(Chapter)
    chapter = Chapter(
        heading=faker.sentence(nb_words=4),
        body=faker.paragraph(),
        position=1,
        pov=Ref.to(hero),
        appears=[Ref.to(hero)],
    )
    chapters.add(chapter)
    drafted = book.wall.now()

    book.wall.ms += 60_000
    chapter.body += "\n\n" + faker.paragraph()
    chapter.appears = [Ref.to(hero), Ref.to(mentor)]
    chapters.update(chapter)

    assert chapters.get(chapter.dat_id) == chapter
    first = chapters.get(chapter.dat_id, as_of=drafted)
    assert first is not None
    assert first.appears == [Ref.to(hero)]
    assert [len(c.appears) for c in chapters.history(chapter.dat_id)] == [1, 2]
    assert book.chapters_with(mentor) == [str(chapter.dat_id)]
    assert book.dat.repository(Character).get(mentor.dat_id) == mentor


def test_a_cut_chapter_is_erased_from_text_and_graph(book: Book, faker: Faker) -> None:
    _, hero = cast(book, faker)
    chapters = book.dat.repository(Chapter)
    chapter = Chapter(
        heading=faker.sentence(), body=faker.paragraph(), position=2, pov=None, appears=[]
    )
    chapters.add(chapter)
    chapter.appears = [Ref.to(hero)]
    chapters.update(chapter)

    chapters.delete(chapter.dat_id)

    assert chapters.get(chapter.dat_id) is None
    assert chapters.history(chapter.dat_id) == []
    assert book.chapters_with(hero) == []
    rows = book.dat.store("text").execute(
        "SELECT count(*) AS n FROM story_chapter WHERE dat_id = %s", (chapter.dat_id,)
    )
    assert rows.one()["n"] == 0
    with pytest.raises(NotRestorable):
        chapters.restore(chapter.dat_id)


def test_a_removed_character_comes_back_within_the_window(book: Book, faker: Faker) -> None:
    mentor, hero = cast(book, faker)
    characters = book.dat.repository(Character)
    characters.delete(mentor.dat_id)
    assert characters.get(mentor.dat_id) is None

    book.wall.ms += DAY_MS
    assert characters.restore(mentor.dat_id).value == mentor
    again = characters.get(hero.dat_id)
    assert again is not None
    assert again.mentor == Ref.to(mentor)

    characters.delete(hero.dat_id)
    book.wall.ms += 31 * DAY_MS  # past the default window
    assert characters.purge_expired() == 1
    assert characters.get(hero.dat_id, include_deleted=True) is None


def test_a_crash_after_the_decision_is_rolled_forward(book: Book, faker: Faker) -> None:
    _, hero = cast(book, faker)
    chapters = book.dat.repository(Chapter)
    chapter = Chapter(
        heading=faker.sentence(), body=faker.paragraph(), position=3, pov=None, appears=[]
    )
    chapters.add(chapter)
    participant = book.dat.store("text").participant()
    original = participant.finish

    def die(*a: object, **k: object) -> None:
        raise Crash()

    participant.finish = die  # type: ignore[method-assign]
    book.wall.ms += 60_000
    chapter.pov, chapter.appears = Ref.to(hero), [Ref.to(hero)]
    with pytest.raises(Crash):
        chapters.update(chapter)
    participant.finish = original  # type: ignore[method-assign]

    # drevo committed first; the text part is still prepared: reads see the old version.
    seen = chapters.get(chapter.dat_id)
    assert seen is not None
    assert seen.pov is None

    book.recover()
    assert chapters.get(chapter.dat_id) == chapter
    assert book.chapters_with(hero) == [str(chapter.dat_id)]
