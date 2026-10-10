"""Entities spread over several stores, round-tripped through in-memory stand-ins."""

from collections import defaultdict
from collections.abc import Iterator
from typing import Any
from uuid import UUID

import pytest
from faker import Faker

from datorium.core.mapping import Ref, entity, mapping_of
from datorium.core.result import Result


class MemoryStores:
    """One dict per store name, keyed by dat_id, standing in for the real stores."""

    def __init__(self) -> None:
        self.data: dict[str, dict[UUID, dict[str, Any]]] = defaultdict(dict)

    def save(self, obj: object) -> None:
        m = mapping_of(type(obj))
        for store, part in m.split(obj).items():
            self.data[store][obj.dat_id] = {"dat_type": m.type_name, **part}  # type: ignore[attr-defined]

    def load(self, cls: type, dat_id: UUID) -> object:
        m = mapping_of(cls)
        parts = {}
        for store in m.stores:
            row = dict(self.data[store][dat_id])
            assert row.pop("dat_type") == m.type_name
            parts[store] = row
        return m.assemble(dat_id, parts)

    def exists(self, type_name: str, dat_id: UUID) -> bool:
        return any(
            row.get("dat_type") == type_name
            for rows in self.data.values()
            if (row := rows.get(dat_id))
        )

    def query(self, store: str, **where: object) -> Result:
        def rows() -> Iterator[dict[str, Any]]:
            for dat_id, row in self.data[store].items():
                if all(row.get(k) == v for k, v in where.items()):
                    yield {"dat_id": dat_id, **row}

        return Result(rows(), raw=self.data[store])


def dangling_refs(stores: MemoryStores, obj: object) -> list[str]:
    """What the Unit of Work checks before commit: every Ref points at a stored entity."""
    m = mapping_of(type(obj))
    missing = []
    for field, target in m.refs.items():
        value = getattr(obj, field)
        refs = value if isinstance(value, list) else [value]
        missing += [
            f"{field}->{target}"
            for r in refs
            if r is not None and not stores.exists(target, r.dat_id)
        ]
    return missing


# --- story / book editor -----------------------------------------------------


@entity(stores={"pg": ["name"]})
class Author:
    name: str


@entity(
    stores={
        "pg": ["title", "author"],
        "drevo": ["chapters", "characters"],
        "iceberg": ["drafts"],
    }
)
class Book:
    title: str
    author: Ref[Author]
    chapters: list[Ref["Chapter"]]
    characters: list[str]
    drafts: list[str]


@entity(stores={"pg": ["heading", "text"]})
class Chapter:
    heading: str
    text: str


def test_book_is_split_across_three_stores_and_read_back(faker: Faker) -> None:
    stores = MemoryStores()
    author = Author(name=faker.name())
    chapters = [Chapter(heading=faker.sentence(), text=faker.paragraph()) for _ in range(3)]
    book = Book(
        title=faker.catch_phrase(),
        author=Ref.to(author),
        chapters=[Ref.to(c) for c in chapters],
        characters=[faker.first_name() for _ in range(4)],
        drafts=[faker.text() for _ in range(2)],
    )
    for obj in [author, *chapters, book]:
        stores.save(obj)

    assert set(stores.data["drevo"]) == {book.dat_id}  # type: ignore[attr-defined]
    assert stores.data["iceberg"][book.dat_id]["drafts"] == book.drafts  # type: ignore[attr-defined]
    assert stores.load(Book, book.dat_id) == book  # type: ignore[attr-defined]
    assert dangling_refs(stores, book) == []


def test_unsaved_chapter_is_reported_as_a_dangling_reference(faker: Faker) -> None:
    stores = MemoryStores()
    author = Author(name=faker.name())
    draft_chapter = Chapter(heading=faker.sentence(), text=faker.paragraph())
    book = Book(
        title=faker.catch_phrase(),
        author=Ref.to(author),
        chapters=[Ref.to(draft_chapter)],
        characters=[],
        drafts=[],
    )
    stores.save(author)
    stores.save(book)
    assert dangling_refs(stores, book) == ["chapters->Chapter"]


def test_reference_to_an_entity_of_the_wrong_type_is_dangling(faker: Faker) -> None:
    stores = MemoryStores()
    impostor = Chapter(heading=faker.sentence(), text=faker.paragraph())
    stores.save(impostor)
    book = Book(
        title=faker.catch_phrase(),
        author=Ref(impostor.dat_id),  # type: ignore[attr-defined]
        chapters=[],
        characters=[],
        drafts=[],
    )
    assert dangling_refs(stores, book) == ["author->Author"]


# --- bug tracker -------------------------------------------------------------


@entity(
    stores={"pg": ["title", "status", "reporter"], "clickhouse": ["events"]}, name="tracker.Bug"
)
class Bug:
    title: str
    status: str
    reporter: Ref[Author]
    events: list[str]


@pytest.fixture
def tracker(faker: Faker) -> tuple[MemoryStores, list[Bug]]:
    stores = MemoryStores()
    reporter = Author(name=faker.name())
    stores.save(reporter)
    bugs = [
        Bug(
            title=faker.sentence(),
            status=faker.random_element(["open", "open", "closed"]),
            reporter=Ref.to(reporter),
            events=["opened"],
        )
        for _ in range(20)
    ]
    for bug in bugs:
        stores.save(bug)
    return stores, bugs


def test_open_bugs_are_queried_natively_and_streamed(
    tracker: tuple[MemoryStores, list[Bug]],
) -> None:
    stores, bugs = tracker
    expected = {b.dat_id for b in bugs if b.status == "open"}  # type: ignore[attr-defined]

    with stores.query("pg", status="open", dat_type="tracker.Bug") as result:
        assert {row["dat_id"] for row in result} == expected
    assert result.raw is stores.data["pg"]


def test_first_open_bug_and_exactly_one_by_id(tracker: tuple[MemoryStores, list[Bug]]) -> None:
    stores, bugs = tracker
    first = stores.query("pg", status="open").first()
    assert first is not None
    assert first["status"] == "open"

    target = bugs[7]
    one = stores.query("pg", title=target.title, dat_type="tracker.Bug").one()
    assert one["dat_id"] == target.dat_id  # type: ignore[attr-defined]


def test_bug_analytics_to_arrow(tracker: tuple[MemoryStores, list[Bug]]) -> None:
    pytest.importorskip("pyarrow")
    stores, bugs = tracker
    table = stores.query("clickhouse").to_arrow()
    assert table.num_rows == len(bugs)
    assert set(table.column_names) == {"dat_id", "dat_type", "events"}
