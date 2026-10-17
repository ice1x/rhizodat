"""Vector fields on entities, versioned with them.

A bug tracker keeps an embedding of every issue's title next to the model that
made it. The vector is an ordinary field: written in the same Unit of Work as
the rest of the entity, so re-embedding with another model is a new version,
and ``get(as_of=...)`` / ``history()`` show which model made the vectors at a
moment. ``similar`` finds the issues nearest to a vector (or a text, with the
embedder that made the vectors) among the versions valid at that moment.
"""

# @entity adds dat_id and the service times at runtime; mypy cannot see them.
# mypy: disable-error-code="attr-defined, call-arg"

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Annotated, cast

import pytest
from faker import Faker

from rhizodat import Match, Rhizodat, Vector, entity
from rhizodat.core.clock import HybridLogicalClock
from rhizodat.core.journal import MemoryJournal
from rhizodat.core.mapping import MappingError, mapping_of
from rhizodat.embed import HashEmbedder
from rhizodat.testing.memory import MemoryEntityStore


@entity(stores={"pg": ["title", "model"], "graph": ["embedding"]}, soft_delete=True)
class Issue:
    title: str
    embedding: Annotated[list[float], Vector(3)] | None
    model: str | None


@entity(stores={"pg": ["sku", "embedding"]}, valid_time=True)
class Product:
    sku: str
    embedding: Annotated[list[float], Vector(2)]


@entity(stores={"pg": ["title", "embedding"]})
class Doc:
    title: str
    embedding: Annotated[list[float], Vector(16)]


class Wall:
    def __init__(self) -> None:
        self.ms = int(datetime(2026, 10, 17, 9, 0, tzinfo=UTC).timestamp() * 1000)

    def __call__(self) -> int:
        return self.ms

    def tick(self) -> datetime:
        """Move on a minute; the moment before the move is in the past for good."""
        before = datetime.fromtimestamp(self.ms / 1000, UTC)
        self.ms += 60_000
        return before


@pytest.fixture
def wall() -> Wall:
    return Wall()


@pytest.fixture
def dat(wall: Wall) -> Iterator[Rhizodat]:
    d = Rhizodat(
        stores={"pg": MemoryEntityStore("pg"), "graph": MemoryEntityStore("graph")},
        journal=MemoryJournal(wall_ms=wall),
        clock=HybridLogicalClock(wall_ms=wall),
    )
    yield d
    d.close()


def issue(faker: Faker, embedding: list[float] | None, model: str | None = "m1") -> Issue:
    return Issue(title=faker.sentence(), embedding=embedding, model=model)


# --- declaring ---------------------------------------------------------------------------


def test_a_vector_field_declares_its_dimensions() -> None:
    assert mapping_of(Issue).vectors() == {"embedding": 3}
    assert mapping_of(Product).vectors() == {"embedding": 2}
    assert Vector(3) == Vector(3)
    assert repr(Vector(3)) == "Vector(3)"
    with pytest.raises(ValueError, match="at least 1"):
        Vector(0)


def test_a_vector_annotation_must_hold_a_list_of_floats() -> None:
    @entity(stores={"pg": ["v"]})
    class Wrong:
        v: Annotated[str, Vector(3)]

    with pytest.raises(MappingError, match="list\\[float\\]"):
        mapping_of(Wrong).vectors()


# --- writing -----------------------------------------------------------------------------


def test_vectors_round_trip_and_are_checked_on_write(dat: Rhizodat, faker: Faker) -> None:
    issues = dat.repository(Issue)
    one = issue(faker, [0.1, 0.2, 0.3])
    issues.add(one)
    got = issues.get(one.dat_id)
    assert got is not None
    assert got.embedding == [0.1, 0.2, 0.3]

    issues.add(issue(faker, None))  # an issue not embedded yet

    stored = issue(faker, (1, 0, 0))  # type: ignore[arg-type]  # any numbers, stored as floats
    issues.add(stored)
    again = issues.get(stored.dat_id)
    assert again is not None
    assert again.embedding == [1.0, 0.0, 0.0]
    assert all(isinstance(x, float) for x in again.embedding)

    with pytest.raises(ValueError, match="3 dimensions, got 2"):
        issues.add(issue(faker, [0.1, 0.2]))
    with pytest.raises(ValueError, match="numbers"):
        issues.add(issue(faker, [0.1, "x", 0.3]))  # type: ignore[list-item]
    with pytest.raises(ValueError, match="numbers"):
        issues.add(issue(faker, [0.1, True, 0.3]))
    with pytest.raises(ValueError, match="finite"):
        issues.add(issue(faker, [0.1, float("nan"), 0.3]))


# --- searching ---------------------------------------------------------------------------


def test_similar_returns_the_nearest_entities_closest_first(dat: Rhizodat, faker: Faker) -> None:
    issues = dat.repository(Issue)
    near, middle, far = issue(faker, [1, 0.1, 0]), issue(faker, [1, 1, 0]), issue(faker, [0, 0, 1])
    for i in (far, middle, near, issue(faker, None)):
        issues.add(i)

    found = issues.similar("embedding", [1, 0, 0], k=2)
    assert [m.entity.dat_id for m in found] == [near.dat_id, middle.dat_id]
    assert all(isinstance(m, Match) for m in found)
    assert found[0].score == pytest.approx(1 / (1.01**0.5))
    assert found[0].score > found[1].score
    assert found[0].entity.title == near.title  # the whole entity, every store's part

    issues.delete(near.dat_id)  # soft-deleted entities are not found
    assert [m.entity.dat_id for m in issues.similar("embedding", [1, 0, 0], k=1)] == [middle.dat_id]
    assert issues.similar("embedding", [0, 0, 1], k=10)[-1].score == pytest.approx(0.0)


def test_reembedding_is_a_version_and_the_past_is_searched_as_it_was(
    dat: Rhizodat, faker: Faker, wall: Wall
) -> None:
    issues = dat.repository(Issue)
    a, b = issue(faker, [1, 0, 0], "m1"), issue(faker, [0, 1, 0], "m1")
    issues.add(a)
    issues.add(b)
    before = wall.tick()

    a.embedding, a.model = [0, 1, 0], "m2"
    b.embedding, b.model = [1, 0, 0], "m2"
    issues.update(a)
    issues.update(b)

    [now] = issues.similar("embedding", [1, 0, 0], k=1)
    assert (now.entity.dat_id, now.entity.model) == (b.dat_id, "m2")
    [then] = issues.similar("embedding", [1, 0, 0], k=1, as_of=before)
    assert (then.entity.dat_id, then.entity.model) == (a.dat_id, "m1")
    assert then.entity.embedding == [1.0, 0.0, 0.0]

    old = issues.get(a.dat_id, as_of=before)
    assert old is not None
    assert old.model == "m1"
    assert [v.model for v in issues.history(a.dat_id)] == ["m1", "m2"]
    assert issues.similar("embedding", [1, 0, 0], as_of=before - timedelta(days=1)) == []


def test_a_valid_time_type_is_searched_among_the_versions_holding_then(
    dat: Rhizodat, faker: Faker, wall: Wall
) -> None:
    def day(n: int) -> datetime:
        return datetime(2026, 1, 1, tzinfo=UTC) + timedelta(days=n)

    products = dat.repository(Product)
    winter = Product(sku=faker.ean8(), embedding=[1, 0], valid_from=day(0), valid_to=day(90))
    products.add(winter)
    summer = Product(sku=winter.sku, embedding=[0, 1], valid_from=day(90), valid_to=None)
    summer.dat_id = winter.dat_id
    products.update(summer)
    known = wall.tick()

    [jan] = products.similar("embedding", [1, 0], k=1, as_of=day(10))
    assert jan.entity.embedding == [1.0, 0.0]
    assert jan.score == pytest.approx(1.0)
    [jul] = products.similar("embedding", [1, 0], k=1, as_of=day(200))
    assert jul.entity.embedding == [0.0, 1.0]
    assert jul.score == pytest.approx(0.0)
    assert products.similar("embedding", [1, 0], as_of=day(-1)) == []  # holds nowhere then
    [seen] = products.similar("embedding", [1, 0], k=1, as_of=day(200), known_at=known)
    assert seen.entity.embedding == [0.0, 1.0]
    with pytest.raises(ValueError, match="time zone"):
        products.similar("embedding", [1, 0], as_of=datetime(2026, 1, 1))


def test_a_text_query_is_embedded_with_the_model_that_made_the_vectors(
    dat: Rhizodat, faker: Faker
) -> None:
    embedder = HashEmbedder(16)
    docs = dat.repository(Doc)
    titles = ["login page crashes", "invoice totals are wrong", "dark mode colours"]
    for title in titles:
        docs.add(Doc(title=title, embedding=embedder.embed([title])[0]))

    [best] = docs.similar("embedding", "the login page crashes on submit", k=1, embedder=embedder)
    assert best.entity.title == "login page crashes"

    with pytest.raises(ValueError, match="needs an embedder"):
        docs.similar("embedding", "login")
    with pytest.raises(ValueError, match="16 dimensions, got 2"):
        docs.similar("embedding", [1, 0])
    with pytest.raises(ValueError, match="not a vector field"):
        docs.similar("title", [0.0] * 16)
    with pytest.raises(ValueError, match="k must be"):
        docs.similar("embedding", [0.0] * 16, k=0)


def test_without_valid_time_as_of_and_known_at_are_one_moment(dat: Rhizodat, faker: Faker) -> None:
    issues = dat.repository(Issue)
    now = datetime.now(UTC)
    with pytest.raises(ValueError, match="give one"):
        issues.similar("embedding", [1, 0, 0], as_of=now, known_at=now)
    issues.add(issue(faker, [1, 0, 0]))
    assert len(issues.similar("embedding", [1, 0, 0], known_at=datetime.now(UTC) + timedelta(1)))


def test_a_store_that_cannot_be_scanned_is_named(faker: Faker) -> None:
    # MemoryEntityStore without part_versions: a store that only reads by id.
    unscannable = type(
        "Unscannable",
        (),
        {
            k: v
            for k, v in vars(MemoryEntityStore).items()
            if k not in ("part_versions", "__dict__", "__weakref__")
        },
    )
    Unscannable = cast("type[MemoryEntityStore]", unscannable)

    d = Rhizodat(stores={"pg": Unscannable("pg"), "graph": Unscannable("graph")})
    try:
        with pytest.raises(TypeError, match="cannot be searched"):
            d.repository(Issue).similar("embedding", [1, 0, 0])
    finally:
        d.close()
