"""Field types of an entity, resolved once the classes they name exist, and the
mapping's refusals that the other mapping tests do not reach."""

import typing
import uuid
from typing import ClassVar, Literal

import pytest

from rhizodat.core.mapping import MappingError, Ref, entity, mapping_of


@entity(stores={"pg": ["title", "owner", "deputy", "tags", "level"]}, name="types.Task")
class Task:
    title: "str"
    owner: "Ref[Person]"
    deputy: Ref["Person"] | None = None
    tags: list["Nowhere"] = None  # type: ignore[assignment,name-defined]  # noqa: F821 - names no class
    level: Literal["low", "high"] = "low"


@entity(stores={"pg": ["name"]}, name="types.Person")
class Person:
    kind: "ClassVar[str]" = "person"
    name: str


def test_string_annotations_resolve_once_their_classes_exist() -> None:
    types = mapping_of(Task).field_types()
    assert types["title"] is str
    assert typing.get_origin(types["owner"]) is Ref
    assert typing.get_args(types["owner"]) == (Person,)


def test_what_cannot_be_resolved_is_kept_or_left_as_object() -> None:
    types = mapping_of(Task).field_types()
    # A name no module defines stays as the annotation (list of a forward ref);
    # a literal's strings are values, not names.
    assert typing.get_origin(types["tags"]) is list
    assert types["level"] == Literal["low", "high"]


def test_an_unresolvable_annotation_maps_to_object() -> None:
    @entity(stores={"pg": ["x"]}, name="types.Ghost")
    class Ghost:
        x: "NoSuchType"  # type: ignore[name-defined]  # noqa: F821 - unresolvable on purpose

    assert mapping_of(Ghost).field_types()["x"] is object


def test_a_quoted_class_variable_is_not_a_field() -> None:
    assert mapping_of(Person).stores == {"pg": ("name",)}
    assert Person.kind == "person"


def test_references_name_their_target_however_it_is_written() -> None:
    # A forward name is kept as written: not the target's declared name (types.Person).
    assert mapping_of(Task).refs == {"owner": "Person", "deputy": "Person"}


def test_the_store_of_a_field_and_of_an_unknown_one() -> None:
    m = mapping_of(Task)
    assert m.store_of("title") == "pg"
    with pytest.raises(MappingError, match="'nope' is not a mapped field"):
        m.store_of("nope")


def test_assemble_with_a_field_missing_from_a_part_is_rejected() -> None:
    m = mapping_of(Person)
    with pytest.raises(MappingError, match=r"lacks \['name'\]"):
        m.assemble(uuid.uuid4(), {"pg": {}})


def test_a_service_name_mapped_but_not_declared_is_reserved() -> None:
    with pytest.raises(MappingError, match="reserved"):

        @entity(stores={"pg": ["title", "dat_secret"]}, name="types.Bad")
        class Bad:
            title: str
