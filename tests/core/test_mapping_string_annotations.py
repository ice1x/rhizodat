"""@entity in a module with string annotations (PEP 563)."""

from __future__ import annotations

from typing import ClassVar

import pytest

from rhizodat.core.mapping import MappingError, Ref, entity, mapping_of


@entity(stores={"pg": ["title", "assignee", "parent", "watchers"]})
class Ticket:
    kind: ClassVar[str] = "ticket"
    title: str
    assignee: Ref[User]
    parent: Ref["Ticket"] | None = None  # noqa: UP037 - quoted on purpose
    watchers: list[Ref[User]] | None = None


@entity(stores={"pg": ["login"]})
class User:
    login: str


def test_string_annotations_are_mapped_and_refs_resolved_by_name() -> None:
    m = mapping_of(Ticket)
    assert m.stores == {"pg": ("title", "assignee", "parent", "watchers")}
    assert m.refs == {"assignee": "User", "parent": "Ticket", "watchers": "User"}


def test_string_reference_to_several_types_is_rejected() -> None:
    with pytest.raises(MappingError, match="several types"):

        @entity(stores={"pg": ["subject"]})
        class Note:
            subject: Ref[User] | Ref[Ticket]


def test_class_variables_are_not_fields() -> None:
    user = User(login="ada")
    ticket = Ticket(title="Crash on save", assignee=Ref.to(user))
    assert Ticket.kind == "ticket"
    assert ticket.parent is None
