"""@entity declarations: per-type store mapping, references, valid time."""

import dataclasses
import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from datorium.core.identity import is_uuid7
from datorium.core.mapping import MappingError, Ref, entity, is_entity, mapping_of


@entity(stores={"crm": ["name", "email"]})
class Customer:
    name: str
    email: str


@entity(
    stores={
        "pg": ["status", "amount", "customer"],
        "drevo": ["related"],
        "clickhouse": ["events"],
    }
)
class Order:
    status: str
    amount: Decimal
    customer: Ref["Customer"]
    related: list[Ref["Order"]]
    events: list[str]


def make_order(**overrides: object) -> Order:
    fields: dict[str, object] = {
        "status": "new",
        "amount": Decimal("10.00"),
        "customer": Ref[Customer](uuid.uuid4()),
        "related": [],
        "events": ["created"],
    }
    fields.update(overrides)
    return Order(**fields)  # type: ignore[arg-type]


# --- declaration -----------------------------------------------------------


def test_entity_is_a_keyword_only_dataclass_with_a_generated_dat_id() -> None:
    order = make_order()
    assert dataclasses.is_dataclass(order)
    assert is_uuid7(order.dat_id)  # type: ignore[attr-defined]
    with pytest.raises(TypeError):
        Order("new")  # type: ignore[call-arg]


def test_dat_id_can_be_given_explicitly() -> None:
    known = uuid.uuid4()
    assert make_order(dat_id=known).dat_id == known  # type: ignore[attr-defined]


def test_mapping_describes_type_stores_and_fields() -> None:
    m = mapping_of(Order)
    assert m.type_name == "Order"
    assert m.stores == {
        "pg": ("status", "amount", "customer"),
        "drevo": ("related",),
        "clickhouse": ("events",),
    }
    assert m.store_of("amount") == "pg"
    assert m.valid_time is False
    assert is_entity(Order)
    assert not is_entity(str)


def test_type_name_can_be_set_for_ports_and_renames() -> None:
    @entity(stores={"pg": ["title"]}, name="tracker.Issue")
    class Issue:
        title: str

    assert mapping_of(Issue).type_name == "tracker.Issue"


def test_references_record_the_target_type_name() -> None:
    m = mapping_of(Order)
    assert m.refs == {"customer": "Customer", "related": "Order"}


def test_reference_to_a_class_object_and_optional_reference() -> None:
    @entity(stores={"pg": ["owner", "parent"]})
    class Task:
        owner: Ref[Customer]
        parent: Ref["Task"] | None = None

    assert mapping_of(Task).refs == {"owner": "Customer", "parent": "Task"}


def test_reference_target_uses_the_declared_entity_name() -> None:
    @entity(stores={"pg": ["title"]}, name="tracker.Issue")
    class Issue:
        title: str

    @entity(stores={"pg": ["issue"]})
    class Comment:
        issue: Ref[Issue]

    assert mapping_of(Comment).refs == {"issue": "tracker.Issue"}


def test_reference_to_several_types_is_rejected() -> None:
    with pytest.raises(MappingError, match=r"several types \['Customer', 'Order'\]"):

        @entity(stores={"pg": ["subject"]})
        class Note:
            subject: Ref[Customer] | Ref[Order]


def test_reference_inside_a_mapping_value_is_found() -> None:
    @entity(stores={"pg": ["by_role"]})
    class Team:
        by_role: dict[str, Ref[Customer]]

    assert mapping_of(Team).refs == {"by_role": "Customer"}


def test_valid_time_adds_validity_fields() -> None:
    @entity(stores={"pg": ["rate"]}, valid_time=True)
    class Price:
        rate: Decimal

    start = datetime(2026, 1, 1, tzinfo=UTC)
    price = Price(rate=Decimal("1.5"), valid_from=start)  # type: ignore[call-arg]
    assert mapping_of(Price).valid_time is True
    assert (price.valid_from, price.valid_to) == (start, None)  # type: ignore[attr-defined]


# --- validation ------------------------------------------------------------


def test_field_mapped_to_two_stores_is_rejected() -> None:
    with pytest.raises(MappingError, match=r"'title'.*'pg'.*'ch'"):

        @entity(stores={"pg": ["title"], "ch": ["title"]})
        class Doc:
            title: str


def test_unmapped_field_is_rejected() -> None:
    with pytest.raises(MappingError, match="'body' is not mapped"):

        @entity(stores={"pg": ["title"]})
        class Doc:
            title: str
            body: str


def test_mapping_an_undeclared_field_is_rejected() -> None:
    with pytest.raises(MappingError, match="'summary' is not a field"):

        @entity(stores={"pg": ["title", "summary"]})
        class Doc:
            title: str


@pytest.mark.parametrize("reserved", ["dat_id", "dat_type", "dat_txn", "dat_tx_time", "dat_x"])
def test_service_field_names_are_reserved(reserved: str) -> None:
    with pytest.raises(MappingError, match="reserved"):
        entity(stores={"pg": [reserved]})(type("Doc", (), {"__annotations__": {reserved: str}}))


def test_validity_names_are_reserved_when_valid_time_is_on() -> None:
    with pytest.raises(MappingError, match="reserved"):

        @entity(stores={"pg": ["valid_from"]}, valid_time=True)
        class Doc:
            valid_from: datetime


def test_empty_mapping_and_empty_store_are_rejected() -> None:
    with pytest.raises(MappingError, match="at least one store"):

        @entity(stores={})
        class A:
            x: int

    with pytest.raises(MappingError, match="'pg' maps no fields"):

        @entity(stores={"pg": [], "ch": ["x"]})
        class B:
            x: int


def test_mapping_of_a_plain_class_is_an_error() -> None:
    with pytest.raises(MappingError, match="not an @entity"):
        mapping_of(str)


# --- split / assemble ------------------------------------------------------


def test_split_gives_each_store_its_part() -> None:
    order = make_order()
    assert mapping_of(Order).split(order) == {
        "pg": {"status": "new", "amount": Decimal("10.00"), "customer": order.customer},
        "drevo": {"related": []},
        "clickhouse": {"events": ["created"]},
    }


def test_assemble_is_the_inverse_of_split() -> None:
    order = make_order(related=[Ref[Order](uuid.uuid4())])
    m = mapping_of(Order)
    again = m.assemble(order.dat_id, m.split(order))  # type: ignore[attr-defined]
    assert again == order


def test_assemble_with_a_missing_part_is_rejected() -> None:
    order = make_order()
    m = mapping_of(Order)
    parts = m.split(order)
    del parts["clickhouse"]
    with pytest.raises(MappingError, match="'clickhouse'"):
        m.assemble(order.dat_id, parts)  # type: ignore[attr-defined]


def test_assemble_with_unknown_fields_is_rejected() -> None:
    order = make_order()
    m = mapping_of(Order)
    parts = m.split(order)
    parts["pg"]["extra"] = 1
    with pytest.raises(MappingError, match="'extra'"):
        m.assemble(order.dat_id, parts)  # type: ignore[attr-defined]


# --- Ref -------------------------------------------------------------------


def test_ref_is_a_value_keyed_by_dat_id() -> None:
    key = uuid.uuid4()
    assert Ref[Customer](key) == Ref[Customer](key)
    assert Ref(key) == Ref[Order](key)  # the type parameter is static only
    assert hash(Ref(key)) == hash(Ref(key))
    assert Ref(key).dat_id == key


def test_ref_to_an_entity_instance() -> None:
    customer = Customer(name="Ada", email="ada@example.org")
    assert Ref.to(customer).dat_id == customer.dat_id  # type: ignore[attr-defined]


def test_ref_to_a_plain_object_is_rejected() -> None:
    with pytest.raises(MappingError, match="not an @entity"):
        Ref.to(object())


def test_ref_requires_a_uuid() -> None:
    with pytest.raises(TypeError, match="UUID"):
        Ref("not-a-uuid")  # type: ignore[arg-type]
