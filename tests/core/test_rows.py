"""Row batches as journal payloads: JSON with tagged values, exact round trip."""

import datetime as dt
import decimal
import json
import uuid

import pytest
from faker import Faker
from hypothesis import given
from hypothesis import strategies as st

from datorium.core.rows import Batch, decode_batches, encode_batches


def test_round_trip_keeps_types(faker: Faker) -> None:
    row = [
        uuid.uuid4(),
        faker.name(),
        faker.pyint(),
        1.5,
        decimal.Decimal("12.3400"),
        dt.datetime(2026, 10, 12, 9, 30, 15, 123456, tzinfo=dt.UTC),
        dt.datetime(2026, 10, 12, 9, 30),
        dt.date(2026, 10, 12),
        b"\x00\xff",
        None,
        True,
        [1, 2, 3],
        {"tag": "x", "n": 1},
    ]
    batch = Batch("events", tuple(f"c{i}" for i in range(len(row))), [row])
    [back] = decode_batches(encode_batches([batch]))
    assert back == batch
    assert [type(v) for v in back.rows[0]] == [type(v) for v in row]


def test_payload_is_versioned_json() -> None:
    raw = encode_batches([Batch("t", ("a",), [[1]])])
    assert json.loads(raw)["v"] == 1


def test_unknown_version_is_rejected() -> None:
    with pytest.raises(ValueError, match="version"):
        decode_batches(b'{"v": 99, "batches": []}')


def test_unsupported_value_fails_with_its_column() -> None:
    with pytest.raises(TypeError, match="column 'b'"):
        encode_batches([Batch("t", ("a", "b"), [[1, object()]])])


def test_non_finite_floats_survive() -> None:
    [back] = decode_batches(encode_batches([Batch("t", ("a", "b"), [[float("inf"), -0.0]])]))
    assert back.rows[0][0] == float("inf")
    assert str(back.rows[0][1]) == "-0.0"
    raw = encode_batches([Batch("t", ("a",), [[float("nan")]])])
    assert json.loads(raw)  # strict JSON: no bare NaN
    [nan] = decode_batches(raw)
    assert nan.rows[0][0] != nan.rows[0][0]


def test_map_keys_must_be_strings() -> None:
    with pytest.raises(TypeError, match="map keys"):
        encode_batches([Batch("t", ("a",), [[{1: "x"}]])])


scalars = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(2**127), max_value=2**127),
    st.floats(allow_nan=False),
    st.text(),
    st.binary(),
    st.uuids(),
    st.decimals(allow_nan=False, allow_infinity=False),
    st.dates(),
    st.datetimes(),
    st.datetimes(timezones=st.just(dt.UTC)),
)
values = st.recursive(
    scalars,
    lambda inner: st.lists(inner, max_size=4) | st.dictionaries(st.text(), inner, max_size=4),
    max_leaves=12,
)


@given(st.lists(st.lists(values, min_size=2, max_size=2), max_size=5))
def test_any_supported_batch_round_trips(rows: list[list[object]]) -> None:
    batch = Batch("t", ("a", "b"), rows)
    assert decode_batches(encode_batches([batch])) == [batch]


def test_single_values_round_trip_through_json(faker: Faker) -> None:
    from datorium.core.rows import decode_value, encode_value

    value = {"ids": [uuid.uuid4(), uuid.uuid4()], "total": decimal.Decimal("1.50"), "n": None}
    encoded = encode_value(value)
    assert decode_value(json.loads(json.dumps(encoded))) == value
    word = faker.word()
    assert decode_value(encode_value(word)) == word
