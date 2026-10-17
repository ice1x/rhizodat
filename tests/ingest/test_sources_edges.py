"""Import sources at their edges: empty and broken files, every Parquet type, big columns.

A CBT journal exports its entries in several formats over the years; some
exports are empty, some have a broken line, some carry types an import cannot
take. Each case is refused with what is wrong and where, or read as it is.
"""

import datetime as dt
import decimal
import json
from pathlib import Path

import pytest
from faker import Faker

from rhizodat.ingest import Column, TableSchema, infer_schema, read_batches
from rhizodat.ingest.sources import BATCH_ROWS


def test_an_unopenable_uri_is_refused_without_its_secret(faker: Faker) -> None:
    secret = faker.pystr(min_chars=20, max_chars=20)
    with pytest.raises(ValueError, match="cannot open") as caught:
        infer_schema(f"nosuch://reader:{secret}@bucket/moods.csv")
    assert secret not in str(caught.value)


def test_a_zero_byte_file_has_no_header(tmp_path: Path) -> None:
    empty = tmp_path / "e.csv"
    empty.write_bytes(b"")
    with pytest.raises(ValueError, match="empty"):
        infer_schema(empty)


def test_too_many_distinct_values_are_not_low_cardinality(tmp_path: Path) -> None:
    path = tmp_path / "ids.csv"
    path.write_text("mood\n" + "\n".join(f"m{i % 10_002}" for i in range(110_000)) + "\n")
    [mood] = infer_schema(path).columns
    assert mood.type == "string"
    assert not mood.low_cardinality  # 10 002 distinct: beyond the dictionary limit


def test_sample_rows_zero_reads_no_values(tmp_path: Path) -> None:
    path = tmp_path / "s.csv"
    path.write_text("a\n1\n2\n")
    schema = infer_schema(path, sample_rows=0)
    assert (schema.rows, schema.columns[0].type, schema.columns[0].nullable) == (0, "string", True)


def test_json_lines_skip_blank_lines_and_refuse_broken_ones(tmp_path: Path) -> None:
    ok = tmp_path / "ok.jsonl"
    ok.write_text('{"a": 1}\n\n{"a": 2}\n')
    assert infer_schema(ok).rows == 2
    for name, text, message in [
        ("broken.jsonl", '{"a": 1}\n{"a": \n', "line 2 is not JSON"),
        ("list.jsonl", "[1, 2]\n", "line 1 is not a JSON object"),
        ("empty.jsonl", "\n\n", "no JSON objects"),
    ]:
        path = tmp_path / name
        path.write_text(text)
        with pytest.raises(ValueError, match=message):
            infer_schema(path)


def test_json_lines_sample_and_read_every_type(tmp_path: Path, faker: Faker) -> None:
    path = tmp_path / "entries.jsonl"
    rows = [
        {
            "n": i,
            "score": i if i % 2 else i + 0.5,
            "at": f"2026-01-{i % 28 + 1:02}T10:00:00",
            "at_utc": f"2026-01-{i % 28 + 1:02}T10:00:00+02:00",
            "note": None if i % 3 else faker.sentence(),
        }
        for i in range(BATCH_ROWS + 5)
    ]
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    assert infer_schema(path, sample_rows=3).rows == 3

    schema = infer_schema(path)
    assert [c.type for c in schema.columns] == [
        "int64",
        "float64",
        "timestamp",
        "timestamptz",
        "string",
    ]
    batches = list(read_batches(path, schema))
    assert [b.num_rows for b in batches] == [BATCH_ROWS, 5]
    first = batches[0].to_pylist()[1]
    assert first["score"] == 1.0
    assert first["at"] == dt.datetime(2026, 1, 2, 10)
    assert first["at_utc"] == dt.datetime(2026, 1, 2, 8, tzinfo=dt.UTC)
    assert first["note"] is None


def test_every_parquet_type_maps_or_is_named(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = tmp_path / "t.parquet"
    pq.write_table(
        pa.table(
            {
                "kind": pa.array(["a", "b", "a"]).dictionary_encode(),
                "ok": pa.array([True, False, None]),
                "f": pa.array([1.5, 2.5, None], pa.float32()),
                "d": pa.array([dt.date(2026, 1, 1)] * 3, pa.date32()),
                "t": pa.array([dt.datetime(2026, 1, 1)] * 3, pa.timestamp("ms")),
                "s": pa.array(["x", "y", "z"], pa.large_string()),
            }
        ),
        path,
    )
    schema = infer_schema(path)
    assert [(c.type, c.low_cardinality) for c in schema.columns] == [
        ("string", True),
        ("boolean", False),
        ("float64", False),
        ("date", False),
        ("timestamp", False),
        ("string", False),
    ]
    assert sum(b.num_rows for b in read_batches(path, schema)) == 3

    money = tmp_path / "m.parquet"
    pq.write_table(
        pa.table({"amount": pa.array([decimal.Decimal("1.50")], pa.decimal128(5, 2))}), money
    )
    with pytest.raises(ValueError, match="column amount is decimal"):
        infer_schema(money)


def test_unknown_formats_are_refused(tmp_path: Path) -> None:
    path = tmp_path / "x.csv"
    path.write_text("a\n1\n")
    with pytest.raises(ValueError, match="unknown format 'xml'"):
        infer_schema(path, format="xml")
    with pytest.raises(ValueError, match="unknown format 'xml'"):
        list(read_batches(path, TableSchema((Column("a", "int64"),), 1, "xml")))


def test_a_schema_finds_its_columns() -> None:
    schema = TableSchema((Column("a", "int64"), Column("b", "string")), 2, "csv")
    assert schema.column("b").type == "string"
    with pytest.raises(KeyError, match="columns: \\['a', 'b'\\]"):
        schema.column("c")
