"""Schema inference for file imports: every value of a column decides its type.

An ERP exports its sales as CSV: ids, counts, prices, dates, timestamps, flags
and free text, with ``NULL`` and empty cells for missing values. The inferred
type of a column holds for every row (a late row widens it), never only for
the first one; values that would lose information (leading zeros, numbers
beyond 64 bits) stay text.
"""

import datetime as dt
import gzip
import json
from pathlib import Path

import pytest
from faker import Faker
from hypothesis import given, settings
from hypothesis import strategies as st

from rhizodat.ingest import Column, LogicalType, infer_schema, read_batches
from rhizodat.ingest.schema import join

HEADER = "id,qty,price,day,at,at_utc,paid,note,zip"


def write(path: Path, lines: list[str]) -> Path:
    path.write_text("\n".join(lines) + "\n")
    return path


def sales(tmp_path: Path, faker: Faker, n: int = 50) -> Path:
    rows = [HEADER]
    for i in range(n):
        rows.append(
            ",".join(
                [
                    str(i + 1),
                    str(faker.random_int(-5, 500)),
                    f"{faker.pyfloat(left_digits=3, right_digits=2, positive=True)}",
                    faker.date_between("-2y").isoformat(),
                    faker.date_time_between("-2y").strftime("%Y-%m-%d %H:%M:%S"),
                    faker.date_time_between("-2y", tzinfo=dt.UTC).isoformat(),
                    faker.random_element(["true", "false", "TRUE"]),
                    f'"{faker.sentence()}"',
                    faker.random_element(["01234", "90210", "00501"]),
                ]
            )
        )
    return write(tmp_path / "sales.csv", rows)


def test_a_csv_column_gets_the_narrowest_type_every_row_fits(tmp_path: Path, faker: Faker) -> None:
    schema = infer_schema(sales(tmp_path, faker))
    assert [(c.name, c.type, c.nullable) for c in schema.columns] == [
        ("id", "int64", False),
        ("qty", "int64", False),  # signed: negatives fit (an unsigned type would not)
        ("price", "float64", False),
        ("day", "date", False),
        ("at", "timestamp", False),
        ("at_utc", "timestamptz", False),
        ("paid", "boolean", False),
        ("note", "string", False),
        ("zip", "string", False),  # leading zeros are kept as text
    ]
    assert schema.rows == 50
    assert schema.format == "csv"


def test_a_late_row_widens_the_type(tmp_path: Path) -> None:
    lines = ["n,d,flag"] + ["1,2026-01-01,true"] * 2000 + ["1.5,2026-01-01 10:00:00,maybe"]
    schema = infer_schema(write(tmp_path / "late.csv", lines))
    assert [c.type for c in schema.columns] == ["float64", "timestamp", "string"]


@pytest.mark.parametrize("missing", ["", "NULL", "null", "\\N"])
def test_null_tokens_make_a_column_nullable_and_do_not_decide_its_type(
    tmp_path: Path, missing: str
) -> None:
    schema = infer_schema(write(tmp_path / "n.csv", ["a,b", f"1,{missing}", "2,x"]))
    a, b = schema.columns
    assert (a.type, a.nullable) == ("int64", False)
    assert (b.type, b.nullable) == ("string", True)
    only_null = infer_schema(write(tmp_path / "o.csv", ["a", missing, missing]))
    assert (only_null.columns[0].type, only_null.columns[0].nullable) == ("string", True)


def test_custom_null_tokens(tmp_path: Path) -> None:
    path = write(tmp_path / "n.csv", ["a", "1", "-", "NULL"])
    [a] = infer_schema(path, null_values=("-",)).columns
    assert (a.type, a.nullable) == ("string", True)  # "NULL" is text when not a token


def test_numbers_beyond_64_bits_and_mixed_time_zones_stay_text(tmp_path: Path) -> None:
    lines = [
        "big,mixed",
        "9223372036854775807,2026-01-01T10:00:00",
        "9223372036854775808,2026-01-01T10:00:00+02:00",
    ]
    schema = infer_schema(write(tmp_path / "b.csv", lines))
    assert [c.type for c in schema.columns] == ["string", "string"]


def test_repeated_text_is_marked_low_cardinality(tmp_path: Path, faker: Faker) -> None:
    kinds = ["classic_bike", "electric_bike"]
    lines = ["kind,ride"] + [f"{faker.random_element(kinds)},{faker.uuid4()}" for _ in range(500)]
    kind, ride = infer_schema(write(tmp_path / "r.csv", lines)).columns
    assert kind.low_cardinality
    assert not ride.low_cardinality


def test_tsv_gzip_jsonl_and_parquet(tmp_path: Path, faker: Faker) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    tsv = tmp_path / "t.tsv.gz"
    with gzip.open(tsv, "wt") as f:
        f.write("a\tb\n1\tx y\n2\t\n")
    assert [(c.type, c.nullable) for c in infer_schema(tsv).columns] == [
        ("int64", False),
        ("string", True),
    ]

    jsonl = tmp_path / "e.jsonl"
    jsonl.write_text(
        "\n".join(
            json.dumps(r)
            for r in [
                {"id": 1, "ok": True, "at": "2026-01-01", "tags": ["a"], "score": 1},
                {"id": 2, "ok": False, "at": "2026-01-02", "score": 2.5, "extra": None},
            ]
        )
        + "\n"
    )
    schema = infer_schema(jsonl)
    assert {c.name: (c.type, c.nullable) for c in schema.columns} == {
        "id": ("int64", False),
        "ok": ("boolean", False),
        "at": ("date", False),
        "tags": ("string", True),  # nested values become JSON text; missing in row 2
        "score": ("float64", False),
        "extra": ("string", True),
    }

    parquet = tmp_path / "p.parquet"
    pq.write_table(
        pa.table(
            {
                "n": pa.array([1, None], pa.int32()),
                "t": pa.array(
                    [dt.datetime(2026, 1, 1, tzinfo=dt.UTC)] * 2, pa.timestamp("us", "UTC")
                ),
                "s": pa.array([faker.word(), faker.word()]),
            }
        ),
        parquet,
    )
    assert [(c.type, c.nullable) for c in infer_schema(parquet).columns] == [
        ("int64", True),
        ("timestamptz", True),
        ("string", True),
    ]


def test_batches_carry_the_inferred_types(tmp_path: Path, faker: Faker) -> None:
    path = sales(tmp_path, faker, n=10)
    schema = infer_schema(path)
    batches = list(read_batches(path, schema))
    assert sum(b.num_rows for b in batches) == 10
    first = batches[0].to_pylist()[0]
    assert isinstance(first["day"], dt.date)
    assert isinstance(first["at"], dt.datetime)
    assert first["at"].tzinfo is None
    assert first["at_utc"].tzinfo is not None
    assert isinstance(first["paid"], bool)
    assert first["zip"].startswith("0") or len(first["zip"]) == 5


@pytest.mark.parametrize(
    ("lines", "message"),
    [
        (["a,a", "1,2"], "column a appears twice"),
        (["a,", "1,2"], "column 2 has no name"),
        ([], "empty"),
    ],
)
def test_headers_must_name_every_column_once(
    tmp_path: Path, lines: list[str], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        infer_schema(write(tmp_path / "h.csv", lines))


def test_an_unknown_format_is_named(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match=r"\.xlsx"):
        infer_schema(write(tmp_path / "x.xlsx", ["a"]))


# --- the lattice -------------------------------------------------------------------------

TYPES: list[LogicalType] = [
    "boolean",
    "int64",
    "float64",
    "date",
    "timestamp",
    "timestamptz",
    "string",
]


@given(st.sampled_from(TYPES), st.sampled_from(TYPES), st.sampled_from(TYPES))
@settings(max_examples=200)
def test_join_is_a_semilattice(a: LogicalType, b: LogicalType, c: LogicalType) -> None:
    assert join(a, a) == a
    assert join(a, b) == join(b, a)
    assert join(join(a, b), c) == join(a, join(b, c))
    assert join(a, "string") == "string"


def test_column_is_plain_data() -> None:
    c = Column("price", "float64", nullable=True)
    assert (c.name, c.type, c.nullable, c.low_cardinality) == ("price", "float64", True, False)
