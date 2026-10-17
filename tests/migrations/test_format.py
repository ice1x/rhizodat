"""The migration format: one TOML file per migration, numbered steps per store.

A bug tracker's schema grows over releases: a file ``0001_bugs.toml`` creates
the PostgreSQL table, a later one adds a graph index. Files are read the same
way by every implementation; the checksum pins what was applied.
"""

import hashlib
from pathlib import Path

import pytest
from faker import Faker

from rhizodat.migrations import (
    Migration,
    MigrationFormatError,
    Step,
    canonical,
    load_migrations,
    parse_migration,
)

BUGS = '''
description = "the bug table and its graph index"

[[steps]]
store = "pg"
sql = """
CREATE TABLE IF NOT EXISTS bug (
    id uuid PRIMARY KEY,
    title text NOT NULL
)"""

[[steps]]
store = "graph"
cypher = ["CREATE INDEX bug_title IF NOT EXISTS FOR (b:Bug) ON (b.title)"]

[[steps]]
store = "lake"
iceberg = [{ op = "add_column", table = "tracker.bugs", name = "severity", type = "int" }]
'''


def test_a_file_names_its_id_and_holds_steps_per_store() -> None:
    m = parse_migration("0001_bugs.toml", BUGS)
    assert (m.id, m.name) == (1, "bugs")
    assert m.description == "the bug table and its graph index"
    assert [(s.store, s.kind) for s in m.steps] == [
        ("pg", "sql"),
        ("graph", "cypher"),
        ("lake", "iceberg"),
    ]
    assert m.steps[0].body.startswith("CREATE TABLE IF NOT EXISTS bug")
    assert m.steps[1].body == ["CREATE INDEX bug_title IF NOT EXISTS FOR (b:Bug) ON (b.title)"]
    assert m.steps[2].body == [
        {"op": "add_column", "table": "tracker.bugs", "name": "severity", "type": "int"}
    ]
    assert m.stores == ("pg", "graph", "lake")


def test_the_checksum_is_sha256_of_the_canonical_form() -> None:
    m = parse_migration("0001_bugs.toml", BUGS)
    expected = hashlib.sha256(canonical(m).encode()).hexdigest()
    assert m.checksum == expected
    # Keys sorted, no spaces; the description is not part of it.
    assert canonical(m).startswith('{"id":1,"name":"bugs","steps":[{"body":"CREATE TABLE')
    reworded = parse_migration("0001_bugs.toml", BUGS.replace("the bug table", "bugs"))
    assert reworded.checksum == m.checksum
    edited = parse_migration("0001_bugs.toml", BUGS.replace("title text", "title varchar"))
    assert edited.checksum != m.checksum


def test_migrations_are_built_in_code_too(faker: Faker) -> None:
    sql = f"CREATE TABLE IF NOT EXISTS {faker.word()} (id int)"
    m = Migration.of(7, "seed", [Step("pg", "sql", sql)])
    assert m == parse_migration("0007_seed.toml", f'[[steps]]\nstore = "pg"\nsql = "{sql}"\n')


@pytest.mark.parametrize(
    ("filename", "text", "message"),
    [
        ("bugs.toml", BUGS, "NNNN_name.toml"),
        ("0000_bugs.toml", BUGS, "at least 1"),
        ("0001_Bugs.toml", BUGS, "NNNN_name.toml"),
        ("0001_bugs.toml", "steps = [", "not valid TOML"),
        ("0001_bugs.toml", "", "no steps"),
        ("0001_bugs.toml", "steps = []", "no steps"),
        ("0001_bugs.toml", 'author = "me"\n' + BUGS, "unknown key author"),
        ("0001_bugs.toml", 'description = 1\n[[steps]]\nstore = "pg"\nsql = "x"\n', "description"),
        ("0001_bugs.toml", '[[steps]]\nsql = "SELECT 1"\n', "store"),
        ("0001_bugs.toml", '[[steps]]\nstore = "pg"\n', "exactly one body"),
        ("0001_bugs.toml", '[[steps]]\nstore = "pg"\nsql = "x"\ncypher = "y"\n', "exactly one"),
        ("0001_bugs.toml", '[[steps]]\nstore = "p g"\nsql = "x"\n', "store name"),
        ("0001_bugs.toml", '[[steps]]\nstore = "pg"\nSQL = "x"\n', "step kind"),
        ("0001_bugs.toml", '[[steps]]\nstore = "pg"\nsql = 1.5\n', "floats"),
        ("0001_bugs.toml", '[[steps]]\nstore = "pg"\nsql = 2026-01-01\n', "dates"),
    ],
)
def test_bad_files_are_refused_with_their_name(filename: str, text: str, message: str) -> None:
    with pytest.raises(MigrationFormatError, match=message) as caught:
        parse_migration(filename, text)
    assert filename in str(caught.value)


def test_a_directory_is_read_in_id_order(tmp_path: Path) -> None:
    (tmp_path / "0002_graph.toml").write_text('[[steps]]\nstore = "graph"\ncypher = "RETURN 1"\n')
    (tmp_path / "0001_bugs.toml").write_text(BUGS)
    (tmp_path / "README.md").write_text("not a migration")
    assert [m.id for m in load_migrations(tmp_path)] == [1, 2]


def test_a_directory_with_a_duplicate_id_or_a_stray_toml_is_refused(tmp_path: Path) -> None:
    (tmp_path / "0001_bugs.toml").write_text(BUGS)
    (tmp_path / "001_again.toml").write_text(BUGS)
    with pytest.raises(MigrationFormatError, match="two migrations with id 1"):
        load_migrations(tmp_path)
    (tmp_path / "001_again.toml").unlink()
    (tmp_path / "notes.toml").write_text("")
    with pytest.raises(MigrationFormatError, match=r"notes\.toml"):
        load_migrations(tmp_path)
    with pytest.raises(MigrationFormatError, match="no such directory"):
        load_migrations(tmp_path / "missing")


# --- writing ------------------------------------------------------------------------------

from hypothesis import given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402

from rhizodat.migrations import to_toml  # noqa: E402

_text = st.text(st.characters(codec="utf-8"), max_size=40)
_key = st.text(st.characters(codec="utf-8"), min_size=1, max_size=12)
_body = st.recursive(
    st.one_of(_text, st.integers(-(2**63), 2**63 - 1), st.booleans()),
    lambda inner: st.one_of(st.lists(inner, max_size=4), st.dictionaries(_key, inner, max_size=4)),
    max_leaves=12,
)


@given(
    body=_body,
    kind=st.from_regex(r"[a-z][a-z0-9_]{0,10}", fullmatch=True),
    description=st.none() | _text,
)
@settings(max_examples=300, deadline=None)
def test_a_written_migration_reads_back_the_same(
    body: object, kind: str, description: str | None
) -> None:
    m = Migration.of(
        12, "import_sales", [Step("lake", kind, body), Step("pg", "sql", "SELECT 1")], description
    )
    assert parse_migration("0012_import_sales.toml", to_toml(m)) == m


def test_a_written_migration_is_readable() -> None:
    m = parse_migration("0001_bugs.toml", BUGS)
    text = to_toml(m)
    assert text.startswith('description = "the bug table and its graph index"\n')
    assert '\n[[steps]]\nstore = "pg"\nsql = """\nCREATE TABLE IF NOT EXISTS bug (' in text
    assert parse_migration("0001_bugs.toml", text) == m


@pytest.mark.parametrize(
    ("args", "message"),
    [
        ((0, "a", [Step("pg", "sql", "x")]), "at least 1"),
        ((1, "Bad-Name", [Step("pg", "sql", "x")]), "lower-case"),
        ((1, "a", []), "no steps"),
        ((1, "a", [Step("pg", "kv", {1: "x"})]), "keys must be strings"),
        ((1, "a", [Step("pg", "kv", None)]), "unsupported value None"),
    ],
)
def test_migrations_built_in_code_are_checked_like_files(args: tuple, message: str) -> None:  # type: ignore[type-arg]
    with pytest.raises(MigrationFormatError, match=message):
        Migration.of(*args)


def test_a_migration_that_bypassed_the_checks_cannot_be_written() -> None:
    unchecked = Migration(1, "a", (Step("pg", "kv", object()),))
    with pytest.raises(ValueError, match="cannot write"):
        to_toml(unchecked)


def test_two_migrations_with_one_id_are_refused() -> None:
    from rhizodat.migrations.format import by_id

    m = Migration.of(1, "a", [Step("pg", "sql", "x")])
    with pytest.raises(MigrationFormatError, match="two migrations with id 1"):
        by_id([m, m])
