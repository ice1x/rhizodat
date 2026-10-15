"""ClickHouse branch and participant against a real server.

ClickHouse has no transactions: a branch buffers rows, stage checks them against
the table schema and turns them into the journal payload, and the rows are
inserted only after the decision, idempotently.
"""

import datetime as dt
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from faker import Faker

from rhizodat.adapters.clickhouse import ClickHouseBranch, ClickHouseStore
from rhizodat.core.coordinator import gid_for
from rhizodat.core.errors import Capacity, ErrorKind
from rhizodat.core.identity import new_id
from rhizodat.core.journal import BranchRecord
from rhizodat.core.rows import decode_batches

TS = 1_760_000_000_000 << 16

EVENTS = (
    "CREATE TABLE events (id UUID, kind String, amount Decimal(12, 2), at DateTime64(3, 'UTC'),"
    " dat_txn UUID, dat_tx_time Int64) ENGINE = MergeTree ORDER BY (kind, id)"
)


@pytest.fixture
def store(clickhouse_dsn: str) -> Iterator[ClickHouseStore]:
    s = ClickHouseStore(clickhouse_dsn, name="ch")
    s.install()
    s.execute(EVENTS)
    yield s
    s.close()


def events(faker: Faker, n: int) -> list[dict[str, Any]]:
    return [
        {
            "id": uuid.uuid4(),
            "kind": faker.random_element(["view", "buy"]),
            "amount": faker.pydecimal(left_digits=5, right_digits=2, positive=True),
            "at": faker.date_time(tzinfo=dt.UTC).replace(microsecond=0),
        }
        for _ in range(n)
    ]


def count(store: ClickHouseStore, where: str = "") -> int:
    return int(store.execute(f"SELECT count() AS n FROM events {where}").one()["n"])


def record(branch: ClickHouseBranch, payload: bytes | None) -> BranchRecord:
    return BranchRecord("ch", gid_for(branch.dat_txn, "ch"), False, payload)


# --- native access -------------------------------------------------------------


def test_execute_returns_rows_as_mappings(store: ClickHouseStore) -> None:
    result = store.execute("SELECT 1 AS a, 'x' AS b")
    assert result.columns == ("a", "b")
    assert result.all() == [{"a": 1, "b": "x"}]


def test_execute_binds_parameters(store: ClickHouseStore, faker: Faker) -> None:
    word = faker.word()
    assert store.execute("SELECT {w:String} AS w", {"w": word}).one() == {"w": word}


def test_read_only_reads(store: ClickHouseStore, faker: Faker) -> None:
    word = faker.word()
    assert store.read_only("SELECT {w:String} AS w", {"w": word}).one() == {"w": word}


def test_read_only_refuses_writes(store: ClickHouseStore) -> None:
    store.execute("CREATE TABLE notes (body String) ENGINE = MergeTree ORDER BY body")
    for statement in (
        "INSERT INTO notes VALUES ('x')",
        "DROP TABLE notes",
        "SELECT 1 SETTINGS readonly = 0",
    ):
        with pytest.raises(Exception, match="readonly"):
            store.read_only(statement)
    assert store.execute("SELECT count() AS n FROM notes").one()["n"] == 0


def test_install_is_idempotent(store: ClickHouseStore) -> None:
    store.install()
    assert store.execute("SELECT count() AS n FROM dat_applied").one()["n"] == 0


def test_password_is_not_in_repr(clickhouse_dsn: str) -> None:
    s = ClickHouseStore(clickhouse_dsn)
    assert ":***@" in repr(s)
    s.close()


def test_not_a_clickhouse_dsn_is_rejected() -> None:
    with pytest.raises(ValueError, match="clickhouse"):
        ClickHouseStore("postgresql://localhost/x")


# --- branch: buffer and stage ---------------------------------------------------------


def test_rows_are_invisible_until_committed(store: ClickHouseStore, faker: Faker) -> None:
    branch = store.branch(new_id())
    branch.insert("events", events(faker, 3))
    assert count(store) == 0
    branch.stage(branch.dat_txn, TS)
    assert count(store) == 0


def test_one_phase_commit_inserts_rows_with_their_transaction(
    store: ClickHouseStore, faker: Faker
) -> None:
    branch = store.branch(new_id())
    branch.insert("events", events(faker, 3))
    branch.stage(branch.dat_txn, TS)
    branch.commit_one_phase()
    row = store.execute("SELECT DISTINCT dat_txn, dat_tx_time FROM events").one()
    assert (row["dat_txn"], row["dat_tx_time"]) == (branch.dat_txn, TS)
    assert count(store) == 3
    assert store.participant().is_applied(record(branch, None), branch.dat_txn)


def test_stage_payload_carries_the_rows(store: ClickHouseStore, faker: Faker) -> None:
    rows = events(faker, 2)
    branch = store.branch(new_id())
    branch.insert("events", rows)
    payload = branch.stage(branch.dat_txn, TS)
    assert payload is not None
    [batch] = decode_batches(payload)
    assert batch.table == "events"
    assert batch.columns == ("id", "kind", "amount", "at", "dat_txn", "dat_tx_time")
    assert [r[:4] for r in batch.rows] == [list(r.values()) for r in rows]
    assert {r[4] for r in batch.rows} == {branch.dat_txn}


def test_inserts_into_one_table_are_merged(store: ClickHouseStore, faker: Faker) -> None:
    branch = store.branch(new_id())
    branch.insert("events", events(faker, 2))
    branch.insert("events", events(faker, 1))
    payload = branch.stage(branch.dat_txn, TS)
    assert payload is not None
    assert [len(b.rows) for b in decode_batches(payload)] == [3]


def test_rows_must_have_the_same_columns(store: ClickHouseStore, faker: Faker) -> None:
    branch = store.branch(new_id())
    rows = events(faker, 2)
    del rows[1]["amount"]
    with pytest.raises(ValueError, match="same columns"):
        branch.insert("events", rows)


def test_reserved_columns_are_set_by_rhizodat(store: ClickHouseStore, faker: Faker) -> None:
    rows = events(faker, 1)
    rows[0]["dat_txn"] = uuid.uuid4()
    with pytest.raises(ValueError, match="dat_txn"):
        store.branch(new_id()).insert("events", rows)


def test_table_name_is_validated(store: ClickHouseStore, faker: Faker) -> None:
    with pytest.raises(ValueError, match="table"):
        store.branch(new_id()).insert("events; DROP TABLE x", events(faker, 1))


def test_stage_rejects_an_unknown_column(store: ClickHouseStore, faker: Faker) -> None:
    rows = [{**r, "colour": "red"} for r in events(faker, 1)]
    branch = store.branch(new_id())
    branch.insert("events", rows)
    with pytest.raises(ValueError, match="colour"):
        branch.stage(branch.dat_txn, TS)


def test_stage_rejects_a_table_without_transaction_columns(
    store: ClickHouseStore, faker: Faker
) -> None:
    store.execute("CREATE TABLE plain (id UUID) ENGINE = MergeTree ORDER BY id")
    branch = store.branch(new_id())
    branch.insert("plain", [{"id": uuid.uuid4()}])
    with pytest.raises(ValueError, match="dat_txn"):
        branch.stage(branch.dat_txn, TS)


def test_stage_rejects_an_oversized_batch(clickhouse_dsn: str, faker: Faker) -> None:
    s = ClickHouseStore(clickhouse_dsn, name="ch", max_rows_per_table=2)
    s.execute(EVENTS)
    branch = s.branch(new_id())
    branch.insert("events", events(faker, 3))
    with pytest.raises(ValueError, match="max_rows_per_table"):
        branch.stage(branch.dat_txn, TS)
    s.close()


def test_stage_fails_for_a_missing_table(store: ClickHouseStore, faker: Faker) -> None:
    branch = store.branch(new_id())
    branch.insert("nope", [{"id": 1}])
    with pytest.raises(Exception, match="nope") as caught:
        branch.stage(branch.dat_txn, TS)
    assert not branch.is_retryable(caught.value)


def test_rollback_drops_the_buffer(store: ClickHouseStore, faker: Faker) -> None:
    branch = store.branch(new_id())
    branch.insert("events", events(faker, 2))
    branch.rollback()
    branch.stage(branch.dat_txn, TS)
    branch.commit_one_phase()
    assert count(store) == 0


def test_an_empty_branch_commits_nothing_but_its_marker(store: ClickHouseStore) -> None:
    branch = store.branch(new_id())
    branch.stage(branch.dat_txn, TS)
    branch.commit_one_phase()
    assert count(store) == 0
    assert store.participant().is_applied(record(branch, None), branch.dat_txn)


def test_branch_is_not_two_phase(store: ClickHouseStore) -> None:
    branch = store.branch(new_id())
    assert branch.two_phase is False
    with pytest.raises(TypeError, match="two-phase"):
        branch.prepare(gid_for(branch.dat_txn, "ch"))


# --- participant: finish after the decision ---------------------------------------------


def staged(store: ClickHouseStore, faker: Faker, n: int = 3) -> tuple[ClickHouseBranch, bytes]:
    branch = store.branch(new_id())
    branch.insert("events", events(faker, n))
    payload = branch.stage(branch.dat_txn, TS)
    assert payload is not None
    return branch, payload


def test_finish_inserts_from_the_journal_payload(store: ClickHouseStore, faker: Faker) -> None:
    branch, payload = staged(store, faker)
    participant = store.participant()
    assert not participant.is_applied(record(branch, payload), branch.dat_txn)
    participant.finish(record(branch, payload), branch.dat_txn, TS)
    assert count(store) == 3
    assert participant.is_applied(record(branch, payload), branch.dat_txn)


def test_finish_is_idempotent(store: ClickHouseStore, faker: Faker) -> None:
    branch, payload = staged(store, faker)
    for _ in range(3):
        store.participant().finish(record(branch, payload), branch.dat_txn, TS)
    assert count(store) == 3
    applied = store.execute("SELECT count() AS n FROM dat_applied FINAL").one()["n"]
    assert applied == 1


def test_finish_completes_a_partial_insert(store: ClickHouseStore, faker: Faker) -> None:
    """A crash mid-insert can leave some parts (e.g. one partition) written: no duplicates."""
    branch, payload = staged(store, faker, n=4)
    [batch] = decode_batches(payload)
    store._client.insert("events", batch.rows[:1], column_names=list(batch.columns))
    store.participant().finish(record(branch, payload), branch.dat_txn, TS)
    assert count(store) == 4
    dupes = store.execute("SELECT count() - uniqExact(id) AS d FROM events").one()["d"]
    assert dupes == 0


def test_finish_skips_tables_already_written(store: ClickHouseStore, faker: Faker) -> None:
    """Recovery after a crash between tables: the done table is not written twice."""
    store.execute(EVENTS.replace("events", "audit"))
    branch = store.branch(new_id())
    branch.insert("events", events(faker, 2))
    branch.insert("audit", events(faker, 1))
    payload = branch.stage(branch.dat_txn, TS)
    assert payload is not None
    first, _ = decode_batches(payload)
    store._client.insert("events", first.rows, column_names=list(first.columns))
    store.participant().finish(record(branch, payload), branch.dat_txn, TS)
    assert count(store) == 2
    assert store.execute("SELECT count() AS n FROM audit").one()["n"] == 1


def test_abort_and_prepared_gids_have_nothing_to_do(store: ClickHouseStore, faker: Faker) -> None:
    branch, payload = staged(store, faker)
    participant = store.participant()
    participant.abort(record(branch, payload))
    assert participant.prepared_gids() == []
    assert count(store) == 0


def test_transient_errors_are_retryable(store: ClickHouseStore) -> None:
    from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError

    too_many_parts = DatabaseError("Code: 252. DB::Exception: Too many parts")
    too_many_parts.code = 252
    syntax = DatabaseError("Code: 62. Syntax error")
    syntax.code = 62
    participant = store.participant()
    assert participant.is_retryable(OperationalError("connection refused"))
    assert participant.is_retryable(too_many_parts)
    assert participant.is_retryable(TimeoutError())
    assert not participant.is_retryable(syntax)
    assert not participant.is_retryable(ValueError("bad row"))


# --- error classification and capacity ---------------------------------------------------


def _server_error(code: int, name: str) -> Exception:
    from clickhouse_connect.driver.exceptions import DatabaseError

    return DatabaseError(f"Code: {code}. DB::Exception: ... ({name})", code=code, name=name)


@pytest.mark.parametrize(
    ("code", "name", "kind"),
    [
        (243, "NOT_ENOUGH_SPACE", ErrorKind.STORE_FULL),
        (469, "VIOLATED_CONSTRAINT", ErrorKind.CONSTRAINT),
        (252, "TOO_MANY_PARTS", ErrorKind.TRANSIENT),
        (60, "UNKNOWN_TABLE", ErrorKind.PERMANENT),
    ],
)
def test_errors_are_classified_by_server_code(
    store: ClickHouseStore, code: int, name: str, kind: ErrorKind
) -> None:
    error = _server_error(code, name)
    branch = store.branch(new_id())
    assert branch.classify(error) is kind
    assert store.participant().classify(error) is kind
    assert branch.is_retryable(error) is kind.retryable


def test_a_real_constraint_violation_is_a_constraint(store: ClickHouseStore) -> None:
    store.execute(
        "CREATE TABLE checked (n Int64, dat_txn UUID, dat_tx_time Int64,"
        " CONSTRAINT positive CHECK n > 0) ENGINE = MergeTree ORDER BY n"
    )
    branch = store.branch(new_id())
    branch.insert("checked", [{"n": -1}])
    branch.stage(branch.dat_txn, 1 << 16)
    with pytest.raises(Exception) as err:  # noqa: PT011 - the driver's own error class
        branch.commit_one_phase()
    assert branch.classify(err.value) is ErrorKind.CONSTRAINT


def test_free_space_is_the_least_free_disk(store: ClickHouseStore) -> None:
    assert isinstance(store, Capacity)
    free = store.free_space()
    disks = store.execute("SELECT free_space FROM system.disks").all()
    assert free is not None
    # Other writers change it between the two queries: the same disk, not the same byte.
    assert abs(free - min(int(d["free_space"]) for d in disks)) < 1 << 30
