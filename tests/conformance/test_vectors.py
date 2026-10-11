"""The Python reference against the language-neutral conformance vectors in ``conformance/``.

Every implementation (this one, the Rust core) must pass the same files, bit for
bit. The vectors are data, frozen in the repository: a change here is a
protocol change and needs a deliberate update of the JSON, not a regenerate.
"""

import json
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from datorium.core.clock import (
    ClockSkewError,
    HLCTimestamp,
    HybridLogicalClock,
    LogicalOverflowError,
)
from datorium.core.coordinator import gid_for, parse_gid
from datorium.core.dsn import DSNError, parse_dsn
from datorium.core.identity import IdGenerator
from datorium.core.journal import State, check_transition

VECTORS = Path(__file__).resolve().parents[2] / "conformance"


def load(name: str) -> dict[str, Any]:
    doc: dict[str, Any] = json.loads((VECTORS / name).read_text())
    return doc


def ids(cases: list[dict[str, Any]], key: str = "name") -> list[str]:
    return [str(c[key]) for c in cases]


# --- UUIDv7 ----------------------------------------------------------------------------

UUID_CASES = load("uuid7.json")["cases"]
UUID_ERRORS = {"invalid_clock": "does not fit", "timestamp_exhausted": "exhausted"}


@pytest.mark.parametrize("case", UUID_CASES, ids=ids(UUID_CASES))
def test_uuid7(case: dict[str, Any]) -> None:
    clock = [0]
    queue: list[dict[str, int]] = []

    def randbits(bits: int) -> int:
        assert queue, "the generator asked for more random values than the vector provides"
        want = queue.pop(0)
        assert want["bits"] == bits, f"asked for {bits} random bits, vector has {want['bits']}"
        return want["value"]

    generate = IdGenerator(clock_ms=lambda: clock[0], randbits=randbits)
    for step in case["steps"]:
        clock[0] = step["clock_ms"]
        for _ in range(step.get("repeat", 1)):
            queue[:] = step["random"]
            if "error" in step:
                with pytest.raises(ValueError, match=UUID_ERRORS[step["error"]]):
                    generate()
            else:
                got = generate()
            assert queue == [], "random values left unused"
        if "uuid" in step:
            assert got == uuid.UUID(step["uuid"])


# --- HLC -------------------------------------------------------------------------------

HLC_CASES = load("hlc.json")["cases"]
HLC_ERRORS: dict[str, type[Exception]] = {
    "clock_skew": ClockSkewError,
    "logical_overflow": LogicalOverflowError,
}


@pytest.mark.parametrize("case", HLC_CASES, ids=ids(HLC_CASES))
def test_hlc(case: dict[str, Any]) -> None:
    wall = [0]
    clock = HybridLogicalClock(wall_ms=lambda: wall[0], max_offset_ms=case["max_offset_ms"])
    for step in case["steps"]:
        wall[0] = step["wall_ms"]
        call: Callable[[], HLCTimestamp]
        if step["op"] == "now":
            call = clock.now
        else:
            remote = HLCTimestamp(step["remote"]["wall_ms"], step["remote"]["logical"])

            def call(remote: HLCTimestamp = remote) -> HLCTimestamp:
                return clock.update(remote)

        expect = step["expect"]
        if "error" in expect:
            before = clock.last
            with pytest.raises(HLC_ERRORS[expect["error"]]):
                call()
            if expect["error"] == "clock_skew":
                assert clock.last == before  # a rejected receive changes nothing
            continue
        ts = call()
        assert (ts.wall_ms, ts.logical, ts.encode()) == (
            expect["wall_ms"],
            expect["logical"],
            expect["encoded"],
        )
        assert HLCTimestamp.decode(expect["encoded"]) == ts


# --- gid -------------------------------------------------------------------------------

GID = load("gid.json")


@pytest.mark.parametrize("case", GID["gid_for"], ids=ids(GID["gid_for"], "gid"))
def test_gid_for(case: dict[str, Any]) -> None:
    assert gid_for(uuid.UUID(case["dat_txn"]), case["store"]) == case["gid"]


@pytest.mark.parametrize("case", GID["parse"], ids=ids(GID["parse"], "gid"))
def test_parse_gid(case: dict[str, Any]) -> None:
    parsed = parse_gid(case["gid"])
    expect = case["result"]
    if expect is None:
        assert parsed is None
    else:
        assert parsed == (uuid.UUID(expect["dat_txn"]), expect["store"])


# --- journal ---------------------------------------------------------------------------

JOURNAL = load("journal.json")


def test_journal_states_are_exactly_these() -> None:
    assert [s.value for s in State] == JOURNAL["states"]


@pytest.mark.parametrize(
    "edge", JOURNAL["transitions"], ids=[f"{e['from']}->{e['to']}" for e in JOURNAL["transitions"]]
)
def test_journal_transition(edge: dict[str, Any]) -> None:
    expected, new = State(edge["from"]), State(edge["to"])
    if edge["legal"]:
        check_transition(expected, new, 1)
    else:
        with pytest.raises(ValueError, match="illegal"):
            check_transition(expected, new, 1)


@pytest.mark.parametrize("state", JOURNAL["requires_dat_tx_time"])
def test_journal_state_requires_a_transaction_time(state: str) -> None:
    edge = next(e for e in JOURNAL["transitions"] if e["to"] == state and e["legal"])
    with pytest.raises(ValueError, match="dat_tx_time"):
        check_transition(State(edge["from"]), State(state), None)


# --- DSN -------------------------------------------------------------------------------

DSN_CASES = load("dsn.json")["cases"]


@pytest.mark.parametrize("case", DSN_CASES, ids=ids(DSN_CASES, "raw"))
def test_dsn(case: dict[str, Any]) -> None:
    expect = case["expect"]
    if expect.get("error"):
        with pytest.raises(DSNError):
            parse_dsn(case["raw"])
        return
    d = parse_dsn(case["raw"])
    got = {
        "store": d.store,
        "transport": d.transport,
        "host": d.host,
        "port": d.port,
        "user": d.user,
        "password": d.password,
        "path": d.path,
        "database": d.database,
        "params": dict(d.params),
        "redacted": d.redacted(),
    }
    assert got == expect


def test_every_vector_file_is_exercised() -> None:
    used = {"uuid7.json", "hlc.json", "gid.json", "journal.json", "dsn.json", "crash_matrix.json"}
    assert {p.name for p in VECTORS.glob("*.json")} == used


# --- crash matrix ----------------------------------------------------------------------

CRASH = load("crash_matrix.json")


class _Wall:
    def __init__(self) -> None:
        self.ms = 1_000_000

    def __call__(self) -> int:
        return self.ms


def steps(seen: list[str]) -> list[str]:
    return [point for point in seen if not point.endswith(".after")]


@pytest.mark.parametrize("case", CRASH["cases"], ids=[str(c["crash_at"]) for c in CRASH["cases"]])
def test_crash_matrix(case: dict[str, Any]) -> None:
    from datorium.core.coordinator import Committed, Coordinator, Recovery
    from datorium.core.identity import new_id
    from datorium.core.retry import Retry
    from fakes import Crash, FaultyJournal, World

    world = World()
    for store in CRASH["stores"]:
        world.store(store["name"], two_phase=store["two_phase"])
    wall = _Wall()
    journal = FaultyJournal(world.faults, wall)
    if case["crash_at"] is not None:
        world.faults.add(case["crash_at"], Crash())
    txn = new_id()
    coordinator = Coordinator(
        journal=journal,
        participants=world.participants(),
        clock=HybridLogicalClock(),
        finish=Retry(attempts=1, deadline_s=1.0, base_delay_s=0.0, max_delay_s=0.0),
        sleep=lambda s: None,
    )
    try:
        result = coordinator.commit(txn, world.branches())
        commit = "committed" if isinstance(result, Committed) else "committed_pending"
    except Crash:
        commit = "crashed"
    assert commit == case["commit"]

    def snapshot() -> dict[str, Any]:
        record = journal.get(txn)
        return {
            "journal": record.state.value if record else None,
            "prepared": sorted(world.leftover_prepared()),
            "applied": sorted(s for s, done in world.applied(txn).items() if done),
        }

    assert snapshot() == case["after_crash"]
    assert steps(world.faults.seen) == case["trace"]

    world.faults.plan.clear()
    world.faults.seen.clear()
    wall.ms += CRASH["stale_after_ms"] + 1
    report = Recovery(
        journal=journal,
        participants=world.participants(),
        stale_after_ms=CRASH["stale_after_ms"],
        wall_ms=wall,
    ).run_once()
    action = (
        "aborted" if txn in report.aborted else "finished" if txn in report.finished else "none"
    )
    assert action == case["recovery"]
    assert steps(world.faults.seen) == case["recovery_trace"]
    after = snapshot()
    everything = sorted(s["name"] for s in CRASH["stores"])
    assert after["applied"] in ([], everything), "recovery must end all-or-nothing"
    assert {
        "journal": after["journal"],
        "applied": "all" if after["applied"] else "none",
        "prepared": after["prepared"],
    } == case["after_recovery"]
