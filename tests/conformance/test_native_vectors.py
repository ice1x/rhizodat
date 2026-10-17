"""The Rust core through its Python bindings (``rhizodat_native``) against the vectors.

The same files as the Python reference (``test_vectors.py``): UUIDv7, the
hybrid logical clock, branch gids and migration files. Install the module with
``uv sync --group native`` (needs a Rust toolchain).
"""

import json
import threading
import uuid
from pathlib import Path
from typing import Any

import pytest

native = pytest.importorskip("rhizodat_native")

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

    generate = native.id_generator(clock_ms=lambda: clock[0], randbits=randbits)
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
            assert native.is_uuid7(got)
            assert native.uuid7_ms(got) == got.int >> 80


def test_ids_from_the_system_are_shared_between_threads() -> None:
    generate = native.id_generator()
    made: list[uuid.UUID] = []

    def work() -> None:
        made.extend(generate() for _ in range(500))

    threads = [threading.Thread(target=work) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(set(made)) == 4000
    assert all(native.is_uuid7(i) for i in made)
    first, second = native.new_id(), native.new_id()
    assert first < second
    assert native.uuid7_ms(uuid.UUID(int=1)) is None


def test_a_failing_callback_raises_its_own_error() -> None:
    def broken() -> int:
        raise ZeroDivisionError("clock broke")

    with pytest.raises(ZeroDivisionError, match="clock broke"):
        native.id_generator(clock_ms=broken)()
    with pytest.raises(TypeError):
        native.id_generator(randbits=lambda bits: "x")()  # not an int
    with pytest.raises(ZeroDivisionError, match="clock broke"):
        native.hybrid_logical_clock(wall_ms=broken).now()


def test_injected_sources_stay_on_the_thread_that_made_them() -> None:
    generate = native.id_generator(clock_ms=lambda: 1)
    caught: list[BaseException] = []

    def elsewhere() -> None:
        try:
            generate()
        except BaseException as exc:  # PyO3 reports it as a panic, not an Exception
            caught.append(exc)

    t = threading.Thread(target=elsewhere)
    t.start()
    t.join()
    [error] = caught
    assert "unsendable" in str(error)


# --- HLC -------------------------------------------------------------------------------

HLC_CASES = load("hlc.json")["cases"]
HLC_ERRORS = {"clock_skew": native.ClockSkewError, "logical_overflow": native.LogicalOverflowError}


@pytest.mark.parametrize("case", HLC_CASES, ids=ids(HLC_CASES))
def test_hlc(case: dict[str, Any]) -> None:
    wall = [0]
    clock = native.hybrid_logical_clock(
        wall_ms=lambda: wall[0], max_offset_ms=case["max_offset_ms"]
    )
    for step in case["steps"]:
        wall[0] = step["wall_ms"]
        expect = step["expect"]
        remote = None
        if step["op"] != "now":
            remote = (step["remote"]["wall_ms"] << 16) | step["remote"]["logical"]

        def call(remote: int | None = remote) -> int:
            return int(clock.now() if remote is None else clock.update(remote))

        if "error" in expect:
            before = clock.last
            with pytest.raises(HLC_ERRORS[expect["error"]]):
                call()
            if expect["error"] == "clock_skew":
                assert clock.last == before  # a rejected receive changes nothing
            continue
        got = call()
        assert got == expect["encoded"]
        assert (got >> 16, got & 0xFFFF) == (expect["wall_ms"], expect["logical"])
        assert clock.last == got


def test_the_system_clock_moves_forward_and_refuses_bad_input() -> None:
    clock = native.hybrid_logical_clock()
    assert clock.last is None
    first = clock.now()
    assert clock.update(first) > first
    with pytest.raises(ValueError, match="out of range"):
        clock.update(-1)
    with pytest.raises(ValueError, match="out of range"):
        native.hybrid_logical_clock(wall_ms=lambda: -5).now()


# --- gid -------------------------------------------------------------------------------

GID = load("gid.json")


@pytest.mark.parametrize("case", GID["gid_for"], ids=ids(GID["gid_for"], "gid"))
def test_gid_for(case: dict[str, Any]) -> None:
    assert native.gid_for(uuid.UUID(case["dat_txn"]), case["store"]) == case["gid"]


@pytest.mark.parametrize("case", GID["parse"], ids=ids(GID["parse"], "gid"))
def test_parse_gid(case: dict[str, Any]) -> None:
    parsed = native.parse_gid(case["gid"])
    expect = case["result"]
    if expect is None:
        assert parsed is None
    else:
        assert parsed == (uuid.UUID(expect["dat_txn"]), expect["store"])


# --- migrations ------------------------------------------------------------------------

MIGRATIONS = load("migrations.json")


@pytest.mark.parametrize("case", MIGRATIONS["cases"], ids=lambda c: c["name"])
def test_migration_files(case: dict[str, Any]) -> None:
    m = native.parse_migration(case["filename"], case["text"])
    assert (m["id"], m["name"], m["description"]) == (
        case["id"],
        case["migration_name"],
        case["description"],
    )
    assert m["steps"] == case["steps"]
    assert m["canonical"] == case["canonical"]
    assert m["checksum"] == case["checksum"]


@pytest.mark.parametrize("case", MIGRATIONS["errors"], ids=lambda c: c["name"])
def test_migration_files_rejected(case: dict[str, Any]) -> None:
    with pytest.raises(native.MigrationFormatError):
        native.parse_migration(case["filename"], case["text"])
    assert issubclass(native.MigrationFormatError, ValueError)
