"""Identity and transaction time across hosts with skewed wall clocks."""

from dataclasses import dataclass, field
from uuid import UUID

import pytest
from faker import Faker

from datorium.core.clock import ClockSkewError, HLCTimestamp, HybridLogicalClock
from datorium.core.identity import IdGenerator, is_uuid7, timestamp_ms


class Wall:
    def __init__(self, ms: int) -> None:
        self.ms = ms

    def __call__(self) -> int:
        return self.ms


@dataclass
class Host:
    """One application process: its own wall clock, id generator and HLC."""

    name: str
    wall: Wall
    ids: IdGenerator = field(init=False)
    clock: HybridLogicalClock = field(init=False)

    def __post_init__(self) -> None:
        self.ids = IdGenerator(clock_ms=self.wall)
        self.clock = HybridLogicalClock(wall_ms=self.wall, max_offset_ms=1_000)


@dataclass(frozen=True)
class Record:
    dat_id: UUID
    dat_txn: UUID
    dat_tx_time: int  # encoded HLC, as stored in a BIGINT / Int64 column
    body: str


def write(host: Host, body: str, *, dat_id: UUID | None = None) -> Record:
    return Record(
        dat_id=dat_id or host.ids(),
        dat_txn=host.ids(),
        dat_tx_time=host.clock.now().encode(),
        body=body,
    )


def receive(host: Host, record: Record) -> None:
    host.clock.update(HLCTimestamp.decode(record.dat_tx_time))


def test_bug_tracker_comment_orders_after_the_issue_despite_a_slow_clock(
    faker: Faker,
) -> None:
    # Replica B's wall clock is 300 ms behind replica A's.
    a = Host("a", Wall(1_760_000_000_000))
    b = Host("b", Wall(1_760_000_000_000 - 300))

    issue = write(a, faker.sentence())
    receive(b, issue)
    comment = write(b, faker.paragraph(), dat_id=issue.dat_id)
    receive(a, comment)
    a.wall.ms += 1
    status_change = write(a, "closed", dat_id=issue.dat_id)

    history = sorted([status_change, comment, issue], key=lambda r: r.dat_tx_time)
    assert history == [issue, comment, status_change]
    assert all(is_uuid7(r.dat_txn) for r in history)
    assert len({r.dat_txn for r in history}) == 3


def test_it_task_manager_ids_sort_by_creation_on_one_host(faker: Faker) -> None:
    host = Host("api", Wall(1_760_000_000_000))
    tasks = []
    for i in range(300):
        host.wall.ms += i % 3  # bursts within one millisecond
        tasks.append((host.ids(), faker.catch_phrase()))
    assert [t for t, _ in tasks] == sorted(t for t, _ in tasks)
    assert timestamp_ms(tasks[-1][0]) >= timestamp_ms(tasks[0][0])


def test_cbt_journal_sync_from_a_device_far_ahead_is_refused(faker: Faker) -> None:
    laptop = Host("laptop", Wall(1_760_000_000_000))
    phone = Host("phone", Wall(1_760_000_000_000 + 10 * 60_000))  # 10 minutes ahead

    laptop_entry = write(laptop, faker.paragraph())
    phone_entry = write(phone, faker.paragraph())

    with pytest.raises(ClockSkewError):
        receive(laptop, phone_entry)
    # The refused timestamp did not move the laptop's clock.
    assert laptop.clock.last == HLCTimestamp.decode(laptop_entry.dat_tx_time)
    assert write(laptop, faker.sentence()).dat_tx_time > laptop_entry.dat_tx_time


def test_erp_ledger_merged_from_three_hosts_keeps_causal_order(faker: Faker) -> None:
    hosts = [
        Host(n, Wall(1_760_000_000_000 + skew))
        for n, skew in [("eu", 0), ("us", -250), ("ap", 400)]
    ]
    ledger: list[Record] = []
    for step in range(60):
        host = hosts[step % 3]
        if ledger:
            receive(host, ledger[-1])  # each posting is made after seeing the previous one
        host.wall.ms += faker.random_int(0, 2)
        ledger.append(write(host, f"posting {step}"))

    assert [r.dat_tx_time for r in ledger] == sorted(r.dat_tx_time for r in ledger)
    assert len({r.dat_tx_time for r in ledger}) == len(ledger)
