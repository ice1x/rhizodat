"""Scenario: monitoring and managing cloud resources over all four stores, on either facade.

A cloud resource (a network, a database, a service, a load balancer) is one entity
in two stores: its inventory record (kind, name, region, size, owner) in
PostgreSQL, its dependencies in the drevo graph, where "depends on" is an edge to
the resource it needs. Around it, two application tables carry what a control
plane produces in bulk: the telemetry (latency, errors, requests per minute) in
ClickHouse, where it is aggregated, and every raw API request the control plane
made (method, path, status, payload) in an Iceberg lake, the audit trail.

The scenario runs end to end the way an operator's tooling would: migrate the
schema (SQL, Cypher and Iceberg steps), provision a stack in one transaction
together with its API requests (an interrupted provisioning leaves neither
resources nor requests), ask the graph for the blast radius of a failing
database and confirm against the latest versions, find the unhealthy resource
natively in ClickHouse, scale it with its API request in one transaction and
read the configuration as of before the incident, audit a resource's requests
natively in the lake, decommission a load balancer and bring it back, and run
the same estate from both facades in turn.

The same scenario runs on the Python facade and on the native facade (the core
in Rust under the same API), so both must agree.
"""

# @entity adds dat_id and the service times at runtime; mypy cannot see them.
# mypy: disable-error-code="attr-defined,call-arg"

import contextlib
import json
import time
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import pyarrow as pa
import pytest
from faker import Faker

from rhizodat import Ref, Rhizodat, UnitOfWork, entity
from rhizodat.adapters.drevo import ANCHOR, DrevoStore
from rhizodat.adapters.iceberg import IcebergStore
from rhizodat.core.native_uow import NativeUnitOfWork
from rhizodat.core.parts import Part
from rhizodat.migrations import Migration, Step


@entity(
    stores={
        "inventory": ["kind", "name", "region", "size", "owner"],
        "topology": ["depends_on"],
    },
    name="ops.Resource",
    soft_delete=timedelta(days=30),
)
class Resource:
    kind: str  # network | database | service | balancer
    name: str
    region: str
    size: int  # instances
    owner: str
    depends_on: list[Ref["Resource"]]


RESOURCE_PG = Part.of(Resource, "inventory").table
RESOURCE_GRAPH = Part.of(Resource, "topology").table
SEED_LABEL = "OpsCloudSeed"
METRICS = "ops_metric"
REQUESTS = "api_request"


def schema(namespace: str) -> list[Migration]:
    """The lake table lives in the test's own namespace, so the schema names it."""
    return [
        Migration.of(
            1,
            "estate",
            [
                Step(
                    "inventory",
                    "sql",
                    "CREATE TABLE ops_region (code text PRIMARY KEY, provider text NOT NULL)",
                ),
                Step(
                    "inventory",
                    "sql",
                    "INSERT INTO ops_region VALUES ('eu-west-1', 'aws'), ('us-east-1', 'aws')",
                ),
                Step(
                    "telemetry",
                    "sql",
                    f"CREATE TABLE {METRICS} (resource_id String, minute Int64,"
                    " latency_ms UInt32, errors UInt32, requests UInt32,"
                    " dat_txn UUID, dat_tx_time Int64,"
                    " INDEX dat_txn_idx dat_txn TYPE bloom_filter GRANULARITY 1)"
                    " ENGINE = MergeTree ORDER BY (resource_id, minute)",
                ),
                Step("topology", "cypher", f"CREATE (:{SEED_LABEL} {{provider: 'aws'}})"),
                Step(
                    "lake",
                    "iceberg",
                    [
                        {"op": "create_namespace", "namespace": namespace},
                        {
                            "op": "create_table",
                            "table": f"{namespace}.{REQUESTS}",
                            "columns": [
                                {"name": "request_id", "type": "string"},
                                {"name": "resource_id", "type": "string"},
                                {"name": "method", "type": "string"},
                                {"name": "path", "type": "string"},
                                {"name": "status", "type": "long"},
                                {"name": "at_ms", "type": "long"},
                                {"name": "payload", "type": "string"},
                                {"name": "dat_txn", "type": "string"},
                                {"name": "dat_tx_time", "type": "long"},
                            ],
                        },
                    ],
                ),
            ],
            description="regions, the telemetry table, the provider node, the request log",
        )
    ]


@pytest.fixture
def journal_schema(pg_dsn: str) -> Iterator[str]:
    name = f"dat_journal_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{name}"')
    yield name
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA "{name}" CASCADE')


@pytest.fixture
def namespace(iceberg_rest_dsn: str) -> Iterator[str]:
    """The lake outlives the test: a namespace of its own, purged afterwards."""
    name = f"dat_ops_{uuid.uuid4().hex[:10]}"
    yield name
    cleaner = IcebergStore(iceberg_rest_dsn, name="cleaner")
    try:
        with contextlib.suppress(Exception):
            for ident in cleaner.catalog.list_tables(name):
                cleaner.catalog.purge_table(ident)
            cleaner.catalog.drop_namespace(name)
    finally:
        cleaner.close()


@pytest.fixture(autouse=True)
def _clean_graph(drevo_bolt_dsn: str) -> Iterator[None]:
    """The graph is shared: remove this run's versions, seed and bare anchors."""
    yield
    cleaner = DrevoStore(drevo_bolt_dsn, name="cleaner")
    try:
        with contextlib.suppress(Exception):
            cleaner.execute(f"MATCH (v:{RESOURCE_GRAPH}) DETACH DELETE v")
            cleaner.execute(f"MATCH (s:{SEED_LABEL}) DELETE s")
            cleaner.execute(f"MATCH (a:{ANCHOR}) WHERE NOT (a)<--() DELETE a")
    finally:
        cleaner.close()


def python_facade(
    pg_dsn: str, pg_schema: str, journal_schema: str, ch_dsn: str, bolt: str, lake_dsn: str, ns: str
) -> Rhizodat:
    from rhizodat.adapters.clickhouse import ClickHouseStore
    from rhizodat.adapters.postgres import PostgresJournal, PostgresStore

    telemetry = ClickHouseStore(ch_dsn, name="telemetry")
    telemetry.install()
    return Rhizodat(
        stores={
            "inventory": PostgresStore(pg_dsn, name="inventory", schema=pg_schema),
            "topology": DrevoStore(bolt, name="topology"),
            "telemetry": telemetry,
            "lake": IcebergStore(lake_dsn, name="lake", parts_namespace=ns),
        },
        journal=PostgresJournal(pg_dsn, schema=journal_schema),
    )


def native_facade(
    pg_dsn: str, pg_schema: str, journal_schema: str, ch_dsn: str, bolt: str, lake_dsn: str, ns: str
) -> Rhizodat:
    native = pytest.importorskip("rhizodat_native")
    return Rhizodat(
        stores={
            "inventory": native.PostgresStore(pg_dsn, "inventory", pg_schema),
            "topology": native.DrevoStore(bolt, "topology"),
            "telemetry": native.ClickHouseStore(ch_dsn, "telemetry"),
            "lake": native.IcebergStore(lake_dsn, "lake", parts_namespace=ns),
        },
        journal=native.PostgresJournal(pg_dsn, journal_schema),
    )


FACADES: dict[str, Callable[..., Rhizodat]] = {"python": python_facade, "native": native_facade}


@pytest.fixture
def estate(
    pg_dsn: str,
    pg_schema: str,
    journal_schema: str,
    clickhouse_dsn: str,
    drevo_bolt_dsn: str,
    iceberg_rest_dsn: str,
    namespace: str,
) -> tuple[str, ...]:
    return (
        pg_dsn,
        pg_schema,
        journal_schema,
        clickhouse_dsn,
        drevo_bolt_dsn,
        iceberg_rest_dsn,
        namespace,
    )


@pytest.fixture(params=list(FACADES))
def dat(request: pytest.FixtureRequest, estate: tuple[str, ...]) -> Iterator[Rhizodat]:
    d = FACADES[request.param](*estate)
    d.migrate(schema(estate[-1]))
    yield d
    d.close()


# --- the control plane --------------------------------------------------------------------


def record_request(
    uow: UnitOfWork, ns: str, resource: Resource, method: str, status: int, payload: Any
) -> None:
    """The raw API request goes to the lake in the same transaction as its effect."""
    path = f"/v1/{resource.kind}s/{resource.dat_id}"
    row = {
        "request_id": str(uuid.uuid4()),
        "resource_id": str(resource.dat_id),
        "method": method,
        "path": path,
        "status": status,
        "at_ms": time.time_ns() // 1_000_000,
        "payload": json.dumps(payload, sort_keys=True),
    }
    uow.store("lake").append(f"{ns}.{REQUESTS}", pa.Table.from_pylist([row]))


def provision(
    dat: Rhizodat, ns: str, faker: Faker, *, fail_after: int | None = None
) -> dict[str, Resource]:
    """network <- database <- service <- balancer, in one transaction with a POST each."""
    owner = faker.user_name()
    stack: dict[str, Resource] = {}

    def resource(kind: str, size: int, needs: list[str]) -> Resource:
        return Resource(
            kind=kind,
            name=f"{kind}-{faker.word()}",
            region="eu-west-1",
            size=size,
            owner=owner,
            depends_on=[Ref.to(stack[n]) for n in needs],
        )

    stack["network"] = resource("network", 1, [])
    stack["database"] = resource("database", 1, ["network"])
    stack["service"] = resource("service", 2, ["network", "database"])
    stack["balancer"] = resource("balancer", 1, ["service"])

    @dat.transactional
    def create(uow: UnitOfWork) -> None:
        repo = uow.repository(Resource)
        for i, r in enumerate(stack.values()):
            if fail_after is not None and i == fail_after:
                raise RuntimeError("provisioning interrupted")
            repo.add(r)
            record_request(uow, ns, r, "POST", 201, {"name": r.name, "size": r.size})

    create()
    return stack


def scale(dat: Rhizodat, ns: str, resource: Resource, size: int) -> None:
    @dat.transactional
    def patch(uow: UnitOfWork) -> None:
        resource.size = size
        uow.repository(Resource).update(resource)
        record_request(uow, ns, resource, "PATCH", 200, {"size": size})

    patch()


def ingest(dat: Rhizodat, samples: list[dict[str, Any]]) -> None:
    """A batch of telemetry rows; ClickHouse joins one-phase, rows in the journal."""

    @dat.transactional
    def batch(uow: UnitOfWork) -> None:
        sink = uow.store("telemetry")
        if isinstance(uow, NativeUnitOfWork):
            columns = list(samples[0])
            sink.insert(METRICS, columns, [[s[c] for c in columns] for s in samples])
        else:
            sink.insert(METRICS, samples)

    batch()


def an_incident(stack: dict[str, Resource]) -> list[dict[str, Any]]:
    """Ten minutes: the service fails a fifth of its requests, the database is fine."""
    samples = []
    for minute in range(10):
        for kind, latency, errors in (("service", 900, 40), ("database", 12, 0)):
            samples.append(
                {
                    "resource_id": str(stack[kind].dat_id),
                    "minute": minute,
                    "latency_ms": latency,
                    "errors": errors,
                    "requests": 200,
                }
            )
    return samples


# --- the operator's questions ---------------------------------------------------------------


def dependents(dat: Rhizodat, of: Resource) -> set[uuid.UUID]:
    """Who depends on ``of`` now: the graph names every version that ever pointed at it,
    the latest version of each confirms (a decommissioned one has none)."""
    rows = (
        dat.store("topology")
        .execute(
            f"MATCH (v:{RESOURCE_GRAPH})-[:depends_on]->(a:{ANCHOR} {{dat_id: $of}})"
            " RETURN DISTINCT v.dat_id AS d",
            {"of": str(of.dat_id)},
        )
        .all()
    )
    repo = dat.repository(Resource)
    confirmed = set()
    for row in rows:
        latest = repo.get(uuid.UUID(str(row["d"])))
        if latest and any(r.dat_id == of.dat_id for r in latest.depends_on):
            confirmed.add(latest.dat_id)
    return confirmed


def blast_radius(dat: Rhizodat, failing: Resource) -> set[uuid.UUID]:
    """Everything that depends on ``failing``, directly or through others."""
    repo = dat.repository(Resource)
    seen: set[uuid.UUID] = set()
    frontier = [failing]
    while frontier:
        for d in dependents(dat, frontier.pop()) - seen:
            seen.add(d)
            found = repo.get(d)
            assert found is not None
            frontier.append(found)
    return seen


def unhealthy(dat: Rhizodat, *, max_error_rate: float) -> dict[str, float]:
    """Native ClickHouse: the error rate per resource over the whole window."""
    rows = (
        dat.store("telemetry")
        .execute(
            "SELECT resource_id, sum(errors) / sum(requests) AS rate"
            f" FROM {METRICS} GROUP BY resource_id"
            " HAVING rate > {limit:Float64} ORDER BY resource_id",
            {"limit": max_error_rate},
        )
        .all()
    )
    return {str(r["resource_id"]): float(r["rate"]) for r in rows}


def requests_of(dat: Rhizodat, ns: str, resource: Resource) -> list[tuple[str, int]]:
    """Native lake scan: the resource's raw requests, oldest first."""
    rows = (
        dat.store("lake")
        .execute(
            f"{ns}.{REQUESTS}",
            {
                "row_filter": f"resource_id == '{resource.dat_id}'",
                "selected_fields": ["method", "status", "at_ms", "dat_tx_time"],
            },
        )
        .all()
    )
    ordered = sorted(rows, key=lambda r: (int(r["dat_tx_time"]), int(r["at_ms"])))
    return [(str(r["method"]), int(r["status"])) for r in ordered]


def all_requests(dat: Rhizodat, ns: str) -> int:
    return len(dat.store("lake").execute(f"{ns}.{REQUESTS}").all())


def inventory_size(dat: Rhizodat) -> int:
    row = dat.store("inventory").execute(f"SELECT count(DISTINCT dat_id) AS n FROM {RESOURCE_PG}")
    return int(row.one()["n"])


# --- the scenario ---------------------------------------------------------------------------


def test_the_schema_is_migrated_once_across_the_four_stores(dat: Rhizodat, namespace: str) -> None:
    assert [m.id for m in dat.schema_versions()] == [1]
    report = dat.migrate(schema(namespace))
    assert (report.applied, report.already) == ((), (1,))
    seeds = dat.store("topology").execute(f"MATCH (s:{SEED_LABEL}) RETURN s.provider AS p").all()
    assert [r["p"] for r in seeds] == ["aws"]
    assert all_requests(dat, namespace) == 0


def test_a_stack_is_provisioned_with_its_requests_or_not_at_all(
    dat: Rhizodat, namespace: str, faker: Faker
) -> None:
    with pytest.raises(RuntimeError, match="interrupted"):
        provision(dat, namespace, faker, fail_after=3)
    assert (inventory_size(dat), all_requests(dat, namespace)) == (0, 0)

    stack = provision(dat, namespace, faker)
    repo = dat.repository(Resource)
    for r in stack.values():
        assert repo.get(r.dat_id) == r
    assert (inventory_size(dat), all_requests(dat, namespace)) == (4, 4)
    assert requests_of(dat, namespace, stack["service"]) == [("POST", 201)]


def test_the_blast_radius_of_a_failing_database_comes_from_the_graph(
    dat: Rhizodat, namespace: str, faker: Faker
) -> None:
    stack = provision(dat, namespace, faker)
    assert blast_radius(dat, stack["database"]) == {
        stack["service"].dat_id,
        stack["balancer"].dat_id,
    }
    assert blast_radius(dat, stack["network"]) == {
        stack["database"].dat_id,
        stack["service"].dat_id,
        stack["balancer"].dat_id,
    }
    assert blast_radius(dat, stack["balancer"]) == set()


def test_the_unhealthy_service_is_found_scaled_and_audited(
    dat: Rhizodat, namespace: str, faker: Faker
) -> None:
    stack = provision(dat, namespace, faker)
    service = stack["service"]
    before_incident = datetime.now(UTC)
    time.sleep(0.01)
    ingest(dat, an_incident(stack))

    assert unhealthy(dat, max_error_rate=0.05) == {str(service.dat_id): 0.2}

    scale(dat, namespace, service, 4)
    repo = dat.repository(Resource)
    now = repo.get(service.dat_id)
    assert now is not None
    assert now.size == 4
    then = repo.get(service.dat_id, as_of=before_incident)
    assert then is not None
    assert then.size == 2
    assert [v.size for v in repo.history(service.dat_id)] == [2, 4]
    assert requests_of(dat, namespace, service) == [("POST", 201), ("PATCH", 200)]
    assert requests_of(dat, namespace, stack["database"]) == [("POST", 201)]


def test_a_decommissioned_balancer_leaves_the_graph_and_comes_back(
    dat: Rhizodat, namespace: str, faker: Faker
) -> None:
    stack = provision(dat, namespace, faker)
    balancer = stack["balancer"]
    repo = dat.repository(Resource)

    repo.delete(balancer.dat_id)
    assert repo.get(balancer.dat_id) is None
    assert blast_radius(dat, stack["database"]) == {stack["service"].dat_id}
    gone = repo.get(balancer.dat_id, include_deleted=True)
    assert gone is not None
    assert gone.dat_deleted_at is not None

    repo.restore(balancer.dat_id)
    assert blast_radius(dat, stack["database"]) == {
        stack["service"].dat_id,
        balancer.dat_id,
    }


def test_both_facades_run_the_same_estate(estate: tuple[str, ...], faker: Faker) -> None:
    """Provisioned on the Python facade, operated natively, audited on both."""
    namespace = estate[-1]
    with python_facade(*estate) as python_side:
        python_side.migrate(schema(namespace))
        stack = provision(python_side, namespace, faker)
    service = stack["service"]
    with native_facade(*estate) as native_side:
        assert [m.id for m in native_side.schema_versions()] == [1]
        ingest(native_side, an_incident(stack))
        assert unhealthy(native_side, max_error_rate=0.05) == {str(service.dat_id): 0.2}
        scale(native_side, namespace, service, 3)
        assert blast_radius(native_side, stack["database"]) == {
            service.dat_id,
            stack["balancer"].dat_id,
        }
    with python_facade(*estate) as python_side:
        assert unhealthy(python_side, max_error_rate=0.05) == {str(service.dat_id): 0.2}
        assert [v.size for v in python_side.repository(Resource).history(service.dat_id)] == [2, 3]
        assert requests_of(python_side, namespace, service) == [("POST", 201), ("PATCH", 200)]
