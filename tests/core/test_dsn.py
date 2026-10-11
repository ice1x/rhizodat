"""Connection strings for stores and the journal (README "Connection strings")."""

from urllib.parse import quote

import pytest
from hypothesis import given
from hypothesis import strategies as st

from datorium.core.dsn import DSN, DSNError, parse_dsn

# --- PostgreSQL ------------------------------------------------------------


def test_postgresql_full() -> None:
    dsn = parse_dsn("postgresql://app:s3cret@db.local:6432/orders?sslmode=require")
    assert dsn.store == "postgres"
    assert dsn.transport is None
    assert (dsn.host, dsn.port) == ("db.local", 6432)
    assert (dsn.user, dsn.password) == ("app", "s3cret")
    assert dsn.database == "orders"
    assert dsn.params == {"sslmode": "require"}


def test_postgres_alias_and_default_port() -> None:
    dsn = parse_dsn("postgres://localhost/app")
    assert dsn.store == "postgres"
    assert dsn.port == 5432


def test_postgresql_unix_socket_has_no_host() -> None:
    dsn = parse_dsn("postgresql:///app?host=/var/run/postgresql")
    assert dsn.host is None
    assert dsn.port is None
    assert dsn.database == "app"
    assert dsn.params["host"] == "/var/run/postgresql"


# --- ClickHouse ------------------------------------------------------------


def test_clickhouse_default_http_port() -> None:
    dsn = parse_dsn("clickhouse://localhost/analytics")
    assert dsn.store == "clickhouse"
    assert (dsn.host, dsn.port, dsn.database) == ("localhost", 8123, "analytics")


def test_clickhouse_requires_a_host() -> None:
    with pytest.raises(DSNError, match="host"):
        parse_dsn("clickhouse:///analytics")


# --- drevo -----------------------------------------------------------------


def test_drevo_embedded_file() -> None:
    dsn = parse_dsn("drevo:///data/graph.drevo")
    assert (dsn.store, dsn.transport) == ("drevo", "embedded")
    assert dsn.path == "/data/graph.drevo"
    assert dsn.host is None


def test_drevo_embedded_path_must_be_absolute_and_local() -> None:
    with pytest.raises(DSNError, match="drevo:///"):
        parse_dsn("drevo://data/graph.drevo")


def test_drevo_embedded_needs_a_path() -> None:
    with pytest.raises(DSNError, match="path"):
        parse_dsn("drevo://")


def test_drevo_in_memory() -> None:
    dsn = parse_dsn("drevo+mem://")
    assert (dsn.store, dsn.transport) == ("drevo", "memory")
    assert dsn.path is None


def test_drevo_in_memory_takes_no_location() -> None:
    with pytest.raises(DSNError, match="drevo\\+mem://"):
        parse_dsn("drevo+mem:///tmp/x.drevo")


def test_drevo_bolt_default_port() -> None:
    dsn = parse_dsn("drevo+bolt://neo:pw@graph.local")
    assert (dsn.store, dsn.transport) == ("drevo", "bolt")
    assert (dsn.host, dsn.port, dsn.user, dsn.password) == ("graph.local", 7687, "neo", "pw")


# --- Iceberg ---------------------------------------------------------------


@pytest.mark.parametrize("catalog", ["rest", "sql", "glue"])
def test_iceberg_catalog_kinds(catalog: str) -> None:
    dsn = parse_dsn(f"iceberg+{catalog}://catalog.local:8181/warehouse")
    assert (dsn.store, dsn.transport) == ("iceberg", catalog)
    assert dsn.database == "warehouse"


def test_iceberg_needs_a_catalog_kind() -> None:
    with pytest.raises(DSNError, match="iceberg\\+rest"):
        parse_dsn("iceberg://catalog.local/warehouse")


def test_iceberg_rest_requires_a_host() -> None:
    with pytest.raises(DSNError, match="host"):
        parse_dsn("iceberg+rest:///warehouse")


# --- file (development journal) --------------------------------------------


def test_file_dsn() -> None:
    dsn = parse_dsn("file:///var/lib/datorium/journal.db")
    assert dsn.store == "file"
    assert dsn.path == "/var/lib/datorium/journal.db"


def test_file_dsn_must_be_local() -> None:
    with pytest.raises(DSNError, match="file:///"):
        parse_dsn("file://server/journal.db")


# --- common rules ----------------------------------------------------------


@pytest.mark.parametrize(
    "raw", ["mysql://localhost/app", "drevo+grpc://x", "iceberg+hive://x/w", "localhost:5432"]
)
def test_unknown_schemes_are_rejected_with_the_supported_list(raw: str) -> None:
    with pytest.raises(DSNError, match=r"supported: .*postgresql"):
        parse_dsn(raw)


def test_scheme_is_case_insensitive() -> None:
    assert parse_dsn("PostgreSQL://localhost/app").store == "postgres"


@pytest.mark.parametrize(
    "raw", ["postgresql://h:0/db", "postgresql://h:70000/db", "postgresql://h:x/db"]
)
def test_bad_ports_are_rejected(raw: str) -> None:
    with pytest.raises(DSNError, match="port"):
        parse_dsn(raw)


def test_repeated_query_parameter_is_rejected() -> None:
    with pytest.raises(DSNError, match="sslmode"):
        parse_dsn("postgresql://h/db?sslmode=require&sslmode=disable")


def test_empty_string_is_rejected() -> None:
    with pytest.raises(DSNError):
        parse_dsn("")


def test_ipv6_host() -> None:
    dsn = parse_dsn("postgresql://[::1]:5433/app")
    assert (dsn.host, dsn.port) == ("::1", 5433)


def test_percent_encoded_parts_are_decoded() -> None:
    dsn = parse_dsn("postgresql://us%40er:p%3Aw%2Fd@h/my%20db")
    assert (dsn.user, dsn.password, dsn.database) == ("us@er", "p:w/d", "my db")


@given(st.text(min_size=1, max_size=40), st.text(min_size=1, max_size=40))
def test_any_credentials_survive_percent_encoding(user: str, password: str) -> None:
    raw = f"postgresql://{quote(user, safe='')}:{quote(password, safe='')}@h/db"
    dsn = parse_dsn(raw)
    assert (dsn.user, dsn.password) == (user, password)


# --- secrets ---------------------------------------------------------------


def test_repr_and_str_never_show_the_password() -> None:
    dsn = parse_dsn("postgresql://app:s3cret@db/app?password=q1")
    for text in (repr(dsn), str(dsn), dsn.redacted()):
        assert "s3cret" not in text
        assert "q1" not in text
    assert dsn.redacted() == "postgresql://app:***@db/app?password=***"


def test_unredacted_string_is_available_explicitly() -> None:
    raw = "drevo+bolt://neo:pw@graph.local:7687"
    assert parse_dsn(raw).unredacted() == raw


def test_dsn_is_immutable_and_hashable() -> None:
    dsn = parse_dsn("clickhouse://localhost/a?secure=true")
    with pytest.raises(AttributeError):
        dsn.host = "other"  # type: ignore[misc]
    assert hash(dsn) == hash(parse_dsn("clickhouse://localhost/a?secure=true"))
    assert isinstance(dsn, DSN)
