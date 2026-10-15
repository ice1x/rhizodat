"""The language-neutral migration format: one TOML file per migration.

``NNNN_name.toml`` (the number is the migration's id, at least 1; the name is
lower-case letters, digits and ``_``) holds an optional ``description`` and a
list of ``steps``. A step names the store it runs on (a store name of the
``Rhizodat`` configuration) and has exactly one body, keyed by its kind::

    description = "bugs and their graph index"

    [[steps]]
    store = "pg"
    sql = "CREATE TABLE IF NOT EXISTS bug (id uuid PRIMARY KEY, title text NOT NULL)"

    [[steps]]
    store = "graph"
    cypher = ["CREATE INDEX bug_title IF NOT EXISTS FOR (b:Bug) ON (b.title)"]

The kind (``sql``, ``cypher``, ``iceberg``, or a plug-in adapter's own) decides
which store can run the step; the format only checks its shape. A body is
strings, integers, booleans, arrays and tables: floats and dates have no single
text form across languages, so they are refused.

The checksum is the sha256 (hex) of the canonical form: the JSON of
``{"id", "name", "steps": [{"body", "kind", "store"}]}`` with keys sorted, no
whitespace and non-ASCII text as UTF-8 (``\\uXXXX`` escapes only for control
characters). The description is not part of it.
"""

import datetime as dt
import hashlib
import json
import re
import tomllib
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

__all__ = [
    "Migration",
    "MigrationFormatError",
    "Step",
    "canonical",
    "load_migrations",
    "parse_migration",
    "statements",
]

_FILE = re.compile(r"([0-9]+)_([a-z0-9_]+)\.toml")
_STORE = re.compile(r"[A-Za-z0-9_.-]{1,64}")
_KIND = re.compile(r"[a-z][a-z0-9_]{0,31}")


class MigrationFormatError(ValueError):
    """A migration file or directory that does not follow the format."""


@dataclass(frozen=True, slots=True)
class Step:
    store: str
    kind: str
    body: Any  # str | int | bool | list | dict, as TOML gives it


@dataclass(frozen=True, slots=True)
class Migration:
    id: int
    name: str
    steps: tuple[Step, ...]
    description: str | None = None
    checksum: str = ""

    @classmethod
    def of(
        cls, id: int, name: str, steps: Iterable[Step], description: str | None = None
    ) -> "Migration":
        """A migration built in code, checked like a file ``{id:04}_{name}.toml``."""
        return _build(f"{id:04}_{name}.toml", id, name, steps, description)

    @property
    def stores(self) -> tuple[str, ...]:
        """The stores its steps run on, in order of first appearance."""
        return tuple(dict.fromkeys(s.store for s in self.steps))


def _build(
    where: str, id: int, name: str, steps: Iterable[Step], description: str | None
) -> Migration:
    if id < 1:
        raise MigrationFormatError(f"{where}: the id must be at least 1")
    if not re.fullmatch(r"[a-z0-9_]+", name):
        raise MigrationFormatError(f"{where}: a name is lower-case letters, digits and _")
    checked = tuple(_step(where, i, s.store, s.kind, s.body) for i, s in enumerate(steps, 1))
    if not checked:
        raise MigrationFormatError(f"{where}: no steps")
    draft = Migration(id, name, checked, description)
    checksum = hashlib.sha256(canonical(draft).encode()).hexdigest()
    return Migration(id, name, checked, description, checksum)


def canonical(migration: Migration) -> str:
    """The text the checksum is taken of (see the module documentation)."""
    return json.dumps(
        {
            "id": migration.id,
            "name": migration.name,
            "steps": [{"body": s.body, "kind": s.kind, "store": s.store} for s in migration.steps],
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def parse_migration(filename: str, text: str) -> Migration:
    """The migration in ``text``, read from a file named ``filename``."""
    found = _FILE.fullmatch(filename)
    if not found:
        raise MigrationFormatError(
            f"{filename}: a migration file is named NNNN_name.toml"
            " (a number, then lower-case letters, digits and _)"
        )
    id, name = int(found.group(1)), found.group(2)
    if id < 1:
        raise MigrationFormatError(f"{filename}: the id must be at least 1")
    try:
        doc = tomllib.loads(text)
    except tomllib.TOMLDecodeError as exc:
        raise MigrationFormatError(f"{filename}: not valid TOML: {exc}") from None
    unknown = sorted(set(doc) - {"description", "steps"})
    if unknown:
        raise MigrationFormatError(f"{filename}: unknown key {', '.join(unknown)}")
    description = doc.get("description")
    if description is not None and not isinstance(description, str):
        raise MigrationFormatError(f"{filename}: description must be a string")
    raw = doc.get("steps")
    if not raw:
        raise MigrationFormatError(f"{filename}: no steps")
    if not isinstance(raw, list) or not all(isinstance(s, dict) for s in raw):
        raise MigrationFormatError(f"{filename}: steps must be an array of tables ([[steps]])")
    steps = []
    for i, entry in enumerate(raw, 1):
        store = entry.get("store")
        bodies = {k: v for k, v in entry.items() if k != "store"}
        if len(bodies) != 1:
            raise MigrationFormatError(
                f"{filename}: step {i} needs exactly one body (sql, cypher, iceberg, ...),"
                f" got {sorted(bodies) or 'none'}"
            )
        [(kind, body)] = bodies.items()
        steps.append(Step(store, kind, body))
    return _build(filename, id, name, steps, description)


def _step(where: str, i: int, store: object, kind: object, body: object) -> Step:
    if not isinstance(store, str):
        raise MigrationFormatError(f"{where}: step {i} needs a store (a string)")
    if not _STORE.fullmatch(store):
        raise MigrationFormatError(f"{where}: step {i}: invalid store name {store!r}")
    if not isinstance(kind, str) or not _KIND.fullmatch(kind):
        raise MigrationFormatError(
            f"{where}: step {i}: invalid step kind {kind!r} (lower-case letters, digits, _)"
        )
    _check_body(where, i, body)
    return Step(store, kind, body)


def _check_body(where: str, i: int, value: object) -> None:
    match value:
        case bool() | int() | str():
            return
        case float():
            raise MigrationFormatError(f"{where}: step {i}: floats have no portable form")
        case dt.date() | dt.time():
            raise MigrationFormatError(f"{where}: step {i}: dates and times are not allowed")
        case list():
            for v in value:
                _check_body(where, i, v)
            return
        case dict():
            for k, v in value.items():
                if not isinstance(k, str):
                    raise MigrationFormatError(f"{where}: step {i}: keys must be strings")
                _check_body(where, i, v)
            return
    raise MigrationFormatError(f"{where}: step {i}: unsupported value {value!r}")


def statements(kind: str, body: object) -> list[str]:
    """A ``sql`` / ``cypher``-like body: one statement or a non-empty list of them."""
    if isinstance(body, str):
        return [body]
    if isinstance(body, list) and body and all(isinstance(s, str) for s in body):
        return list(body)
    raise ValueError(f"a {kind} step is a statement or a list of statements, got {body!r}")


def load_migrations(directory: str | Path) -> list[Migration]:
    """Every ``*.toml`` in ``directory``, by id; other files are ignored."""
    root = Path(directory)
    if not root.is_dir():
        raise MigrationFormatError(f"{root}: no such directory")
    found: dict[int, Migration] = {}
    for path in sorted(root.glob("*.toml")):
        migration = parse_migration(path.name, path.read_text(encoding="utf-8"))
        if migration.id in found:
            raise MigrationFormatError(
                f"{root}: two migrations with id {migration.id}:"
                f" {found[migration.id].name} and {migration.name}"
            )
        found[migration.id] = migration
    return [found[i] for i in sorted(found)]


def by_id(migrations: Sequence[Migration]) -> list[Migration]:
    """``migrations`` sorted by id; ``MigrationFormatError`` on a duplicate id."""
    seen: dict[int, Migration] = {}
    for m in migrations:
        if m.id in seen:
            raise MigrationFormatError(f"two migrations with id {m.id}")
        seen[m.id] = m
    return [seen[i] for i in sorted(seen)]
