"""Entity types for rhizodat-MCP: loading them, and their values to and from JSON.

The server does not know the application's classes: ``RHIZODAT_MCP_ENTITIES``
names them by module path (``tracker.models:Bug``). Each field's annotation
decides how its JSON value is read: a reference is the target's ``dat_id``, a
decimal its exact text, a date or time ISO 8601, bytes ``{"base64": ...}``.
"""

import base64
import binascii
import dataclasses
import datetime as dt
import decimal
import importlib
import types
import typing
import uuid
from collections.abc import Mapping
from typing import Any

from rhizodat.core.mapping import EntityMapping, Ref, is_entity, mapping_of
from rhizodat.core.parts import VALIDITY

__all__ = [
    "describe",
    "entity_json",
    "field_types",
    "fields",
    "from_json",
    "load_entity_types",
    "replace",
    "values_from_json",
]

_SERVICE = ("dat_id", "dat_created_at", "dat_updated_at", "dat_deleted_at")


def load_entity_types(spec: str) -> list[type]:
    """``module:Class`` entries, separated by commas, each an ``@entity`` class."""
    out: list[type] = []
    for entry in (e.strip() for e in spec.split(",")):
        if not entry:
            continue
        module, sep, attr = entry.partition(":")
        if not sep:
            raise ValueError(f"{entry!r}: name an entity as module:Class")
        try:
            found = getattr(importlib.import_module(module), attr)
        except (ImportError, AttributeError) as exc:
            raise ValueError(f"cannot load {entry!r}: {exc}") from exc
        if not is_entity(found):
            raise ValueError(f"{entry!r} is not an @entity class")
        out.append(found)
    return out


# --- descriptions -----------------------------------------------------------------------


def _optional(annotation: object) -> tuple[object, bool]:
    origin = typing.get_origin(annotation)
    if origin is types.UnionType or origin is typing.Union:
        args = typing.get_args(annotation)
        rest = [a for a in args if a is not type(None)]
        if len(rest) == 1 and len(rest) < len(args):
            return rest[0], True
    return annotation, False


def _ref_target(annotation: object, mapping: EntityMapping, field: str) -> str | None:
    """The referenced type's name if ``annotation`` is a reference, else ``None``."""
    if annotation is Ref:
        return mapping.refs.get(field, "an entity")
    if typing.get_origin(annotation) is Ref:
        args = typing.get_args(annotation)
        if args and is_entity(args[0]):
            return mapping_of(args[0]).type_name
        return mapping.refs.get(field, "an entity")
    return None


def describe(mapping: EntityMapping, field: str, annotation: object) -> str:
    """How an agent writes the field, in a few words."""
    inner, optional = _optional(annotation)
    text = _describe(mapping, field, inner)
    return f"{text}, or null" if optional else text


def _describe(mapping: EntityMapping, field: str, annotation: object) -> str:
    target = _ref_target(annotation, mapping, field)
    if target is not None:
        return f"reference to {target}"
    origin = typing.get_origin(annotation)
    if origin in (list, tuple, set, frozenset):
        args = typing.get_args(annotation)
        item = _describe(mapping, field, args[0]) if args else "any"
        return f"list of {item}"
    if origin is dict or annotation is dict:
        return "object"
    names: dict[object, str] = {
        str: "string",
        bool: "boolean",
        int: "integer",
        float: "number",
        decimal.Decimal: "decimal (as a string)",
        dt.datetime: "date-time (ISO 8601, with a time zone)",
        dt.date: "date (ISO 8601)",
        uuid.UUID: "uuid (as a string)",
        bytes: 'bytes ({"base64": ...})',
    }
    return names.get(annotation, "any JSON value")


# --- JSON → values ----------------------------------------------------------------------


def from_json(mapping: EntityMapping, field: str, annotation: object, value: object) -> Any:  # noqa: ANN401
    """The field's value from its JSON; ``ValueError`` naming the field if it does not fit."""
    inner, optional = _optional(annotation)
    if value is None:
        if optional or inner is object or inner is Any:
            return None
        raise ValueError(f"{field}: null is not allowed")
    return _convert(mapping, field, inner, value)


def _convert(mapping: EntityMapping, field: str, annotation: object, value: object) -> Any:  # noqa: ANN401
    if _ref_target(annotation, mapping, field) is not None:
        return Ref(_uuid(field, value, "a dat_id (uuid)"))
    origin = typing.get_origin(annotation)
    if origin in (list, tuple, set, frozenset):
        if not isinstance(value, list):
            raise ValueError(f"{field}: expected a list, got {type(value).__name__}")
        args = typing.get_args(annotation)
        item = args[0] if args else object
        items = [
            None
            if v is None and _optional(item)[1]
            else _convert(mapping, field, _optional(item)[0], v)
            for v in value
        ]
        return items if origin is list else origin(items)
    if origin is dict or annotation is dict:
        if not isinstance(value, dict):
            raise ValueError(f"{field}: expected an object, got {type(value).__name__}")
        return dict(value)
    if annotation is bool:
        if not isinstance(value, bool):
            raise ValueError(f"{field}: expected a boolean, got {value!r}")
        return value
    if annotation is int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{field}: expected an integer, got {value!r}")
        return value
    if annotation is float:
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise ValueError(f"{field}: expected a number, got {value!r}")
        return float(value)
    if annotation is str:
        if not isinstance(value, str):
            raise ValueError(f"{field}: expected a string, got {value!r}")
        return value
    if annotation is decimal.Decimal:
        try:
            if isinstance(value, bool) or not isinstance(value, str | int):
                raise decimal.InvalidOperation
            return decimal.Decimal(value)
        except decimal.InvalidOperation:
            raise ValueError(f"{field}: expected a decimal as a string, got {value!r}") from None
    if annotation is dt.datetime:
        return _iso(field, value, dt.datetime.fromisoformat, "date-time")
    if annotation is dt.date:
        return _iso(field, value, dt.date.fromisoformat, "date")
    if annotation is uuid.UUID:
        return _uuid(field, value, "a uuid")
    if annotation is bytes:
        raw = value.get("base64") if isinstance(value, dict) else None
        if not isinstance(raw, str):
            raise ValueError(f'{field}: expected {{"base64": "..."}}, got {value!r}')
        try:
            return base64.b64decode(raw, validate=True)
        except binascii.Error:
            raise ValueError(f"{field}: expected base64 text") from None
    return value  # unknown or Any: as given


def _uuid(field: str, value: object, what: str) -> uuid.UUID:
    try:
        return uuid.UUID(str(value)) if isinstance(value, str) else _bad(field, what, value)
    except ValueError:
        return _bad(field, what, value)


def _iso(field: str, value: object, parse: Any, what: str) -> Any:  # noqa: ANN401
    if isinstance(value, str):
        try:
            return parse(value)
        except ValueError:
            pass
    raise ValueError(f"{field}: expected an ISO 8601 {what}, got {value!r}")


def _bad(field: str, what: str, value: object) -> typing.NoReturn:
    raise ValueError(f"{field}: expected {what}, got {value!r}")


def fields(mapping: EntityMapping) -> tuple[str, ...]:
    """What an agent reads and writes: the mapped fields and, with valid time, the period."""
    return (*mapping.fields, *(VALIDITY if mapping.valid_time else ()))


def field_types(mapping: EntityMapping) -> dict[str, Any]:
    types_ = mapping.field_types()
    if mapping.valid_time:
        types_ |= dict.fromkeys(VALIDITY, dt.datetime | None)
    return types_


def values_from_json(
    mapping: EntityMapping, raw: Mapping[str, object], *, partial: bool
) -> dict[str, Any]:
    """Field values from JSON; every mapped field unless ``partial`` (a period may be left
    out: unbounded)."""
    service = sorted(set(raw) & set(_SERVICE))
    if service:
        raise ValueError(f"{', '.join(service)}: service fields are set by rhizodat")
    known = fields(mapping)
    unknown = sorted(set(raw) - set(known))
    if unknown:
        listed = ", ".join(known)
        raise ValueError(
            f"{mapping.type_name} has no field {', '.join(unknown)} (fields: {listed})"
        )
    if not partial:
        missing = [f for f in mapping.fields if f not in raw]
        if missing:
            raise ValueError(f"{mapping.type_name}: missing field(s) {', '.join(missing)}")
    types_ = field_types(mapping)
    return {f: from_json(mapping, f, types_[f], v) for f, v in raw.items()}


# --- values → JSON ----------------------------------------------------------------------


def _plain(value: object) -> object:
    if isinstance(value, Ref):
        return value.dat_id
    if isinstance(value, list | tuple | set | frozenset):
        return [_plain(v) for v in value]
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    return value


def entity_json(obj: object) -> dict[str, Any]:
    """An entity as JSON: ``dat_id``, the service times and every field."""
    from rhizodat.mcp.tools import jsonable

    mapping = mapping_of(type(obj))
    out: dict[str, Any] = {name: jsonable(getattr(obj, name, None)) for name in _SERVICE}
    for f in fields(mapping):
        out[f] = jsonable(_plain(getattr(obj, f)))
    return out


def replace(obj: object, values: Mapping[str, Any]) -> object:
    """``obj`` with ``values`` changed, keeping its id and service times."""
    return dataclasses.replace(obj, **values)  # type: ignore[type-var]
