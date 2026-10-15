"""``@entity`` declarations and per-type store mapping.

Which part of an entity lives in which store is declared on the type; there is
no central registry. Rules:

- every declared field is mapped to exactly one store, and every mapped name
  is a declared field;
- ``dat_*`` names are reserved for service fields, and ``valid_from`` /
  ``valid_to`` too when valid time is on;
- links to other entities are ``Ref[T]`` values holding the target's
  ``dat_id``; the mapping records the target type name for integrity checks.

``@entity`` turns the class into a keyword-only dataclass with a ``dat_id``
field (a new UUIDv7 unless given) and, with ``valid_time=True``, the
``valid_from`` / ``valid_to`` fields. Every instance also carries the service
times ``dat_created_at``, ``dat_updated_at`` and ``dat_deleted_at``, set by the
repository on reads and ignored by equality.

``soft_delete=True`` makes ``delete`` hide the entity for the configured window
(``Rhizodat(soft_delete_window=...)``) instead of erasing it;
``soft_delete=timedelta(...)`` sets this type's own window.
"""

import dataclasses
import functools
import inspect
import operator
import re
import sys
import types
import typing
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, ClassVar, TypeGuard, TypeVar, dataclass_transform

from rhizodat.core.identity import new_id

__all__ = ["EntityMapping", "MappingError", "Ref", "entity", "is_entity", "mapping_of"]

T = TypeVar("T")

_MAPPING_ATTR = "__rhizodat_mapping__"
_SERVICE_PREFIX = "dat_"
_VALIDITY_FIELDS = ("valid_from", "valid_to")
_SERVICE_TIMES = ("dat_created_at", "dat_updated_at", "dat_deleted_at")
_REF_IN_STRING = re.compile(r"""\bRef\[\s*['"]?([\w.]+)['"]?\s*\]""")


class MappingError(TypeError):
    """An entity declaration or a stored part does not match the mapping."""


@dataclass(frozen=True, slots=True)
class Ref[T]:
    """A reference to another entity by its ``dat_id``."""

    dat_id: uuid.UUID

    def __post_init__(self) -> None:
        if not isinstance(self.dat_id, uuid.UUID):
            raise TypeError(f"Ref needs a UUID dat_id, got {type(self.dat_id).__name__}")

    @classmethod
    def to(cls, target: object) -> "Ref[Any]":
        mapping_of(type(target))
        return cls(target.dat_id)  # type: ignore[attr-defined]


@dataclass(frozen=True, slots=True)
class EntityMapping:
    type_name: str
    stores: Mapping[str, tuple[str, ...]]
    refs: Mapping[str, str]
    valid_time: bool
    cls: type
    soft_delete: bool = False
    soft_delete_window: timedelta | None = None
    """The type's own window; ``None``: the configured default."""

    @property
    def fields(self) -> tuple[str, ...]:
        return tuple(f for fields in self.stores.values() for f in fields)

    def field_types(self) -> dict[str, Any]:
        """Each mapped field's type, resolved now that referenced classes exist;
        a field whose annotation still cannot be resolved maps to ``object``."""
        module = sys.modules.get(self.cls.__module__)
        namespace = dict(vars(module)) if module is not None else {}
        namespace.setdefault("Ref", Ref)
        raw = _own_annotations(self.cls)
        types_: dict[str, Any] = {}
        for f in self.fields:
            ann = raw.get(f, object)
            if isinstance(ann, typing.ForwardRef):
                ann = ann.__forward_arg__
            if isinstance(ann, str):
                try:
                    ann = eval(ann, namespace)
                except Exception:
                    ann = object
            types_[f] = _resolve_forward(ann, namespace)
        return types_

    def store_of(self, field: str) -> str:
        for store, fields in self.stores.items():
            if field in fields:
                return store
        raise MappingError(f"{self.type_name}: {field!r} is not a mapped field")

    def split(self, obj: object) -> dict[str, dict[str, Any]]:
        """Each store's part of ``obj``, without service fields."""
        return {
            store: {name: getattr(obj, name) for name in fields}
            for store, fields in self.stores.items()
        }

    def assemble(
        self, dat_id: uuid.UUID, parts: Mapping[str, Mapping[str, Any]], **service: object
    ) -> object:
        """Rebuild an instance from every store's part (the inverse of ``split``)."""
        values: dict[str, Any] = {}
        for store, fields in self.stores.items():
            if store not in parts:
                raise MappingError(f"{self.type_name}: part for store {store!r} is missing")
            part = parts[store]
            unknown = set(part) - set(fields)
            if unknown:
                raise MappingError(
                    f"{self.type_name}: store {store!r} part has unmapped fields "
                    f"{sorted(unknown)!r}"
                )
            missing = [f for f in fields if f not in part]
            if missing:
                raise MappingError(f"{self.type_name}: store {store!r} part lacks {missing!r}")
            values.update(part)
        return self.cls(dat_id=dat_id, **values, **service)


def is_entity(cls: object) -> TypeGuard[type]:
    return isinstance(cls, type) and isinstance(cls.__dict__.get(_MAPPING_ATTR), EntityMapping)


def mapping_of(cls: type) -> EntityMapping:
    mapping = cls.__dict__.get(_MAPPING_ATTR) if isinstance(cls, type) else None
    if not isinstance(mapping, EntityMapping):
        raise MappingError(f"{getattr(cls, '__name__', cls)!r} is not an @entity")
    return mapping


@dataclass_transform(kw_only_default=True)
def entity(
    *,
    stores: Mapping[str, Iterable[str]],
    name: str | None = None,
    valid_time: bool = False,
    soft_delete: bool | timedelta = False,
) -> Callable[[type[T]], type[T]]:
    """Declare an entity type and which of its fields live in which store."""
    store_fields = {store: tuple(fields) for store, fields in stores.items()}
    soft, window = _soft_delete(name, soft_delete)

    def wrap(cls: type[T]) -> type[T]:
        type_name = name or cls.__name__
        annotations = _own_annotations(cls)
        declared = [f for f, ann in annotations.items() if not _is_classvar(ann)]
        _validate(type_name, declared, store_fields, valid_time)

        cls.__annotations__ = {**annotations, "dat_id": uuid.UUID}
        cls.dat_id = dataclasses.field(default_factory=new_id)  # type: ignore[attr-defined]
        if valid_time:
            cls.__annotations__ |= {"valid_from": datetime | None, "valid_to": datetime | None}
            cls.valid_from = None  # type: ignore[attr-defined]
            cls.valid_to = None  # type: ignore[attr-defined]
        for service in _SERVICE_TIMES:
            cls.__annotations__[service] = datetime | None
            setattr(cls, service, dataclasses.field(default=None, compare=False))
        data_cls = dataclasses.dataclass(kw_only=True)(cls)

        refs: dict[str, str] = {}
        for f in declared:
            targets = _ref_targets(annotations[f])
            if len(targets) > 1:
                raise MappingError(
                    f"{type_name}: field {f!r} refers to several types {sorted(targets)!r}; "
                    "a reference has one target type"
                )
            if targets:
                refs[f] = targets.pop()
        setattr(
            data_cls,
            _MAPPING_ATTR,
            EntityMapping(
                type_name=type_name,
                stores=types.MappingProxyType(store_fields),
                refs=types.MappingProxyType(refs),
                valid_time=valid_time,
                cls=data_cls,
                soft_delete=soft,
                soft_delete_window=window,
            ),
        )
        return data_cls

    return wrap


def _soft_delete(name: str | None, value: object) -> tuple[bool, timedelta | None]:
    if isinstance(value, bool):
        return value, None
    if isinstance(value, timedelta):
        if value <= timedelta(0):
            raise MappingError(f"{name or 'entity'}: the soft_delete window must be positive")
        return True, value
    raise MappingError(
        f"{name or 'entity'}: soft_delete is True, False or a timedelta, not {value!r}"
    )


def _resolve_forward(annotation: object, namespace: dict[str, Any]) -> object:
    """Resolve string arguments such as ``Ref["Customer"]`` inside an annotation."""
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if origin is None or not args:
        return annotation
    resolved = []
    for arg in args:
        if isinstance(arg, typing.ForwardRef):
            arg = arg.__forward_arg__
        if isinstance(arg, str):
            try:
                arg = eval(arg, namespace)
            except Exception:
                return annotation
        resolved.append(_resolve_forward(arg, namespace))
    if origin is types.UnionType or origin is typing.Union:
        return functools.reduce(operator.or_, resolved)
    try:
        return origin[tuple(resolved)] if len(resolved) > 1 else origin[resolved[0]]
    except TypeError:
        return annotation


def _validate(
    type_name: str,
    declared: list[str],
    stores: dict[str, tuple[str, ...]],
    valid_time: bool,
) -> None:
    if not stores:
        raise MappingError(f"{type_name}: map the fields to at least one store")
    reserved = set(_VALIDITY_FIELDS) if valid_time else set()
    for f in declared:
        if f.startswith(_SERVICE_PREFIX) or f in reserved:
            raise MappingError(f"{type_name}: field name {f!r} is reserved")
    owner: dict[str, str] = {}
    for store, fields in stores.items():
        if not fields:
            raise MappingError(f"{type_name}: store {store!r} maps no fields")
        for f in fields:
            if f.startswith(_SERVICE_PREFIX) or f in reserved:
                raise MappingError(f"{type_name}: field name {f!r} is reserved")
            if f in owner:
                raise MappingError(
                    f"{type_name}: field {f!r} is mapped to both {owner[f]!r} and {store!r}"
                )
            if f not in declared:
                raise MappingError(f"{type_name}: {f!r} is not a field of the class")
            owner[f] = store
    for f in declared:
        if f not in owner:
            raise MappingError(f"{type_name}: field {f!r} is not mapped to any store")


def _own_annotations(cls: type) -> dict[str, Any]:
    """Annotations of ``cls`` itself, with unresolved names kept as forward refs."""
    if sys.version_info >= (3, 14):
        import annotationlib

        return dict(annotationlib.get_annotations(cls, format=annotationlib.Format.FORWARDREF))
    return dict(inspect.get_annotations(cls))


def _is_classvar(annotation: object) -> bool:
    if isinstance(annotation, str):
        return annotation.strip().startswith(("ClassVar", "typing.ClassVar"))
    return annotation is ClassVar or typing.get_origin(annotation) is ClassVar


def _ref_targets(annotation: object) -> set[str]:
    """Target type names of every ``Ref[...]`` inside ``annotation``."""
    if isinstance(annotation, str):
        return set(_REF_IN_STRING.findall(annotation))
    if isinstance(annotation, typing.ForwardRef):
        return _ref_targets(annotation.__forward_arg__)
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if origin is Ref and args:
        return {_type_name(args[0])}
    found: set[str] = set()
    for arg in args:
        found |= _ref_targets(arg)
    return found


def _type_name(target: object) -> str:
    if isinstance(target, str):
        return target
    if isinstance(target, typing.ForwardRef):
        return target.__forward_arg__
    if is_entity(target):
        return mapping_of(target).type_name
    return getattr(target, "__name__", repr(target))
