"""Vectors: entity fields that hold embeddings, and what a graph holding them returns.

``Vector(n)`` marks an entity field as an embedding of ``n`` dimensions::

    @entity(stores={"pg": ["title", "embedding", "model"]})
    class Issue:
        title: str
        embedding: Annotated[list[float], Vector(384)] | None
        model: str | None

The field is stored like any list of floats and versioned with the entity;
``EntityRepository.similar`` searches it (a ``Match`` per entity found).
``VectorCandidate`` and ``Neighbour`` are what a graph store with the
``rhizodat.core.adapter.Vectors`` capability reads and returns for
``rhizodat.embed``.
"""

import math
import types
import typing
from dataclasses import dataclass, field
from typing import Any

__all__ = ["Match", "Neighbour", "Vector", "VectorCandidate", "cosine", "vector_dimensions"]


@dataclass(frozen=True, slots=True)
class Vector:
    """Marks a ``list[float]`` field as a vector of ``dimensions`` floats."""

    dimensions: int

    def __post_init__(self) -> None:
        if not isinstance(self.dimensions, int) or self.dimensions < 1:
            raise ValueError(f"a vector has at least 1 dimension, not {self.dimensions!r}")

    def __repr__(self) -> str:
        return f"Vector({self.dimensions})"


def vector_dimensions(annotation: object) -> int | None:
    """The dimensions of a ``Vector`` field (optional or not); ``None`` for other fields.

    Raises ``TypeError`` when ``Vector`` marks something other than ``list[float]``.
    """
    origin = typing.get_origin(annotation)
    if origin is types.UnionType or origin is typing.Union:
        found = {vector_dimensions(a) for a in typing.get_args(annotation)} - {None}
        return found.pop() if len(found) == 1 else None
    if origin is not typing.Annotated:
        return None
    base, *extras = typing.get_args(annotation)
    marks = [e for e in extras if isinstance(e, Vector)]
    if not marks:
        return None
    if base != list[float]:
        raise TypeError(f"Vector marks a list[float] field, not {base!r}")
    return marks[0].dimensions


def as_vector(value: object, dimensions: int, what: str) -> list[float]:
    """``value`` as a list of ``dimensions`` finite floats, or ``ValueError`` naming ``what``."""
    if isinstance(value, str | bytes) or not isinstance(value, typing.Sequence):
        raise ValueError(f"{what}: a vector is a sequence of numbers, not {type(value).__name__}")
    if any(isinstance(x, bool) or not isinstance(x, int | float) for x in value):
        raise ValueError(f"{what}: a vector holds numbers only")
    out = [float(x) for x in value]
    if len(out) != dimensions:
        raise ValueError(f"{what}: the vector has {dimensions} dimensions, got {len(out)}")
    if not all(math.isfinite(x) for x in out):
        raise ValueError(f"{what}: a vector holds finite numbers only")
    return out


def cosine(a: typing.Sequence[float], b: typing.Sequence[float]) -> float:
    """Cosine similarity, 1.0 the closest; 0.0 when either vector is all zeros."""
    norm = math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b))
    return sum(x * y for x, y in zip(a, b, strict=True)) / norm if norm else 0.0


@dataclass(frozen=True, slots=True)
class Match[T]:
    """An entity found by ``similar`` and how close its vector is to the query."""

    entity: T
    score: float
    """Cosine similarity, 1.0 the closest."""


@dataclass(frozen=True, slots=True)
class VectorCandidate:
    """A node with text to embed, and what its current vector was made from."""

    key: str
    text: str
    hash: str | None
    """The hash of the text the stored vector was made from; ``None`` if there is none."""
    model: str | None
    """The model that made the stored vector."""


@dataclass(frozen=True, slots=True)
class Neighbour:
    key: str
    score: float
    """Cosine similarity, 1.0 the closest."""
    properties: dict[str, Any] = field(default_factory=dict)
    """The node's properties, without the vector and its bookkeeping."""
