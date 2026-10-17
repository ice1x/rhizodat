"""What a store holding embeddings (``rhizodat.core.adapter.Vectors``) reads and returns."""

from dataclasses import dataclass, field
from typing import Any

__all__ = ["Neighbour", "VectorCandidate"]


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
