"""Embeddings: vectors for nodes' text, kept beside them, and the nearest nodes by meaning.

An ``Embedder`` turns texts into vectors (``model``, ``dimensions``,
``embed(texts)``); rhizodat ships none that understands meaning, only
``HashEmbedder`` for tests and offline demos. A real model plugs in as
``module:Class`` (``load_embedder``), for example a thin class around an API
client or a local model.

``embed_nodes`` embeds the text of every node with a label and writes the
vectors in one unit, each with the model that made it and a hash of its text,
so a second run embeds only edited texts, or everything after a model change.
``similar`` finds the nodes nearest to a text or a vector. The store needs the
``Vectors`` capability (drevo has it).
"""

import hashlib
import importlib
import math
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from rhizodat.api import Rhizodat
    from rhizodat.core.vectors import Neighbour

__all__ = ["EmbedReport", "Embedder", "HashEmbedder", "embed_nodes", "load_embedder", "similar"]

_WORD = re.compile(r"\w+", re.UNICODE)


@runtime_checkable
class Embedder(Protocol):
    @property
    def model(self) -> str:
        """The model's name and version: a vector made by another model is made again."""
        ...

    @property
    def dimensions(self) -> int: ...

    def embed(self, texts: list[str]) -> list[list[float]]:
        """One vector of ``dimensions`` floats per text, in order."""
        ...


class HashEmbedder:
    """Words hashed into a fixed number of buckets, the vector normalised.

    Equal texts give equal vectors and texts sharing words are closer than texts
    sharing none, with no model and no network. It knows no meaning (synonyms are
    strangers): for tests and offline demos only.
    """

    def __init__(self, dimensions: int = 256) -> None:
        if dimensions < 1:
            raise ValueError(f"dimensions must be at least 1, not {dimensions}")
        self.dimensions = dimensions
        self.model = f"hash-{dimensions}"

    def embed(self, texts: list[str]) -> list[list[float]]:
        return [self._one(t) for t in texts]

    def _one(self, text: str) -> list[float]:
        v = [0.0] * self.dimensions
        for word in _WORD.findall(text.lower()):
            digest = hashlib.sha256(word.encode()).digest()
            bucket = int.from_bytes(digest[:8], "big") % self.dimensions
            v[bucket] += 1.0 if digest[8] & 1 else -1.0
        norm = math.sqrt(sum(x * x for x in v))
        return [x / norm for x in v] if norm else v


def load_embedder(spec: str) -> Embedder:
    """``hash`` / ``hash:<dimensions>``, or ``module:Name`` (a class or factory called
    with no arguments)."""
    name, _, arg = spec.partition(":")
    if name == "hash":
        if arg and not arg.isdigit():
            raise ValueError(f"{spec!r}: write hash:<dimensions>, e.g. hash:256")
        return HashEmbedder(int(arg) if arg else 256)
    if not arg:
        raise ValueError(f"{spec!r}: name an embedder as module:Class, or hash[:<dimensions>]")
    try:
        made = getattr(importlib.import_module(name), arg)()
    except (ImportError, AttributeError) as exc:
        raise ValueError(f"cannot load {spec!r}: {exc}") from exc
    if not isinstance(made, Embedder):
        raise ValueError(f"{spec!r} is not an embedder (model, dimensions, embed(texts))")
    return made


@dataclass(frozen=True, slots=True)
class EmbedReport:
    embedded: int
    unchanged: int
    model: str


def _store(dat: "Rhizodat", store: str) -> Any:  # noqa: ANN401 - Vectors
    from rhizodat.core.adapter import Vectors

    target = dat.store(store)
    if not isinstance(target, Vectors):
        raise ValueError(
            f"store {store!r} ({type(target).__name__}) cannot hold vectors: its adapter"
            " implements no Vectors capability"
        )
    return target


def _hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:32]


def embed_nodes(
    dat: "Rhizodat",
    *,
    store: str,
    label: str,
    text: str,
    embedder: Embedder,
    property: str = "embedding",
    batch: int = 64,
) -> EmbedReport:
    """Embed the ``text`` property of every ``label`` node whose text or model changed
    since its vector was made; all new vectors are written as one unit."""
    target = _store(dat, store)
    candidates = target.vector_candidates(label, text, property)
    todo = [c for c in candidates if c.model != embedder.model or c.hash != _hash(c.text)]
    total = len(candidates)
    rows: list[dict[str, Any]] = []
    for i in range(0, len(todo), batch):
        chunk = todo[i : i + batch]
        vectors = embedder.embed([c.text for c in chunk])
        for c, vector in zip(chunk, vectors, strict=True):
            if len(vector) != embedder.dimensions:
                raise ValueError(
                    f"{embedder.model} promised {embedder.dimensions} dimensions,"
                    f" gave {len(vector)} for {c.key}"
                )
            rows.append(
                {"key": c.key, "vector": vector, "hash": _hash(c.text), "model": embedder.model}
            )
    if rows:
        target.write_vectors(label, property, rows)
    return EmbedReport(len(rows), total - len(rows), embedder.model)


def similar(
    dat: "Rhizodat",
    *,
    store: str,
    label: str,
    query: str | Sequence[float],
    embedder: Embedder | None = None,
    property: str = "embedding",
    k: int = 10,
) -> "list[Neighbour]":
    """The ``k`` ``label`` nodes nearest to ``query`` (a text, embedded with ``embedder``
    — the model that made the stored vectors — or a vector), closest first."""
    target = _store(dat, store)
    if isinstance(query, str):
        if embedder is None:
            raise ValueError("a text query needs an embedder: the one that made the vectors")
        [vector] = embedder.embed([query])
    else:
        vector = [float(x) for x in query]
    found: list[Neighbour] = target.nearest(label, property, list(vector), k=k)
    return found
