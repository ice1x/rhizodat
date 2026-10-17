"""Embedders: the protocol, the built-in hash embedder, and loading one by name.

The hash embedder needs no model and no network: it maps words to buckets of a
fixed-size vector, so equal texts give equal vectors and texts sharing words
are closer than texts that share none. It is for tests and offline demos, not
for meaning; a real model plugs in as ``module:Class``.
"""

import math

import pytest
from faker import Faker

from rhizodat.embed import Embedder, HashEmbedder, load_embedder


def cosine(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b, strict=True))


def test_the_hash_embedder_is_deterministic_and_normalised(faker: Faker) -> None:
    e = HashEmbedder(64)
    assert isinstance(e, Embedder)
    assert (e.model, e.dimensions) == ("hash-64", 64)
    text = faker.sentence()
    [a, b] = e.embed([text, text])
    assert a == b
    assert len(a) == 64
    assert math.isclose(math.sqrt(sum(x * x for x in a)), 1.0)


def test_shared_words_bring_texts_closer() -> None:
    e = HashEmbedder(256)
    engines, also_engines, gardens = e.embed(
        ["analytical engines compute", "the analytical engines of babbage", "roses and gardens"]
    )
    assert cosine(engines, also_engines) > cosine(engines, gardens)


def test_empty_text_is_the_zero_vector() -> None:
    [v] = HashEmbedder(8).embed([""])
    assert v == [0.0] * 8


def test_dimensions_must_be_positive() -> None:
    with pytest.raises(ValueError, match="dimensions"):
        HashEmbedder(0)


def test_embedders_load_by_name() -> None:
    assert load_embedder("hash").dimensions == 256
    assert load_embedder("hash:32").model == "hash-32"
    loaded = load_embedder("embedders_fixture:Fixed")
    assert loaded.model == "fixed-1"
    for spec, message in [
        ("hash:x", "hash:<dimensions>"),
        ("nope", "module:Class"),
        ("no_such_module:X", "no_such_module"),
        ("embedders_fixture:not_an_embedder", "not an embedder"),
    ]:
        with pytest.raises(ValueError, match=message):
            load_embedder(spec)
