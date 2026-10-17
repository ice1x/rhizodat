# Embeddings

Vectors for the text of graph nodes, kept beside them, and the nodes nearest
to a text or a vector. The store needs the `Vectors` capability: drevo has it,
and searches by cosine without a separate index.

```python
from rhizodat.embed import embed_nodes, load_embedder, similar

embedder = load_embedder("mypkg.models:SentenceEmbedder")   # yours, see below
report = embed_nodes(dat, store="graph", label="Chapter", text="summary", embedder=embedder)
report.embedded, report.unchanged

for n in similar(dat, store="graph", label="Chapter", query="a stormy night", embedder=embedder, k=5):
    print(n.score, n.key, n.properties["summary"])
```

```
rhizodat embed --store graph --label Chapter --text summary --model mypkg.models:SentenceEmbedder
rhizodat similar --store graph --label Chapter --model mypkg.models:SentenceEmbedder "a stormy night"
```

## Embedders

An embedder has a `model` (its name and version), `dimensions` and
`embed(texts) -> list of vectors`. rhizodat ships no model that understands
meaning. Wrap the one you use (an API client, a local model) in a small class
and name it as `module:Class`; it is constructed with no arguments:

```python
class SentenceEmbedder:
    model = "all-MiniLM-L6-v2@2"
    dimensions = 384

    def __init__(self) -> None:
        from sentence_transformers import SentenceTransformer
        self._m = SentenceTransformer("all-MiniLM-L6-v2")

    def embed(self, texts: list[str]) -> list[list[float]]:
        return self._m.encode(texts).tolist()
```

`hash` / `hash:<dimensions>` is the built-in `HashEmbedder`: words hashed into
buckets, no model and no network. Equal texts give equal vectors and shared
words bring texts closer, but it knows no meaning. It is for tests and
offline demos.

## What is stored, and when it is made again

The vector goes into a node property (`embedding` unless `property=` says
otherwise), with the model that made it (`embedding_model`) and a hash of the
text (`embedding_hash`). A second `embed_nodes` embeds only nodes whose text
changed, or every node after a model change, and writes all new vectors in
one transaction: a failure (the model's or the store's) writes none.
`similar` returns the nodes' properties without the vector and these two.

Search with the model that made the stored vectors: vectors of different
models are not comparable.
