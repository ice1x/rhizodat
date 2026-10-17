"""An ontology for exported tables, proposed by Claude, applied only as a person accepts it.

``propose`` shows the model each table's columns (with their inferred types)
and a few rows, and asks for the classes the tables hold, their properties and
the relationships between them, each relationship with a confidence and a
reason. The answer is constrained to a JSON schema. The proposal keeps the
model that answered, the prompt version and a hash of the sample, and drops
(listing them) relationships on tables or columns the data does not have.

Every relationship starts with ``"accepted": false``. ``apply`` loads only the
accepted ones, each checked against the data (``rhizodat.ingest.relate.
measure``), through rhizodat's deterministic graph import. Nothing the model
says reaches a store without a person in between.
"""

import datetime as dt
import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from rhizodat import Rhizodat
    from rhizodat.ingest import TablesGraphReport

__all__ = [
    "MODEL",
    "PROMPT_VERSION",
    "ProposalError",
    "ProposalRefused",
    "accepted_relationships",
    "apply",
    "propose",
]

MODEL = "claude-opus-5-5"
PROMPT_VERSION = "1"
FORMAT = 1

SYSTEM = """\
You design graph ontologies for data exported from business systems. You are \
shown tables: their names, columns with inferred types, and a few rows. Propose \
the class each table holds (a singular CamelCase label), what its columns mean, \
and the relationships between tables: a column of one table whose values \
identify rows of another table (its key). Only propose relationships the \
columns and rows support; give each a confidence from 0 to 1 and the evidence \
in one sentence. A person reviews your proposal before anything is loaded, so \
say what you are unsure of in the notes rather than guessing."""

_PROPERTY = {
    "type": "object",
    "properties": {"column": {"type": "string"}, "meaning": {"type": "string"}},
    "required": ["column", "meaning"],
    "additionalProperties": False,
}
SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "classes": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "table": {"type": "string"},
                    "label": {"type": "string"},
                    "description": {"type": "string"},
                    "key": {"type": ["string", "null"]},
                    "properties": {"type": "array", "items": _PROPERTY},
                },
                "required": ["table", "label", "description", "key", "properties"],
                "additionalProperties": False,
            },
        },
        "relationships": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "table": {"type": "string"},
                    "column": {"type": "string"},
                    "target": {"type": "string"},
                    "name": {"type": "string"},
                    "confidence": {"type": "number"},
                    "reason": {"type": "string"},
                },
                "required": ["table", "column", "target", "name", "confidence", "reason"],
                "additionalProperties": False,
            },
        },
        "notes": {"type": "string"},
    },
    "required": ["classes", "relationships", "notes"],
    "additionalProperties": False,
}


class ProposalError(RuntimeError):
    """The model gave no usable proposal."""


class ProposalRefused(ProposalError):
    """The model (and its fallbacks) declined the request."""


def _sample(sources: Mapping[str, str | Path], rows: int) -> dict[str, Any]:
    from rhizodat.ingest import infer_schema, read_batches

    sample: dict[str, Any] = {}
    for name, source in sources.items():
        schema = infer_schema(source)
        head: list[dict[str, Any]] = []
        for batch in read_batches(source, schema):
            head.extend(batch.slice(0, rows - len(head)).to_pylist())
            if len(head) >= rows:
                break
        sample[name] = {
            "columns": {c.name: c.type for c in schema.columns},
            "rows": head,
        }
    return sample


def _client() -> Any:  # noqa: ANN401 - anthropic.Anthropic
    import anthropic

    return anthropic.Anthropic()


def propose(
    sources: Mapping[str, str | Path],
    *,
    client: Any = None,  # noqa: ANN401 - an anthropic.Anthropic, or a stand-in in tests
    model: str = MODEL,
    sample_rows: int = 20,
    effort: str = "high",
) -> dict[str, Any]:
    """A proposal for the tables in ``sources`` (name → file), to review and accept."""
    sample = _sample(sources, sample_rows)
    text = json.dumps(sample, default=str, ensure_ascii=False, sort_keys=True)
    response = (client or _client()).beta.messages.create(
        model=model,
        max_tokens=16000,
        system=SYSTEM,
        messages=[{"role": "user", "content": f"The tables, as JSON:\n\n{text}"}],
        output_config={"effort": effort, "format": {"type": "json_schema", "schema": SCHEMA}},
        betas=["server-side-fallback-2026-07-01"],
        fallbacks="default",
    )
    if response.stop_reason == "refusal":
        details = getattr(response, "stop_details", None)
        raise ProposalRefused(f"the model declined ({getattr(details, 'category', None)})")
    if response.stop_reason != "end_turn":
        raise ProposalError(f"the answer was cut off ({response.stop_reason})")
    answer = next((b.text for b in response.content if b.type == "text"), "")
    try:
        found = json.loads(answer)
    except json.JSONDecodeError:
        raise ProposalError("the answer is not JSON") from None

    columns = {name: set(t["columns"]) for name, t in sample.items()}
    kept, dropped = [], []
    for rel in found.get("relationships", []):
        problem = None
        if rel["table"] not in columns or rel["target"] not in columns:
            problem = f"no table {rel['table'] if rel['table'] not in columns else rel['target']}"
        elif rel["column"] not in columns[rel["table"]]:
            problem = f"no column {rel['column']} in {rel['table']}"
        if problem:
            dropped.append(f"{rel['table']}.{rel['column']} -> {rel['target']}: {problem}")
        else:
            kept.append({**rel, "accepted": False})
    return {
        "format": FORMAT,
        "provenance": {
            "model": model,
            "served_by": getattr(response, "model", model),
            "prompt_version": PROMPT_VERSION,
            "sample_sha256": hashlib.sha256(text.encode()).hexdigest(),
            "created_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
        },
        "tables": {name: str(path) for name, path in sources.items()},
        "classes": [c for c in found.get("classes", []) if c["table"] in columns],
        "relationships": kept,
        "dropped": dropped,
        "notes": found.get("notes", ""),
    }


def accepted_relationships(proposal: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [r for r in proposal.get("relationships", []) if r.get("accepted") is True]


def apply(dat: "Rhizodat", proposal: Mapping[str, Any], *, store: str) -> "TablesGraphReport":
    """Load the tables of ``proposal`` into the graph ``store`` with only its accepted
    relationships, each measured against the data first."""
    from rhizodat.ingest import import_tables_graph
    from rhizodat.ingest.relate import measure, read_tables

    if proposal.get("format") != FORMAT:
        raise ProposalError(f"not a proposal of format {FORMAT}")
    accepted = accepted_relationships(proposal)
    if not accepted:
        raise ProposalError('no relationship is accepted: set "accepted": true on those to keep')
    sources = dict(proposal["tables"])
    tables = read_tables(sources)
    keys = {
        c["table"]: c["key"]
        for c in proposal.get("classes", [])
        if c.get("key") and c["table"] in tables and c["key"] in tables[c["table"]].columns
    }
    tables = read_tables(sources, keys=keys)
    measured = [measure(tables, r["table"], r["column"], r["target"]) for r in accepted]
    return import_tables_graph(dat, sources, store=store, keys=keys, relationships=measured)
