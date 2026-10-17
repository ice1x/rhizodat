# Ontology proposals (plugin)

`rhizodat-ontology` asks Claude what exported tables hold, and proposes an
ontology: a class per table, what the columns mean, and the relationships
between tables, each with a confidence and the reason. It is a separate
package because a model's answer is not deterministic; rhizodat itself never
calls a model.

```bash
pip install rhizodat-ontology
rhizodat-ontology propose people.csv bugs.csv comments.csv -o proposal.json
```

The model sees each table's columns (with their inferred types) and its first
rows (`--sample-rows`, 20). Its answer is constrained to a JSON schema. The
proposal keeps:

- `provenance`: the model asked, the model that answered (a refusal may have
  been served by a fallback model), the prompt version, a SHA-256 of the
  sample it saw and the time;
- `classes` and `relationships` (every one with `"accepted": false`);
- `dropped`: relationships on tables or columns the data does not have;
- `notes`: what the model was unsure of.

## Review, then apply

Set `"accepted": true` on the relationships to keep, then:

```bash
RHIZODAT_STORE_GRAPH=drevo+bolt://... rhizodat-ontology apply proposal.json --store graph
bugs.reporter -> people.id  20/20
loaded 25 nodes, 20 edges -> graph
```

`apply` loads the tables with only the accepted relationships, each first
measured against the data (how many values are keys of the target), through
the [graph import from tables](imports.md#graphs-from-several-tables). Keys
come from the proposal's classes when they are keys in the data.

The deterministic way to the same graph, with relationships inferred from
column names and values, is `rhizodat relate`; the plugin is for tables whose
names do not say how they relate.

Credentials are the Anthropic SDK's (`ANTHROPIC_API_KEY`, or a profile from
`ant auth login`). The model defaults to `claude-opus-5-5` with high effort;
a refused request is retried server-side on a fallback model.
