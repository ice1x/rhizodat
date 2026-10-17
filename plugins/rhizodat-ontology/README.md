# rhizodat-ontology 🍄

Proposes an ontology for exported tables with Claude: the classes they hold,
their properties and the relationships between them, each with a confidence
and the reason given. The result is a **proposal file to review**; nothing is
written to a store until a person accepts relationships in it and runs
`apply`, which checks them against the data and loads the graph with
rhizodat's deterministic graph import.

```bash
pip install rhizodat-ontology
rhizodat-ontology propose customers.csv orders.csv -o proposal.json
# review proposal.json, set "accepted": true on the relationships to keep
RHIZODAT_STORE_GRAPH=drevo+bolt://... rhizodat-ontology apply proposal.json --store graph
```

It is a plugin, not part of rhizodat, because a model's answer is not
deterministic. The proposal records the model, the prompt version and a hash
of the data sample it saw, so a result can be traced.

Credentials: the Anthropic SDK's usual ones (`ANTHROPIC_API_KEY`, or a profile
from `ant auth login`).
