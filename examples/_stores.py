"""Where the examples find their stores: the environment, else the compose stack.

docker compose --profile clickhouse up -d --wait
"""

import os

PG = os.environ.get("RHIZODAT_PG", "postgresql://postgres:rhizodat@127.0.0.1:55440/rhizodat")
GRAPH = os.environ.get("RHIZODAT_GRAPH", "drevo+bolt://127.0.0.1:57687")
CLICKHOUSE = os.environ.get(
    "RHIZODAT_CLICKHOUSE", "clickhouse://rhizodat:rhizodat@127.0.0.1:58123/rhizodat"
)
JOURNAL = os.environ.get("RHIZODAT_JOURNAL", PG)
