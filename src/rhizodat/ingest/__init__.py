"""Importing files into a store, with the table's schema inferred from the file.

``infer_schema`` reads a CSV, TSV, JSON Lines or Parquet file (local or in
object storage) and gives every column the narrowest portable type that holds
for all its values. ``import_file`` turns a new table, or new columns, into a
migration written next to the others, runs it, and loads the rows into the
store as one unit (the store's ``Loads`` capability).
"""

from rhizodat.ingest.importer import ImportPlan, ImportReport, import_file, plan_import
from rhizodat.ingest.schema import Column, LogicalType, TableSchema
from rhizodat.ingest.sources import DEFAULT_NULLS, infer_schema, read_batches

__all__ = [
    "DEFAULT_NULLS",
    "Column",
    "ImportPlan",
    "ImportReport",
    "LogicalType",
    "TableSchema",
    "import_file",
    "infer_schema",
    "plan_import",
    "read_batches",
]
