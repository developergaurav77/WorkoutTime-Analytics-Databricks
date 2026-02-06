
import argparse
from dataclasses import dataclass


@dataclass
class RuntimeArgs:
    catalog: str
    ingestion_db: str
    ingestion_table: str
    schema_registry_table: str
    history_table: str


def get_runtime_args() -> RuntimeArgs:
    parser = argparse.ArgumentParser(description="Ingestion master task")

    parser.add_argument("--catalog", default="dev")
    parser.add_argument("--ingestion_db", default="control")
    parser.add_argument("--ingestion_table", default="ingest_table_registry")
    parser.add_argument("--schema_registry_table", default="schema_registry")
    parser.add_argument("--history_table", default="ingest_run_history")

    args = parser.parse_args()

    return RuntimeArgs(
        catalog=args.catalog,
        ingestion_db=args.ingestion_db,
        ingestion_table=args.ingestion_table,
        schema_registry_table=args.schema_registry_table,
        history_table=args.history_table,
    )



