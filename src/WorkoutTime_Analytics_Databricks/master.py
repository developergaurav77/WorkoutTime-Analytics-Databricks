from ingestion import get_spark
from common.parameters import get_runtime_args


def get_configs(catalog, ingestion_db, ingestion_table, spark):
    query = f"""
    SELECT *
    FROM {catalog}.{ingestion_db}.{ingestion_table}
    WHERE enabled = TRUE

    """
    df = spark.sql(query)
    if df.isEmpty():
        raise ValueError(f"No configurations found for table_ids")

    configs_dict = df.select(
        "table_id",
        "source_path",
        "source_format",
        "header",
        "target_catalog",
        "target_schema",
        "table_version",
        "target_table",
        "write_mode",
        "checkpoint_path",
        "merge_key",
        "partition_by",
        "enabled",
        "trigger_mode",
        "processing_time",
        "schema_evolution_mode",
    )

    return [row.asDict() for row in configs_dict.collect()]


def main():
    spark = get_spark(profile="dev-free-edition")

    args = get_runtime_args()
    catalog = args.catalog
    ingestion_db = args.ingestion_db
    ingestion_table = args.ingestion_table

    configs_dict = get_configs(catalog, ingestion_db, ingestion_table, spark)
    table_ids = [conf["table_id"] for conf in configs_dict]

    print(f"Fetched configurations for table_ids {table_ids} from {catalog}.{ingestion_db}.{ingestion_table}")
    dbutils.jobs.taskValues.set(key="table_ids", value=table_ids)
    dbutils.jobs.taskValues.set(key="configs", value=configs_dict)

    print(f"Set table_ids {table_ids} and configs_dict for downstream tasks.")

if __name__ == "__main__":
    main()
