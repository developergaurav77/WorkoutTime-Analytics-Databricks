catalog = "dev"

ingestion_db = "control"
ingestion_table = "ingest_table_registry"
bronze_db = "bronze_workout"
schema_registry_table = "schema_registry"
history_table = "ingest_run_history"

table_id = 1
# table_id = dbutils.widgets.get("table_id")


from spark import get_spark

spark = get_spark(profile="dev-free-edition")
print("Spark session initialized:", spark)


def get_configs(table_id: int):
    query = f"""
    SELECT *
    FROM {catalog}.{ingestion_db}.{ingestion_table}
    WHERE table_id = {table_id}
    """
    df = spark.sql(query)
    if df.isEmpty():
        raise ValueError(f"No configurations found for table_id {table_id}")

    configs_dict = (
        df.select(
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
        .collect()[0]
        .asDict()
    )

    return configs_dict


configs_dict = get_configs(table_id=table_id)
print("Ingestion configurations retrieved:", configs_dict)