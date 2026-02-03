"""
1. setup_ingestion_master.py - Sets up the ingestion master table and inserts a test record.
2. schema_registry_table.py - Creates a schema registry entry based on the source data schema.
3. create_bronze_table.py - Creates bronze tables based on ingestion configurations and schema regisrty configuration.
"""



catalog = "dev"

ingestion_db = "control"
ingestion_table = "ingest_table_registry"
bronze_db = "bronze_workout"
schema_registry_table = "schema_registry"
history_table = "ingest_run_history"

table_id = 1
# table_id = dbutils.widgets.get("table_id")


from spark import get_spark




def get_configs(table_id: int,spark):
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





from create_bronze_table import get_defined_schema, load_date, merge_to_delta

from pyspark.sql import SparkSession, DataFrame


def start_stream(df:DataFrame, configs_dict:dict,spark:SparkSession):
    writer = (
        df.writeStream
          .foreachBatch(lambda batch_df, batch_id:
                        merge_to_delta(spark, batch_df, configs_dict))
         .option("checkpointLocation", configs_dict["checkpoint_path"]+configs_dict["target_table"]) 
          .queryName(configs_dict["target_table"])
    )

    if configs_dict["trigger_mode"] == "availableNow":
        return writer.trigger(availableNow=True).start()
    else:
        return writer.trigger(processingTime=configs_dict["processing_time"]).start()




def main():

    spark = get_spark(profile="dev-free-edition")
    print("Spark session initialized:", spark)

    configs_dict = get_configs(table_id=table_id,spark=spark)
    print("Ingestion configurations retrieved:", configs_dict)


    schema = get_defined_schema(table_id=table_id,spark=spark)
    print(f"Schema retrieved for table_id: {table_id} \n {schema}")

    raw_data = load_date(spark=spark,configs_dict=configs_dict,schema=schema)

    start_stream(df=raw_data, configs_dict=configs_dict,spark=spark)


if __name__ == "__main__":
    main()

