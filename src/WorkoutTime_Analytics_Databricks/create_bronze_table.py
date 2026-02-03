import json
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession, DataFrame


from spark import get_spark

catalog = "dev"

ingestion_db = "control"
ingestion_table = "ingest_table_registry"
bronze_db = "bronze_workout"
schema_registry_table = "schema_registry"
history_table = "ingest_run_history"


# spark = get_spark(profile="dev-free-edition")


def get_defined_schema(table_id: int,spark: SparkSession):
    schema = (
        spark.table(f"{catalog}.{ingestion_db}.{schema_registry_table}")
        .filter(f"table_id = {table_id} AND is_latest = TRUE")
        .orderBy(F.desc("version"))
        .limit(1)
        .collect()[0]
    )

    return StructType.fromJson(json.loads(schema.schema_definition))





def load_date(spark, configs_dict, schema) -> DataFrame:
    """Load data from a given path with the provided schema."""

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", configs_dict["source_format"])
        .option("cloudFiles.schemaLocation", configs_dict["checkpoint_path"] + "schema/" + configs_dict["target_table"])
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.maxFilesPerTrigger", 1)
        .schema(schema)
        .load(configs_dict["source_path"])
        .withColumn("ingestion_timestamp", F.current_timestamp())

    )


def ensure_target_table_exists(spark, configs_dict):
    try:
        print(f"Creating table: {configs_dict['target_table']}")
        spark.sql(
            f"DESCRIBE TABLE {configs_dict['target_catalog']}.{configs_dict['target_schema']}.{configs_dict['target_table']}"
        )
        return
    except Exception:
        pass

    spark.sql(
        f"CREATE TABLE {configs_dict['target_catalog']}.{configs_dict['target_schema']}.{configs_dict['target_table']}"
    )


def merge_to_delta(spark: SparkSession, df: DataFrame, configs_dict):
    ensure_target_table_exists(spark, configs_dict)
    print(f"merge keys : {configs_dict['merge_key']}")

    target_full_name = (
        f"{configs_dict['target_catalog']}.{configs_dict['target_schema']}.{configs_dict['target_table']}"
    )

    if not configs_dict["merge_key"]:
        print("merge keys not present")
        return (
            df.write.format("delta")
            .option("mergeSchema", "true")
            .mode(configs_dict["write_mode"])
            .saveAsTable(target_full_name)
        )

    else:
        df.createOrReplaceTempView("source_table")

        condition = " AND ".join([f"t.{key} = s.{key}" for key in configs_dict["merge_key"]])

        query = f"""
        MERGE INTO {target_full_name} AS t
        USING source_table AS s
        ON {condition}
        WHEN MATCHED THEN
        UPDATE SET * 
        WHEN NOT MATCHED THEN
        INSERT * 
            
        """

        spark.sql(query)
