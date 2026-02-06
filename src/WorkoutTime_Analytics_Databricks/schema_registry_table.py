from spark import get_spark
import argparse
from common.parameters import get_runtime_args





def get_schema_file(format: str, path: str,header,spark):
    try:
        df = spark.read.format(format).load(path,header=header)
        return df.schema

    except Exception as e:
        print(f"Error reading schema from {path}: {e}")
        raise e


def main(all_table_ids: list, configs_dict: dict,spark):
   
    args = get_runtime_args()
    catalog = args.catalog
    ingestion_db = args.ingestion_db
    ingestion_table = args.ingestion_table
    schema_registry_table = args.schema_registry_table



    schema = get_schema_file(format=configs_dict["source_format"], path=configs_dict["source_path"],header=configs_dict["header"],spark=spark)



    table_id_schema_registry = spark.sql(f"select distinct table_id as table_id,source_schema_evolution from {catalog}.{ingestion_db}.{schema_registry_table}")

    all_schema_registry_table_ids = [i.table_id for i in table_id_schema_registry.select("table_id").collect()]
    automatic_schema_registry_table_ids = [i.table_id for i in table_id_schema_registry.filter("source_schema_evolution = 1").select("table_id").collect()]

    new_table_ids = list(set(all_table_ids).difference(set(all_schema_registry_table_ids)))

    new_table_ids.extend(automatic_schema_registry_table_ids)

    print(f"all table ids : {all_table_ids}")
    print(f"automatic_schema_registry_table_ids : {automatic_schema_registry_table_ids}")
    print(f"new_table_ids: {new_table_ids}")

    if configs_dict["table_id"] in new_table_ids:

        spark.sql(f"""
        UPDATE {catalog}.{ingestion_db}.{schema_registry_table}
        SET is_latest = FALSE
        WHERE table_id = {configs_dict["table_id"]}
        AND is_latest = TRUE
        """)


        schema_registry_query = f"""
        INSERT INTO {catalog}.{ingestion_db}.{schema_registry_table} (
            table_id,
            table_name,
            version,
            is_latest,
            schema_format,
            schema_definition,
            created_by
        )
        SELECT
            {configs_dict["table_id"]} AS table_id,
            '{configs_dict["target_table"]}' AS table_name,

            COALESCE(MAX(version), 0) + 1 AS version,

            TRUE AS is_latest,
            'json' AS schema_format,
            '{schema.json()}' AS schema_definition,
            'gaurav.thagunna' AS created_by

        FROM {catalog}.{ingestion_db}.{schema_registry_table}
        WHERE table_id = {configs_dict["table_id"]}
        """



        print(f"Creating schema registry for table_id {configs_dict['table_id']}... : \n {schema_registry_query}")
        spark.sql(schema_registry_query)

    else:
        print(f"No need to insert schema registry for table_id : {table_id}")



if __name__ == "__main__":


    spark = get_spark(profile="dev-free-edition")

    # from pyspark.dbutils import DBUtils
    # dbutils = DBUtils(spark.sparkContext)

    all_table_ids = dbutils.jobs.taskValues.get(
    taskKey="Ingestion_Master",
    key="table_ids",
    debugValue=[]
    )

    configs_dicts = dbutils.jobs.taskValues.get(
    taskKey="Ingestion_Master",
    key="configs",
    debugValue=[]
    ) 

    for table_id in all_table_ids:

        print(f"Running for table_id : {table_id}")
        configs_dict = [d for d in configs_dicts if d["table_id"] == table_id][0]
        main(all_table_ids,configs_dict,spark)
        
        print(f"Schema Registry completed for table_id : {table_id}")

