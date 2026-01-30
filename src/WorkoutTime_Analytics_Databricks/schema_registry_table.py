from ingestion import catalog, ingestion_db, ingestion_table, schema_registry_table, configs_dict
from spark import get_spark

spark = get_spark(profile="dev-free-edition")


def get_schema_file(format: str, path: str):
    try:
        df = spark.read.format(format).load(path)
        return df.schema

    except Exception as e:
        print(f"Error reading schema from {path}: {e}")
        raise e


schema = get_schema_file(format=configs_dict["source_format"], path=configs_dict["source_path"])

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
VALUES (
  {configs_dict["table_id"]},
  '{configs_dict["target_table"]}',
  1,
  TRUE,
  'json',
  '{schema.json()}',
  'gaurav.thagunna'
)

"""
print(f"Creating schema registry for table_id {configs_dict['table_id']}... : \n {schema_registry_query}")
spark.sql(schema_registry_query)
