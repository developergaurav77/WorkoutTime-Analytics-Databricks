import argparse
from common.parameters import get_runtime_args


def main():


    args = get_runtime_args()
    catalog = args.catalog
    ingestion_db = args.ingestion_db
    ingestion_table = args.ingestion_table
    schema_registry_table = args.schema_registry_table
    history_table = args.history_table

    print(f"Running for catalog {catalog}")

    from spark import get_spark

    spark = get_spark(profile="dev-free-edition")

    ingestion_table_query = f"""

  CREATE TABLE IF NOT EXISTS {catalog}.{ingestion_db}.{ingestion_table} (
    table_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    source_path STRING NOT NULL,
    source_format STRING DEFAULT 'parquet',
    header BOOLEAN DEFAULT TRUE,
    table_version INT DEFAULT 1,

    target_catalog STRING DEFAULT 'dev',
    target_schema STRING DEFAULT 'test',
    target_table STRING NOT NULL,
    write_mode STRING DEFAULT 'append'
        CHECK (write_mode IN ('append', 'merge', 'overwrite')),
    checkpoint_path STRING NOT NULL,
    merge_key ARRAY<STRING> DEFAULT ARRAY(),
    partition_by ARRAY<STRING> DEFAULT ARRAY(),
    enabled BOOLEAN DEFAULT TRUE,

  trigger_mode STRING DEFAULT 'availableNow'
      CHECK (trigger_mode IN ('availableNow', 'processingTime', 'continuous')),
    processing_time STRING DEFAULT '5 minutes',
    schema_evolution_mode STRING DEFAULT 'BACKWARD'
      CHECK (schema_evolution_mode IN ('BACKWARD', 'FORWARD', 'FULL', 'NONE')),
    
    expected_rows_per_day BIGINT,
    last_success TIMESTAMP,
    last_error STRING,
    consecutive_failures INT,
    last_run_duration_seconds INT,
    created_by STRING,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_by STRING,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
  )
  TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.feature.allowColumnDefaults' = 'supported'
  );

  """

    print(f"Creating ingestion master table if not exists... : \n {ingestion_table_query}")
    spark.sql(ingestion_table_query)

    print("Ingestion master table setup completed.")

    ## need to comment this insert after first run
    # insert_query_test = f"""

    # INSERT INTO {catalog}.{ingestion_db}.{ingestion_table}
    # (source_path,                         source_format,header,table_version,target_catalog,target_schema,target_table,write_mode,checkpoint_path,merge_key,partition_by,enabled,trigger_mode,processing_time,schema_evolution_mode,expected_rows_per_day,last_success,last_error,consecutive_failures,last_run_duration_seconds,created_by,created_at,updated_by,updated_at)
    # VALUES
    # ('/Volumes/dev/bronze_workout/rw/bpm/', 'json', TRUE, 1, 'dev', 'bronze_workout', 'bpm', 'append', '/Volumes/dev/bronze_workout/checkpoints/', ARRAY(), ARRAY(), TRUE, 'availableNow', '5 minutes', 'BACKWARD', 1000000, NULL, NULL, 0, NULL, 'gaurav.thagunna', CURRENT_TIMESTAMP(), NULL, CURRENT_TIMESTAMP());

    # """

    # spark.sql(insert_query_test)

    schema_registry_query = f"""CREATE TABLE IF NOT EXISTS {catalog}.{ingestion_db}.{schema_registry_table} (
    id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    table_id INT NOT NULL,
    table_name STRING NOT NULL,
    version INT DEFAULT 1,
    is_latest BOOLEAN DEFAULT TRUE,                 
    schema_format STRING,          
    schema_definition STRING,  
    source_schema_evolution INT DEFAULT 0,   
    created_by STRING,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
    )
    TBLPROPERTIES (
      'delta.autoOptimize.autoCompact' = 'true',
      'delta.feature.allowColumnDefaults' = 'supported'
    );

    """

    spark.sql(schema_registry_query)

    history_table_query = f"""
  CREATE TABLE IF NOT EXISTS {catalog}.{ingestion_db}.{history_table} (
      
    id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    run_id STRING NOT NULL,
    table_id INT NOT NULL,
    start_ts TIMESTAMP,    
    end_ts TIMESTAMP DEFAULT NULL,      
    duration_seconds BIGINT DEFAULT 0,
    status STRING DEFAULT 'running',     
    error STRING DEFAULT NULL,
    created_by STRING DEFAULT 'gaurav.thagunna',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
    )
    TBLPROPERTIES (
      'delta.autoOptimize.autoCompact' = 'true',
      'delta.feature.allowColumnDefaults' = 'supported'
    );    

  """

    spark.sql(history_table_query)

    print("Schema registry and history tables setup completed.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error running setup_ingestion_master.py: {e}")
        raise e
