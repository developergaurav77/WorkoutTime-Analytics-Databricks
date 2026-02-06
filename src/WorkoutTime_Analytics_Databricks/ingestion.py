"""
1. setup_ingestion_master.py - Sets up the ingestion master table and inserts a test record.
2. schema_registry_table.py - Creates a schema registry entry based on the source data schema.
3. create_bronze_table.py - Creates bronze tables based on ingestion configurations and schema regisrty configuration.
"""
import argparse
from pyspark.sql import SparkSession, DataFrame


from ingestion_history import now_ts, start_run,finish_run_success,finish_run_failure
from spark import get_spark
from create_bronze_table import get_defined_schema, load_date, merge_to_delta
from common.logger import setup_logger


logger = setup_logger()



def start_stream(df: DataFrame, configs_dict: dict, spark: SparkSession):
    writer = (
        df.writeStream.foreachBatch(lambda batch_df, batch_id: merge_to_delta(spark, batch_df, configs_dict))
        .option("checkpointLocation", configs_dict["checkpoint_path"] + configs_dict["target_table"])
        .queryName(configs_dict["target_table"])
    )

    if configs_dict["trigger_mode"] == "availableNow":
        return writer.trigger(availableNow=True).start()
    else:
        return writer.trigger(processingTime=configs_dict["processing_time"]).start()


def start_stream_test(df: DataFrame, configs_dict: dict, spark: SparkSession):
    target_full_name = (
        f"{configs_dict['target_catalog']}.{configs_dict['target_schema']}.{configs_dict['target_table']}"
    )

    logger.info(f"Triggering for table {configs_dict['target_table']}")

    writer = (
        df.writeStream.format("delta")
        .option("mergeSchema", "true")
        .option("checkpointLocation", configs_dict["checkpoint_path"] + configs_dict["target_table"])
        .queryName(configs_dict["target_table"])
        .outputMode(configs_dict["write_mode"])  
    )

    if configs_dict["trigger_mode"] == "availableNow":
        logger.info(f"Triggering availableNow for table {configs_dict['target_table']}")
        query = writer.trigger(availableNow=True).toTable(target_full_name)

        query.awaitTermination()

        progress = query.lastProgress
        rows = int(progress["sources"][0]["numInputRows"])

        if rows == 0:
            logger.warning(f"No new files found for {configs_dict['target_table']}")
        else:
            logger.info(f"{rows} rows processed for {configs_dict['target_table']}")

        return query
    

    else:
        return writer.trigger(processingTime=configs_dict["processing_time"]).toTable(target_full_name)


# def view_streaming_df(df):
#     """View streaming DataFrame"""
#     return display(
#         df,
#         checkpointLocation=f"{configs_dict['checkpoint_path']}tmp/{configs_dict['target_table']}"
#     )


def view_streaming_df(df: DataFrame):
    """View streaming DataFrame in console for testing purposes."""
    query = df.writeStream.format("console").outputMode("append").start()

    query.awaitTermination()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table_id", required=True)
    args = parser.parse_args()

    table_id = int(args.table_id)


    logger.info(f"Running for table_id: {table_id}, type: {type(table_id)}")


    spark = get_spark(profile="dev-free-edition")
    logger.info(f"Spark session initialized: {spark}")

    configs_dict = dbutils.jobs.taskValues.get(
    taskKey="Ingestion_Master",
    key="configs",
    debugValue=[]
    ) 
    configs_dict = [d for d in configs_dict if d["table_id"] == table_id][0]

    start_ts, run_id  = start_run(table_id=table_id,configs_dict=configs_dict, spark=spark)

    try:

        schema = get_defined_schema(table_id=table_id, spark=spark)
        logger.info(f"Schema retrieved for table_id: {table_id} \n {schema}")

        raw_data = load_date(spark=spark, configs_dict=configs_dict, schema=schema)
        logger.info("Raw data loaded as streaming DataFrame.")
        # view_streaming_df(df=raw_data)

        # start_stream(df=raw_data, configs_dict=configs_dict,spark=spark)
        start_stream_test(df=raw_data, configs_dict=configs_dict, spark=spark)

        finish_run_success(run_id=run_id,start_ts=start_ts, table_id=table_id,configs_dict=configs_dict, spark=spark)
        logger.info("Ingestion process stopped.")

    except Exception as e:
        logger.info(f"Error during ingestion: {e}")

        finish_run_failure(run_id=run_id, start_ts=start_ts, table_id=table_id,configs_dict=configs_dict, err_msg=e,spark=spark)
        raise e


if __name__ == "__main__":
    main()
