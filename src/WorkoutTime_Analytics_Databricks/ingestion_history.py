import datetime, uuid


ingestion_db = "control"
ingestion_table = "ingest_table_registry"
history_table = "ingest_run_history"

from pyspark.sql import SparkSession
def get_runid():
    return str(uuid.uuid4())


def now_ts():
    return datetime.datetime.utcnow()


def start_run(table_id: str,configs_dict:dict, spark: SparkSession) -> str:
    start_ts = now_ts()
    start_ts_str = start_ts.isoformat()
    run_id = get_runid()



    spark.sql(f"""INSERT INTO {configs_dict['target_catalog']}.{ingestion_db}.{history_table} (run_id, table_id, start_ts)
                  VALUES ('{run_id}','{table_id}', '{start_ts_str}')
                  
                  """)
    
    return start_ts,run_id


def finish_run_success(run_id,start_ts, table_id,configs_dict, spark: SparkSession):
    end_ts = now_ts()
    duration_seconds = int((end_ts - start_ts).total_seconds())
    end_ts_str = end_ts.isoformat()
    
    spark.sql(f"""
              UPDATE {configs_dict['target_catalog']}.{ingestion_db}.{history_table} 
              SET end_ts = TIMESTAMP'{end_ts}', status='success', 
              duration_seconds={duration_seconds}
              WHERE run_id='{run_id}'""")
    
    spark.sql(f"""
              MERGE INTO {configs_dict['target_catalog']}.{ingestion_db}.{ingestion_table} tr USING (SELECT '{table_id}' as table_id) s ON tr.table_id = s.table_id WHEN MATCHED THEN UPDATE SET 
              last_success = TIMESTAMP'{end_ts}', 
              last_error = NULL, 
              consecutive_failures = 0, 
              last_run_duration_seconds = '{duration_seconds}',
              updated_at = TIMESTAMP'{end_ts_str}'""")
    



def finish_run_failure(run_id, start_ts, table_id,configs_dict, err_msg,spark:SparkSession):
    end_ts = now_ts()

    duration_seconds = int((end_ts - start_ts).total_seconds())


    end_ts_str = end_ts.isoformat()
    # esc = err_msg.replace("'", "''")[:2000]
    spark.sql(f"""UPDATE {configs_dict['target_catalog']}.{ingestion_db}.{history_table} 
              SET end_ts = TIMESTAMP'{end_ts_str}', status='failed', error = '{err_msg}', duration_seconds = '{duration_seconds}' WHERE run_id = '{run_id}'""")
    
    spark.sql(f"""MERGE INTO {configs_dict['target_catalog']}.{ingestion_db}.{ingestion_table} tr USING (SELECT '{table_id}' as table_id) s 
              ON tr.table_id = s.table_id WHEN MATCHED THEN 
              UPDATE SET last_error = '{err_msg}', 
              consecutive_failures = coalesce(tr.consecutive_failures,0)+1, 
              last_run_duration_seconds = '{duration_seconds}',
              updated_at = TIMESTAMP'{end_ts_str}'""")
