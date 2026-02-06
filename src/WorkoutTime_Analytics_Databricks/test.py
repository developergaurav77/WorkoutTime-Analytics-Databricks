import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--table_id", required=True)
args = parser.parse_args()

table_id = args.table_id
print(f"Running for table_id: {table_id}, type: {type(table_id)}")

table_id = int(table_id)
print(f"Running for table_id update: {table_id}, type: {type(table_id)}")

all_table_ids = dbutils.jobs.taskValues.get(taskKey="Ingestion_Master", key="table_ids", debugValue=[])
print("Fetched all table_ids:", all_table_ids)

configs_dicts = dbutils.jobs.taskValues.get(taskKey="Ingestion_Master", key="configs", debugValue=[])
print("Fetched configs_dicts:", configs_dicts)

configs_dict = [d for d in configs_dicts if d["table_id"] == table_id][0]

print(f"Fetched configs_dict for table_id : {table_id}", configs_dict)
