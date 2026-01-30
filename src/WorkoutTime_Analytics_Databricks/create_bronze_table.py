from ingestion import (
    catalog,
    ingestion_db,
    ingestion_table,
    bronze_db,
)
from spark import get_spark     

spark = get_spark(profile="dev-free-edition")

