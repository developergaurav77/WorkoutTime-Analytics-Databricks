

import os


def get_spark(profile: str = "dev-free-edition"):
    """Get Spark session based on environment"""
    
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        print("Running in Databricks environment")
        from databricks.sdk.runtime import spark
        return spark
    else:
        print("Running locally with Databricks Connect")
        from databricks.connect import DatabricksSession
        spark = (DatabricksSession
                .builder
                .profile(profile)
                .serverless(True)
                .getOrCreate())
        return spark
