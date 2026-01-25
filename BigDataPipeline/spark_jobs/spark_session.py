from pyspark.sql import SparkSession

def get_spark_session(app_name: str):
    """
    Creates and returns a SparkSession with Hive support enabled.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .getOrCreate()

