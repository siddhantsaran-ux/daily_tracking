"""
Spark session management for BenchmarkManager.
"""
from pyspark.sql import SparkSession


def initialize_spark_session():
    """
    Initialize and return a Spark session with predefined configurations.
    
    Returns:
        SparkSession: Configured Spark session
    """
    spark = SparkSession.builder \
        .appName("BenchmarkManager") \
        .config("spark.sql.debug.maxToStringFields", "1000") \
        .config("spark.executor.memory", "30g") \
        .config("spark.driver.memory", "32g") \
        .config("spark.executor.cores", "8") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    print("Spark session initialized")
    return spark


def get_or_create_spark(spark_instance=None):
    """
    Get existing Spark session or create a new one.
    
    Args:
        spark_instance: Existing Spark session or None
        
    Returns:
        SparkSession: Spark session
    """
    if spark_instance is None:
        return initialize_spark_session()
    return spark_instance