from pyspark.sql import SparkSession

def create_spark():
    spark = SparkSession.builder \
        .appName("HaloStatsAnalysis") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark
