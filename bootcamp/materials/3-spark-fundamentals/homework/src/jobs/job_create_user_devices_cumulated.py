from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, max as _max

def run_job(input_df):
    return input_df.groupBy("user_id", "device_id") \
        .agg(
            _sum("session_count").alias("total_sessions"),
            _max("last_active_date").alias("last_seen_date")
        )

if __name__ == "__main__":
    spark = SparkSession.builder.appName("UserDevicesCumulatedJob").getOrCreate()
    input_df = spark.read.parquet("input/user_device_activity.parquet")
    result_df = run_job(input_df)
    result_df.write.mode("overwrite").parquet("output/user_devices_cumulated")
