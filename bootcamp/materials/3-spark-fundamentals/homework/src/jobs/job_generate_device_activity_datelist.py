from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, sequence, to_date, lit
from pyspark.sql.types import DateType

def run_job(user_device_df, start_date, end_date):
    date_df = user_device_df.select("user_id", "device_id").distinct()
    date_df = date_df.withColumn("date", explode(sequence(lit(start_date).cast(DateType()), lit(end_date).cast(DateType()))))
    return date_df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("DeviceActivityDateListJob").getOrCreate()
    user_device_df = spark.read.parquet("input/user_device_activity.parquet")
    result_df = run_job(user_device_df, "2023-01-01", "2023-01-07")
    result_df.write.mode("overwrite").parquet("output/device_activity_datelist")
