from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from jobs.job_create_user_devices_cumulated import run_job
from collections import namedtuple

def test_user_devices_cumulated():
    spark = SparkSession.builder.master("local[*]").appName("TestJob").getOrCreate()

    # Input
    input_data = [
        ("u1", "d1", 3, "2023-01-02"),
        ("u1", "d1", 2, "2023-01-05"),
        ("u2", "d2", 1, "2023-01-03")
    ]
    input_df = spark.createDataFrame(input_data, ["user_id", "device_id", "session_count", "last_active_date"])

    # Expected
    expected_data = [
        ("u1", "d1", 5, "2023-01-05"),  # sum(3+2), latest date
        ("u2", "d2", 1, "2023-01-03")
    ]
    expected_df = spark.createDataFrame(expected_data, ["user_id", "device_id", "total_sessions", "last_seen_date"])

    # Actual
    result_df = run_job(input_df)

    # Assert
    assert_df_equality(result_df.orderBy("user_id"), expected_df.orderBy("user_id"))

if __name__ == "__main__":
    test_user_devices_cumulated()
