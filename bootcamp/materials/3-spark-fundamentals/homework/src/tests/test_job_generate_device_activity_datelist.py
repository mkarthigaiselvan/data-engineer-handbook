from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType
from datetime import date
from chispa.dataframe_comparer import assert_df_equality
from jobs.job_generate_device_activity_datelist import run_job
from collections import namedtuple

def test_device_activity_datelist():
    spark = SparkSession.builder.master("local[*]").appName("TestJob").getOrCreate()

    # Input
    input_data = [("u1", "d1"), ("u2", "d2")]
    input_df = spark.createDataFrame(input_data, ["user_id", "device_id"])

    # Expected
    expected_data = [
        ("u1", "d1", date(2023, 1, 1)),
        ("u1", "d1", date(2023, 1, 2)),
        ("u1", "d1", date(2023, 1, 3)),
        ("u2", "d2", date(2023, 1, 1)),
        ("u2", "d2", date(2023, 1, 2)),
        ("u2", "d2", date(2023, 1, 3)),
    ]

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("date", DateType(), False)
    ])

    expected_df = spark.createDataFrame(expected_data, schema=schema)


    # Actual
    result_df = run_job(input_df, "2023-01-01", "2023-01-03")

    # Assert
    assert_df_equality(result_df.orderBy("user_id", "device_id", "date"), expected_df.orderBy("user_id", "device_id", "date"))

if __name__ == "__main__":
    test_device_activity_datelist()
