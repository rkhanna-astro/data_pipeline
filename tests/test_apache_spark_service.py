from apache_spark_pipeline.services.apache_spark_service import SparkService

def test_read_batch_returns_dataframe():
    spark = SparkService()
    df = spark.read_batch("main.sales.users")

    rows = df.collect()
    assert isinstance(rows, list)


def test_read_microbatch_filters_by_timestamp():
    spark = SparkService()

    df = spark.read_microbatch(
        "main.sales.users",
        "2024-01-20T00:00:00"
    )

    records = df.collect()

    assert all(
        r["updated_at"] > "2024-01-20T00:00:00"
        for r in records
    )

    spark.stop_service()

def test_stop_service_does_not_crash():
    spark = SparkService()
    spark.stop_service()
