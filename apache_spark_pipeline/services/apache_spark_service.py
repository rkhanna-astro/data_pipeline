# from pyspark.sql import SparkSession
from ..helpers.unity_data_catalog import UnityCatalog

class SparkService:
    def __init__(self):
        self.unity_catalog = UnityCatalog()

    def read_batch(self, table):
        return self.unity_catalog.table(table)

    def read_microbatch(self, table, last_ts):
        return (
            self.unity_catalog
            .table(table)
            .filter(f"updated_at > '{last_ts}'")
        )

    def stop_service(self):
        self.unity_catalog.spark.stop()
