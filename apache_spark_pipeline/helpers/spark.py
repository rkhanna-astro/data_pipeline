from .spark_data_frame import SparkDataFrame

class Spark:
    def create_dataframe(self, records):
        return SparkDataFrame(records)

    def stop(self):
        print("Spark stopped")
