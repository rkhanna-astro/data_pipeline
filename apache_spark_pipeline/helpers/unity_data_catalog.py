from .mock_iceberg_data import MOCK_ICEBERG_DATA
from .spark import Spark

class UnityCatalog:
    def __init__(self):
        self.spark = Spark()

    def table(self, table_name: str):
        if table_name not in MOCK_ICEBERG_DATA:
            raise ValueError(f"Mock table not found: {table_name}")

        records = MOCK_ICEBERG_DATA[table_name]
        return self.spark.create_dataframe(records)
