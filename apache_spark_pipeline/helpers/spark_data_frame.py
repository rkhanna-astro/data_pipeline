class SparkDataFrame:
    def __init__(self, records):
        self.records = records or []

    def filter(self, expr: str):
        # supports: updated_at > 'timestamp'
        if "updated_at >" in expr:
            ts = expr.split(">")[1].strip().strip("'")
            self.records = [
                r for r in self.records
                if r.get("updated_at", "") > ts
            ]
        return self

    def collect(self):
        return self.records
