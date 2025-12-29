class SparkDataFrame:
    def __init__(self, records):
        self.records = records or []

    def filter(self, expr: str):
        # supports: updated_at > 'timestamp'
        if "updated_at >" not in expr:
            return self

        ts = expr.split(">")[1].strip().strip("'")

        filtered = []
        for r in self.records:
            updated_at = r.get("updated_at")
            if not updated_at:
                continue  # HARD SKIP
            
            print("TIMESTAMP", updated_at, ts)
            if updated_at > ts:
                filtered.append(r)

        # print("Filtered", filtered)
        return SparkDataFrame(filtered)

    def collect(self):
        return self.records
