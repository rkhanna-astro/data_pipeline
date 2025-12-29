from datetime import datetime, timedelta
import random

PURCHASES = []

base_time = datetime(2026, 1, 1, 11, 0, 0)

for i in range(1, 101):
    PURCHASES.append({
        "user_id": f"u{random.randint(1, 200)}",
        "product_id": f"p{random.randint(1, 50)}",
        "amount": round(random.uniform(5, 500), 2),
        "updated_at": (base_time + timedelta(days=i)).isoformat(),
        "ordered_at": (base_time + timedelta(days=i)).isoformat(),
    })
