from datetime import datetime, timedelta

PRODUCTS = []

base_time = datetime(2026, 1, 1, 9, 0, 0)

for i in range(1, 51):
    PRODUCTS.append({
        "product_id": f"p{i}",
        "name": f"Product-{i}",
        "price": round(10 + i * 1.5, 2),
        "updated_at": (base_time + timedelta(days=i * 2)).isoformat(),
    })
