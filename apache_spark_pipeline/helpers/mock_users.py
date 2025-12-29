from datetime import datetime, timedelta
import random

USERS = []

base_time = datetime(2026, 1, 1, 10, 0, 0)

for i in range(1, 201):
    USERS.append({
        "user_id": f"u{i}",
        "name": f"User-{i}",
        "email": f"user{i}@example.com",
        "updated_at": (base_time + timedelta(days=i)).isoformat(),
    })
