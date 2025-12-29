from datetime import datetime

from .mock_users import USERS
from .mock_products import PRODUCTS
from .mock_purchases import PURCHASES

MOCK_ICEBERG_DATA = {
    "main.sales.users": USERS,
    "main.sales.products": PRODUCTS,
    "main.sales.purchases": PURCHASES,
}


# MOCK_ICEBERG_DATA = {
#     "main.sales.users": [
#         {
#             "user_id": "u1",
#             "name": "Alice",
#             "email": "alice@test.com",
#             "updated_at": "2024-01-01T10:00:00"
#         },
#         {
#             "user_id": "u2",
#             "name": "Bob",
#             "email": "bob@test.com",
#             "updated_at": "2024-01-02T10:00:00"
#         }
#     ],

#     "main.sales.products": [
#         {
#             "product_id": "p1",
#             "name": "Book",
#             "price": 12.5,
#             "updated_at": "2024-01-01T09:00:00"
#         }
#     ],

#     "main.sales.purchases": [
#         {
#             "purchase_id": "t1",
#             "user_id": "u1",
#             "product_id": "p1",
#             "amount": 12.5,
#             "ordered_at": datetime(2024, 1, 1, 12, 0),
#             "updated_at": "2024-01-01T12:01:00"
#         }
#     ]
# }
