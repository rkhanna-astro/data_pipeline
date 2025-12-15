# Python Objects inspired from TigerGraphDB
from datetime import datetime

class User:
    user_id: str
    username: str
    email: str
    created_at: datetime
    country: str
    
class Product:
    product_id: str
    name: str
    category: str
    price: float

class Purchase(User, Product):
    user_id: str
    product_id: str
    transaction_id: str
    amount: float
    timestamp: datetime
