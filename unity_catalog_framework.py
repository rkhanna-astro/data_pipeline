from typing import Dict, List, Optional
from models.iceberg_table_models import IcebergColumn, IcebergTableMetadata

class UnityCatalogClient:
    # Mock Unity Catalog REST API
    
    def __init__(self, workspace_url: str, access_token: str):
        self.workspace_url = workspace_url.rstrip('/')
        self.access_token = access_token
        print("Initialized Unity Catalog client")
    
    def get_table_metadata(self, full_table_name: str) -> IcebergTableMetadata:
        print(f"Fetching metadata: {full_table_name}")
        data = self._mock_api_response(full_table_name)
        
        metadata = IcebergTableMetadata(
            catalog_name=data["catalog_name"],
            schema_name=data["schema_name"],
            table_name=data["name"],
            table_type=data["table_type"],
            data_source_format=data["data_source_format"],
            storage_location=data["storage_location"],
            columns=[
                IcebergColumn(name=col["name"], type_name=col["type_name"],
                            nullable=col.get("nullable", True), comment=col.get("comment"))
                for col in data["columns"]
            ],
            properties=data.get("properties", {})
        )
        return metadata
    
    def _mock_api_response(self, full_table_name: str) -> Dict:
        table_name = full_table_name.split('.')[-1]
        
        # Mocked Table Metadata
        tables = {
            "users": {
                "catalog_name": "main", "schema_name": "ecommerce", "name": "users",
                "table_type": "MANAGED", "data_source_format": "ICEBERG",
                "storage_location": "s3://my-data-lake/iceberg/users",
                "columns": [
                    {"name": "user_id", "type_name": "bigint", "nullable": False},
                    {"name": "username", "type_name": "string", "nullable": False},
                    {"name": "email", "type_name": "string", "nullable": True},
                    {"name": "created_at", "type_name": "timestamp", "nullable": False},
                    {"name": "country", "type_name": "string", "nullable": True}
                ],
                "properties": {}
            },
            "products": {
                "catalog_name": "main", "schema_name": "ecommerce", "name": "products",
                "table_type": "MANAGED", "data_source_format": "ICEBERG",
                "storage_location": "s3://my-data-lake/iceberg/products",
                "columns": [
                    {"name": "product_id", "type_name": "bigint", "nullable": False},
                    {"name": "name", "type_name": "string", "nullable": False},
                    {"name": "category", "type_name": "string", "nullable": False},
                    {"name": "price", "type_name": "decimal(10,2)", "nullable": False}
                ],
                "properties": {}
            },
            "transactions": {
                "catalog_name": "main", "schema_name": "ecommerce", "name": "transactions",
                "table_type": "MANAGED", "data_source_format": "ICEBERG",
                "storage_location": "s3://my-data-lake/iceberg/transactions",
                "columns": [
                    {"name": "transaction_id", "type_name": "bigint", "nullable": False},
                    {"name": "user_id", "type_name": "bigint", "nullable": False},
                    {"name": "product_id", "type_name": "bigint", "nullable": False},
                    {"name": "amount", "type_name": "decimal(10,2)", "nullable": False},
                    {"name": "timestamp", "type_name": "timestamp", "nullable": False}
                ],
                "properties": {}
            }
        }
        
        if table_name not in tables:
            raise ValueError(f"Table not found: {full_table_name}")
        
        return tables[table_name]
    
    def read_table_data(self, metadata: IcebergTableMetadata, limit: Optional[int] = None) -> List[Dict]:
        print(f"  Reading data (limit={limit or 'all'})")
        data = self._mock_table_data(metadata.table_name, limit or 100)
        return data
    
    def _mock_table_data(self, table_name: str, limit: int) -> List[Dict]:
        # Mocked Data
        data = {
            "users": [
                {"user_id": 1, "username": "alice_smith", "email": "alice@example.com", 
                 "created_at": "2024-01-15T10:30:00Z", "country": "US"},
                {"user_id": 2, "username": "bob_jones", "email": "bob@example.com",
                 "created_at": "2024-01-20T14:22:00Z", "country": "UK"},
                {"user_id": 3, "username": "carol_williams", "email": "carol@example.com",
                 "created_at": "2024-02-01T09:15:00Z", "country": "CA"}
            ],
            "products": [
                {"product_id": 5001, "name": "Laptop Pro 15", "category": "Electronics", "price": 1299.99},
                {"product_id": 5002, "name": "Wireless Mouse", "category": "Electronics", "price": 29.99},
                {"product_id": 5003, "name": "USB-C Cable", "category": "Accessories", "price": 15.99}
            ],
            "transactions": [
                {"transaction_id": 101, "user_id": 1, "product_id": 5001, "amount": 1299.99, 
                 "timestamp": "2024-03-01T10:30:00Z"},
                {"transaction_id": 102, "user_id": 2, "product_id": 5002, "amount": 29.99,
                 "timestamp": "2024-03-01T14:45:00Z"},
                {"transaction_id": 103, "user_id": 1, "product_id": 5003, "amount": 15.99,
                 "timestamp": "2024-03-02T09:20:00Z"},
                {"transaction_id": 104, "user_id": 3, "product_id": 5001, "amount": 1299.99,
                 "timestamp": "2024-03-03T16:00:00Z"}
            ]
        }
        return data.get(table_name, [])[:limit]