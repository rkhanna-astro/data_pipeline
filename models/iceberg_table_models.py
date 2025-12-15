from typing import Dict, List, Optional
from dataclasses import dataclass

@dataclass
class IcebergColumn:
    name: str
    type_name: str
    nullable: bool
    comment: Optional[str] = None


@dataclass
class IcebergTableMetadata:
    catalog_name: str
    schema_name: str
    table_name: str
    table_type: str
    data_source_format: str
    storage_location: str
    columns: List[IcebergColumn]
    properties: Dict[str, str]
    
    @property
    def full_name(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.table_name}"
