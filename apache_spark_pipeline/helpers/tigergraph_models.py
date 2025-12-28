# -------------------------
# Mapping Definitions
# -------------------------
from dataclasses import dataclass
from typing import Dict, List, Optional

@dataclass
class VertexMapping:
    table: str
    vertex_type: str
    primary_id: str
    attributes: List[str]
    # Iceberg field -> TigerGraph type
    type_conversions: Optional[Dict[str, str]] = None


@dataclass
class EdgeMapping:
    table: str
    edge_type: str
    from_vertex_type: str
    to_vertex_type: str
    from_id: str
    to_id: str
    attributes: List[str]
    # Iceberg field -> TigerGraph type
    type_conversions: Optional[Dict[str, str]] = None
