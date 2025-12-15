from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any


@dataclass
class VertexMapping:
    table: str
    vertex_type: str
    primary_id: str
    attributes: List[str]
    # necessary to make attributes TigerDB storage compatible
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
    # necessary to make attributes TigerDB storage compatible
    type_conversions: Optional[Dict[str, str]] = None


class MappingEngine:
    # Mock mappings of iceberg data format and type definitions for TigerGraphDB
    def __init__(self):
        self.mappings: Dict[str, Tuple[str, Any]] = {}
    
    def add_vertex_mapping(self, mapping: VertexMapping):
        # adding new mappings as vertices if new tables/fields are introduced
        self.mappings[mapping.table] = ('vertex', mapping)
    
    def add_edge_mapping(self, mapping: EdgeMapping):
        # adding new mappings as edges if new relations between two vertices are introduced
        self.mappings[mapping.table] = ('edge', mapping)
    
    def transform_records(self, table_name: str, records: List[Dict]):
        if table_name not in self.mappings:
            raise ValueError(f"No mapping for {table_name}")
        
        mapping_type, mapping = self.mappings[table_name]
        result = {"vertices": [], "edges": []}
        
        if mapping_type == 'vertex':
            result['vertices'] = self.transform_vertices(mapping, records)

        elif mapping_type == 'edge':
            result['edges'] = self.transform_edges(mapping, records)
        
        print(f"These are all vertices and edges {result}")
        return result
    
    def transform_vertices(self, mapping: VertexMapping, records: List[Dict]):
        vertices = []
        for rec in records:
            try:
                v_id = self.convert(rec[mapping.primary_id], mapping.type_conversions, mapping.primary_id)
                attrs = {a: self.convert(rec[a], mapping.type_conversions, a) 
                        for a in mapping.attributes if a in rec}
                
                vertices.append({
                    "v_type": mapping.vertex_type,
                    "v_id": str(v_id),
                    "attributes": attrs
                })

                print(f"These are all {vertices}")

            except Exception as e:
                print(f"Error transforming vertex: {e}")
        return vertices
    
    def transform_edges(self, mapping: EdgeMapping, records: List[Dict]):
        edges = []

        for rec in records:
            try:
                from_id = self.convert(rec[mapping.from_id], mapping.type_conversions, mapping.from_id)
                to_id = self.convert(rec[mapping.to_id], mapping.type_conversions, mapping.to_id)
                attrs = {a: self.convert(rec[a], mapping.type_conversions, a)
                        for a in mapping.attributes if a in rec}
                
                edges.append({
                    "e_type": mapping.edge_type,
                    "from_type": mapping.from_vertex_type,
                    "from_id": str(from_id),
                    "to_type": mapping.to_vertex_type,
                    "to_id": str(to_id),
                    "attributes": attrs
                })

                print(f"These are all {edges}")

            except Exception as e:
                print(f"Error transforming edge: {e}")

        return edges
    
    def convert(self, value: Any, conversions: Optional[Dict], field: str):
        if value is None:
            return ""
        
        if conversions and field in conversions:
            # ensure that datasets are tigerGraph compatible and ready
            conv_type = conversions[field]

            if conv_type == 'string':
                return str(value)
            
            elif conv_type == 'double':
                return float(value)
            
            elif conv_type == 'datetime':
                if isinstance(value, str):
                    try:
                        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
                        return dt.strftime("%Y-%m-%d %H:%M:%S")

                    except:
                        return value
        
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        return value
    
    def get_summary(self) -> str:
        # Just a quick check code for knowing existing mappings
        lines = []

        for table, (mtype, mapping) in self.mappings.items():
            lines.append(f"Table: {table}")

            if mtype == 'vertex':
                lines.append(f"vertex: {mapping.vertex_type}")
                lines.append(f"primary_id: {mapping.primary_id}")
                lines.append(f"attributes: {', '.join(mapping.attributes)}")

            else:
                lines.append(f"edge: {mapping.edge_type}")
                lines.append(f"{mapping.from_vertex_type} to {mapping.to_vertex_type}")
                lines.append(f"from: {mapping.from_id}, to: {mapping.to_id}")

        return "\n".join(lines)
