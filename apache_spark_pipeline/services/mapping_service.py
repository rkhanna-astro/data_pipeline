from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from ..helpers.tigergraph_models import VertexMapping, EdgeMapping

class MappingEngine:
    def __init__(self):
        # table_name -> ('vertex' | 'edge', mapping)
        self.mappings: Dict[str, Tuple[str, Any]] = {}

    def add_vertex_mapping(self, mapping: VertexMapping):
        self.mappings[mapping.table] = ("vertex", mapping)

    def add_edge_mapping(self, mapping: EdgeMapping):
        self.mappings[mapping.table] = ("edge", mapping)

    def transform_records(self, table_name: str, records: List[Dict]):
        if table_name not in self.mappings:
            raise ValueError(f"No mapping registered for table '{table_name}'")

        mapping_type, mapping = self.mappings[table_name]

        if mapping_type == "vertex":
            return self._transform_vertices(mapping, records)

        if mapping_type == "edge":
            return self._transform_edges(mapping, records)

        raise ValueError("Invalid mapping type")

    def _transform_vertices(self, mapping: VertexMapping, records: List[Dict]):
        vertices: Dict[str, Dict] = {}

        for rec in records:
            try:
                v_id = self._convert(
                    rec.get(mapping.primary_id),
                    mapping.type_conversions,
                    mapping.primary_id
                )

                if v_id is None:
                    continue

                attrs = {
                    attr: self._convert(
                        rec.get(attr),
                        mapping.type_conversions,
                        attr
                    )
                    for attr in mapping.attributes
                    if rec.get(attr) is not None
                }

                vertices[str(v_id)] = attrs

            except Exception as e:
                print(f"[Vertex Error] {e}")

        return {
            "vertices": {
                mapping.vertex_type: vertices
            }
        }


    def _transform_edges(self, mapping: EdgeMapping, records: List[Dict]):
        edges: List[Dict] = []

        for rec in records:
            try:
                from_id = self._convert(
                    rec.get(mapping.from_id),
                    mapping.type_conversions,
                    mapping.from_id
                )

                to_id = self._convert(
                    rec.get(mapping.to_id),
                    mapping.type_conversions,
                    mapping.to_id
                )

                if from_id is None or to_id is None:
                    continue

                attrs = {
                    attr: self._convert(
                        rec.get(attr),
                        mapping.type_conversions,
                        attr
                    )
                    for attr in mapping.attributes
                    if rec.get(attr) is not None
                }

                edges.append({
                    "from_type": mapping.from_vertex_type,
                    "from_id": str(from_id),
                    "to_type": mapping.to_vertex_type,
                    "to_id": str(to_id),
                    "attributes": attrs
                })

            except Exception as e:
                print(f"[Edge Error] {e}")

        return {
            "edges": {
                mapping.edge_type: edges
            }
        }

    def _convert(self, value: Any, conversions: Optional[Dict], field: str):
        if value is None:
            return None

        conv_type = conversions.get(field) if conversions else None

        if conv_type == "string":
            return str(value)

        if conv_type == "int":
            return int(value)

        if conv_type == "double":
            return float(value)

        if conv_type == "bool":
            return bool(value)

        if conv_type == "datetime":
            if isinstance(value, datetime):
                return value.isoformat()
            return str(value)

        # Default behavior
        if isinstance(value, datetime):
            return value.isoformat()

        return value

    def get_summary(self) -> str:
        lines = []

        for table, (mtype, mapping) in self.mappings.items():
            lines.append(f"Table: {table}")

            if mtype == "vertex":
                lines.append(f"  Vertex: {mapping.vertex_type}")
                lines.append(f"  Primary ID: {mapping.primary_id}")
                lines.append(f"  Attributes: {', '.join(mapping.attributes)}")

            else:
                lines.append(f"  Edge: {mapping.edge_type}")
                lines.append(
                    f"  From {mapping.from_vertex_type}({mapping.from_id}) "
                    f"To {mapping.to_vertex_type}({mapping.to_id})"
                )

        return "\n".join(lines)
