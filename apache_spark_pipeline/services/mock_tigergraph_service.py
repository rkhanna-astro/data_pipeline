from typing import Dict, List

class TigerGraphService:
    def __init__(self):
        self.vertices: Dict[str, Dict[str, Dict]] = {}

        # EdgeType -> List[edge]
        self.edges: Dict[str, List[Dict]] = {}

    def upsert_vertices(self, payload: Dict):
        """
        Payload format:
        {
          "vertices": {
            "User": {
              "u1": {"name": "Alice"},
              "u2": {"name": "Bob"}
            }
          }
        }
        """
        print("Payload", payload)
        vertices_payload = payload.get("vertices", {})

        for v_type, vertices in vertices_payload.items():
            self.vertices.setdefault(v_type, [])

            for v_id, attributes in vertices.items():
                self.vertices[v_type].append({
                    'v_type': v_type,
                    'v_id': v_id,
                    'attributes': attributes or {}
                })

        return {
            "status": "OK",
            "vertex_types": list(vertices_payload.keys()),
            "count": sum(len(v) for v in vertices_payload.values()),
        }

    def upsert_edges(self, payload: Dict):
        """
        Payload format:
        {
          "edges": {
            "PURCHASED": [
              {
                "from_type": "User",
                "from_id": "u1",
                "to_type": "Product",
                "to_id": "p1",
                "attributes": {"amount": 10}
              }
            ]
          }
        }
        """
        edges_payload = payload.get("edges", {})

        for etype, edges in edges_payload.items():
            self.edges.setdefault(etype, [])

            for e in edges:
                self.edges[etype].append({
                    "e_type": etype,
                    "directed": False,
                    "from_type": e["from_type"],
                    "from_id": e["from_id"],
                    "to_type": e["to_type"],
                    "to_id": e["to_id"],
                    "attributes": e.get("attributes", {}),
                })

        return {
            "status": "OK",
            "edge_types": list(edges_payload.keys()),
            "count": sum(len(e) for e in edges_payload.values()),
        }
    
    def fetch_vertices(self, vtype: str):
        print("Vertices", self.vertices)
        return self.vertices.get(vtype, {})

    def fetch_edges(self, etype: str):
        return self.edges.get(etype, [])
