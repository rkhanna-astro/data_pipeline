from typing import Dict, List

class TigerGraphService:
    def __init__(self):
        self.vertices: Dict[str, Dict[str, Dict]] = {}

        self.edges: Dict[str, List[Dict]] = {}

    def load_vertices(self, payload: Dict):
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

        for vtype, vertices in vertices_payload.items():
            self.vertices.setdefault(vtype, {})

            for vid, attributes in vertices.items():
                self.vertices[vtype][vid] = attributes or {}
        
        print("Vertices", self.vertices)

        return {
            "status": "OK",
            "vertex_types": list(vertices_payload.keys()),
            "count": sum(len(v) for v in vertices_payload.values()),
        }

    def load_edges(self, payload: Dict):
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
