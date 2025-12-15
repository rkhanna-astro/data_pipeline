from typing import List, Dict

class TigerGraphClient:
    # Mocked Tiger Client Rest++
    def __init__(self, host: str, graph_name: str, username: str = "tigergraph",
                 password: str = "", secret: str = ""):
        self.host = host.rstrip('/')
        self.graph_name = graph_name
        self.token = "mock-token-12345"
    
    def upsert_vertices(self, vertex_type: str, vertices: List[Dict], batch_size: int = 1000) -> Dict:
        print(f"Upserting {len(vertices)} {vertex_type} vertices")
        
        # Show sample vertex
        if vertices:
            sample = vertices[0]
            print(f"Sample: {vertex_type}({sample['v_id']}) = {sample['attributes']}")
        
        return {
            "vertex_type": vertex_type,
            "total_accepted": len(vertices),
            "total_skipped": 0
        }
    
    def upsert_edges(self, edge_type: str, edges: List[Dict], batch_size: int = 1000) -> Dict:
        print(f"Upserting {len(edges)} {edge_type} edges")
        
        # Show sample edge
        if edges:
            sample = edges[0]
            print(f"Sample: {sample['from_type']}({sample['from_id']}) "
                        f"-[{edge_type}]-> {sample['to_type']}({sample['to_id']})")
        
        return {
            "edge_type": edge_type,
            "total_accepted": len(edges),
            "total_skipped": 0
        }
    
    def get_vertex_count(self, vertex_type: str) -> int:
        counts = {"User": 3, "Product": 3}
        return counts.get(vertex_type, 0)
    
    def get_edge_count(self, edge_type: str) -> int:
        counts = {"PURCHASED": 4}
        return counts.get(edge_type, 0)
