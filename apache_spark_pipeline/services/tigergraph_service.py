from typing import Dict, List
import pyTigerGraph as tg

class TigerGraphService:
    def __init__(self, host="http://localhost", graphname="EcommerceGraph",
                 username="tigergraph", password="tigergraph"):

        self.conn = tg.TigerGraphConnection(
            host=host,
            graphname=graphname,
            username=username,
            password=password
        )

        secret = self.conn.createSecret()
        self.token = self.conn.getToken(secret)

    def upsert_vertices(self, payload: Dict):
        vertices_payload = payload.get("vertices", {})

        total_count = 0
        print("Payload", vertices_payload)
        for v_type, vertices in vertices_payload.items():
            print("THESE are vertices", v_type)
            self.conn.upsertVertices(v_type, vertices)
            total_count += len(vertices)

        return {
            "status": "OK",
            "vertex_types": list(vertices_payload.keys()),
            "count": total_count,
        }

    def upsert_edges(self, payload: Dict):
        edges_payload = payload.get("edges", {})

        total_count = 0
        for etype, edges in edges_payload.items():
            formatted_edges = [
                (e["from_id"], e["to_id"], e.get("attributes", {}))
                for e in edges
            ]
            print("Formatted_Edges", formatted_edges)
            # Get the source/target types from first edge
            if edges:
                src_type = edges[0]["from_type"]
                tgt_type = edges[0]["to_type"]
                self.conn.upsertEdges(src_type, etype, tgt_type, formatted_edges)
                total_count += len(edges)

        return {
            "status": "OK",
            "edge_types": list(edges_payload.keys()),
            "count": total_count,
        }

    def fetch_vertices(self, vtype: str):
        vertices =  self.conn.getVertices(vtype)
        print("returned vertices", vertices)
        return vertices

    def fetch_edges(self, etype: str):
 
        all_edges = []
        for vtype in self.conn.getVertexTypes():
            print("vertice Type", vtype)
            vertices = self.conn.getVertices(vtype)
            print("Formatted_Edges", vertices)
            for vid in vertices:
                edges = self.conn.getEdges(vtype, vid['v_id'], etype)
                all_edges.extend(edges)
        return all_edges
