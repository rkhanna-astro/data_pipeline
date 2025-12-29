from apache_spark_pipeline.services.tigergraph_service import TigerGraphService

def test_load_and_fetch_vertices():
    tg = TigerGraphService()

    payload = {
        "vertices": {
            "User": {
                "u1": {"name": "Alice"},
                "u2": {"name": "Bob"},
            }
        }
    }

    result = tg.load_vertices(payload)

    assert result["count"] == 2
    users = tg.fetch_vertices("User")
    assert users["u1"]["name"] == "Alice"


def test_load_and_fetch_edges():
    tg = TigerGraphService()

    payload = {
        "edges": {
            "PURCHASED": [
                {
                    "from_type": "User",
                    "from_id": "u1",
                    "to_type": "Product",
                    "to_id": "p1",
                    "attributes": {"amount": 5},
                }
            ]
        }
    }

    result = tg.load_edges(payload)

    assert result["count"] == 1
    edges = tg.fetch_edges("PURCHASED")
    assert edges[0]["from_id"] == "u1"
