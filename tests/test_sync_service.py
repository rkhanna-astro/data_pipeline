from apache_spark_pipeline.services.sync_service import run_sync
from apache_spark_pipeline.services.tigergraph_singleton import TIGERGRAPH_CLIENT

def test_run_sync_batch_end_to_end():
    # Reset singleton state
    TIGERGRAPH_CLIENT.vertices.clear()
    TIGERGRAPH_CLIENT.edges.clear()

    run_sync("batch")

    users = TIGERGRAPH_CLIENT.fetch_vertices("User")
    products = TIGERGRAPH_CLIENT.fetch_vertices("Product")
    purchases = TIGERGRAPH_CLIENT.fetch_edges("PURCHASED")

    assert len(users) > 0
    assert len(products) > 0
    assert len(purchases) > 0

def test_micro_sync_loads_only_new_data():
    TIGERGRAPH_CLIENT.vertices.clear()
    TIGERGRAPH_CLIENT.edges.clear()

    run_sync(
        mode="micro",
        last_ts="2024-01-20T00:00:00"
    )

    users = TIGERGRAPH_CLIENT.fetch_vertices("User")

    assert users  # some data loaded
    assert all(
        user["updated_at"] > "2024-01-20T00:00:00"
        for user in users.values()
    )
