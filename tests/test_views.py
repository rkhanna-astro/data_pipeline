import pytest
from django.test import Client
from apache_spark_pipeline.services.tigergraph_singleton import TIGERGRAPH_CLIENT

pytestmark = pytest.mark.django_db

@pytest.fixture(autouse=True)
def reset_tigergraph():
    TIGERGRAPH_CLIENT.vertices.clear()
    TIGERGRAPH_CLIENT.edges.clear()
    yield

def test_sync_batch_view():
    client = Client()

    response = client.get("/api/sync/batch/")

    assert response.status_code == 200
    assert response.json()["status"] == "Batch sync completed"

def test_sync_micro_view_filters_data():
    client = Client()

    response = client.get(
        "/api/sync/micro/",
        {"last_timestamp": "2024-01-20"}
    )

    assert response.status_code == 200
    assert response.json()["status"] == "Micro-batch sync completed"

def test_sync_micro_missing_timestamp():
    client = Client()

    response = client.get("/api/sync/micro")

    assert response.status_code in (301, 400, 500)

def test_users_view_returns_data():
    client = Client()

    # Load data first
    client.get("/api/sync/batch/")
    response = client.get("/api/graph/users/")

    assert response.status_code == 200

    data = response.json()
    assert isinstance(data, dict)
    assert len(data) > 0

def test_products_view_returns_data():
    client = Client()

    client.get("/api/sync/batch/")
    response = client.get("/api/graph/products/")

    assert response.status_code == 200
    data = response.json()
    assert len(data) > 0

def test_purchases_view_returns_edges():
    client = Client()

    client.get("/api/sync/batch/")
    response = client.get("/api/graph/purchases/")

    assert response.status_code == 200

    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0




