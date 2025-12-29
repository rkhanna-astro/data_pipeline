import pytest
from datetime import datetime

from apache_spark_pipeline.services.mapping_service import MappingEngine
from apache_spark_pipeline.helpers.tigergraph_models import (
    VertexMapping,
    EdgeMapping,
)


@pytest.fixture
def engine():
    return MappingEngine()


def test_vertex_mapping_success(engine):
    mapping = VertexMapping(
        table="users",
        vertex_type="User",
        primary_id="id",
        attributes=["name", "email"],
        type_conversions={"id": "string"},
    )

    engine.add_vertex_mapping(mapping)

    records = [
        {"id": "u1", "name": "Alice", "email": "a@test.com"},
        {"id": "u2", "name": "Bob", "email": "b@test.com"},
    ]

    payload = engine.transform_records("users", records)

    assert "vertices" in payload
    assert "User" in payload["vertices"]
    assert payload["vertices"]["User"]["u1"]["name"] == "Alice"


def test_edge_mapping_success(engine):
    mapping = EdgeMapping(
        table="purchases",
        edge_type="PURCHASED",
        from_vertex_type="User",
        to_vertex_type="Product",
        from_id="user_id",
        to_id="product_id",
        attributes=["amount"],
        type_conversions={"amount": "double"},
    )

    engine.add_edge_mapping(mapping)

    records = [
        {"user_id": "u1", "product_id": "p1", "amount": 10}
    ]

    payload = engine.transform_records("purchases", records)

    edges = payload["edges"]["PURCHASED"]
    assert len(edges) == 1
    assert edges[0]["attributes"]["amount"] == 10.0


def test_convert_all_types(engine):
    assert engine._convert("1", {"a": "int"}, "a") == 1
    assert engine._convert("1.5", {"a": "double"}, "a") == 1.5
    assert engine._convert("x", {"a": "string"}, "a") == "x"
    assert engine._convert(True, {"a": "bool"}, "a") is True
    assert engine._convert(datetime(2024, 1, 1), {}, "a").startswith("2024")


def test_missing_mapping_raises(engine):
    with pytest.raises(ValueError):
        engine.transform_records("unknown", [])
