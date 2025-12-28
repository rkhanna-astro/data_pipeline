from .tigergraph_models import VertexMapping, EdgeMapping

USER_VERTEX = VertexMapping(
    table="users",
    vertex_type="User",
    primary_id="user_id",
    attributes=["name", "email"],
    type_conversions={
        "user_id": "string",
        "name": "string",
        "email": "string"
    }
)

PRODUCT_VERTEX = VertexMapping(
    table="products",
    vertex_type="Product",
    primary_id="product_id",
    attributes=["name", "price"],
    type_conversions={
        "product_id": "string",
        "name": "string",
        "price": "double"
    }
)

PURCHASE_EDGE = EdgeMapping(
    table="purchases",
    edge_type="PURCHASED",
    from_vertex_type="User",
    to_vertex_type="Product",
    from_id="user_id",
    to_id="product_id",
    attributes=["amount", "ts"],
    type_conversions={
        "amount": "double",
        "ts": "datetime"
    }
)
