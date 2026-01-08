from .tigergraph_models import VertexMapping, EdgeMapping

USER_VERTEX = VertexMapping(
    table="users",
    vertex_type="User",
    primary_id="user_id",
    attributes=["name", "email", "updated_at"],
    type_conversions={
        "user_id": "string",
        "name": "string",
        "email": "string",
        "updated_at": "datetime",
    }
)

PRODUCT_VERTEX = VertexMapping(
    table="products",
    vertex_type="Product",
    primary_id="product_id",
    attributes=["name", "price", "updated_at"],
    type_conversions={
        "product_id": "string",
        "name": "string",
        "price": "double",
        "updated_at": "datetime",
    }
)

PURCHASE_EDGE = EdgeMapping(
    table="purchases",
    edge_type="Purchased",
    from_vertex_type="User",
    to_vertex_type="Product",
    from_id="user_id",
    to_id="product_id",
    attributes=["amount", "updated_at", "ordered_at"],
    type_conversions={
        "amount": "double",
        "updated_at": "datetime",
        "ordered_at": "datetime",
    }
)
