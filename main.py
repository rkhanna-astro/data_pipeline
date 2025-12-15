from unity_catalog_framework import UnityCatalogClient
from tigergraph_client_framework import TigerGraphClient
from mapping_framework import MappingEngine, VertexMapping, EdgeMapping
from spark_batching_framework import SparkBatchPipeline

def main():
    # Initialize clients
    unity_catalog_client = UnityCatalogClient("https://my-workspace.databricks.com", "dapi-token-123")
    tiger_graph_client = TigerGraphClient("https://my-tigergraph.com", "ecommerceGraph")
    
    # Configure mappings
    mapper = MappingEngine()
    
    # Can be created using table metadata stored in table_mappings.yaml
    mapper.add_vertex_mapping(VertexMapping(
        table="main.ecommerce.users",
        vertex_type="User",
        primary_id="user_id",
        attributes=["username", "email", "created_at", "country"],
        type_conversions={"user_id": "string", "created_at": "datetime"}
    ))
    
    mapper.add_vertex_mapping(VertexMapping(
        table="main.ecommerce.products",
        vertex_type="Product",
        primary_id="product_id",
        attributes=["name", "category", "price"],
        type_conversions={"product_id": "string", "price": "double"}
    ))
    
    mapper.add_edge_mapping(EdgeMapping(
        table="main.ecommerce.transactions",
        edge_type="PURCHASED",
        from_vertex_type="User",
        to_vertex_type="Product",
        from_id="user_id",
        to_id="product_id",
        attributes=["transaction_id", "amount", "timestamp"],
        type_conversions={
            "user_id": "string",
            "product_id": "string",
            "transaction_id": "string",
            "amount": "double",
            "timestamp": "datetime"
        }
    ))
    
    # Run Spark-based micro-batch pipeline
    pipeline = SparkBatchPipeline(
        uc_client=unity_catalog_client,
        tg_client=tiger_graph_client,
        mapper=mapper,
        tables=[
            "main.ecommerce.users",
            "main.ecommerce.products",
            "main.ecommerce.transactions"
        ],
        micro_batch_size=2,  # Small batch size for demo (maybe much higher in real-life prod)
        parallelism=2  # Number of Spark partitions (higher for production)
    )
    
    pipeline.run()

if __name__ == "__main__":
    main()
