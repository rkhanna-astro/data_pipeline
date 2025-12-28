from apache_spark_pipeline.services.apache_spark_service import SparkService
from apache_spark_pipeline.services.mapping_service import MappingEngine
from apache_spark_pipeline.services.tigergraph_singleton import TIGERGRAPH_CLIENT
# from ..helpers.tigergraph_models import VertexMapping, EdgeMapping
from ..helpers.mappers import USER_VERTEX, PRODUCT_VERTEX, PURCHASE_EDGE

def run_sync(mode="batch", last_ts=None):
    spark_service = SparkService()
    tg = TIGERGRAPH_CLIENT

    if mode == "batch":
        users = spark_service.read_batch("main.sales.users")
        products = spark_service.read_batch("main.sales.products")
        purchases = spark_service.read_batch("main.sales.purchases")
    else:
        users = spark_service.read_microbatch("main.sales.users", last_ts)
        products = spark_service.read_microbatch("main.sales.products", last_ts)
        purchases = spark_service.read_microbatch("main.sales.purchases", last_ts)

    
    user_records = dataframe_to_records(users)
    product_records = dataframe_to_records(products)
    purchase_records = dataframe_to_records(purchases)

    print("RAW", user_records)
    
    mapper = MappingEngine()

    mapper.add_vertex_mapping(USER_VERTEX)
    mapper.add_vertex_mapping(PRODUCT_VERTEX)
    mapper.add_edge_mapping(PURCHASE_EDGE)

    users_payload = mapper.transform_records("users", user_records)
    products_payload = mapper.transform_records("products", product_records)
    purchases_payload = mapper.transform_records("purchases", purchase_records)

    print("Transformed", users_payload)

    tg.load_vertices(users_payload)
    tg.load_vertices(products_payload)
    tg.load_edges(purchases_payload)

    spark_service.stop_service()

def dataframe_to_records(dataframe):
    records = []

    for row in dataframe.collect():
        if isinstance(row, dict):
            records.append(row)

    return records
