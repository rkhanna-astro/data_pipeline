from typing import List, Dict

class SparkBatchPipeline:
    def __init__(self, uc_client, tg_client, mapper, tables: List[str], 
                 micro_batch_size: int = 10000, parallelism: int = 4):
        self.unity_catalog_client = uc_client
        self.tigergraph_client = tg_client
        self.mapper = mapper
        self.tables = tables
        self.micro_batch_size = micro_batch_size
        self.parallelism = parallelism
        self.stats = {'vertices': 0, 'edges': 0, 'tables': 0, 'errors': [], 'batches': 0}
        
        # In real-life, for simplicity
        # self.spark = SparkSession.builder \
        #     .appName("IcebergToTigerGraph") \
        #     .config("spark.sql.catalog.unity", "org.apache.iceberg.spark.SparkCatalog") \
        #     .config("spark.sql.catalog.unity.catalog-impl", "com.databricks.unity.iceberg.UnityCatalog") \
        #     .getOrCreate()
    
    def run(self):        
        try:
            # Initialize and check the Iceberg configs
            self.show_config()
            
            # fetching and going through Iceberg tables
            for table in self.tables:
                # "main.ecommerce.users",
                # "main.ecommerce.products",
                # "main.ecommerce.transactions"
                print(f"Processing Table", table)
                self.process_table_with_spark(table)
                                    
        except Exception as e:
            print(f"Pipeline failed: {e}", exc_info=True)
            raise
    
    def show_config(self):
        print(self.mapper.get_summary())
    
    def process_table_with_spark(self, table: str):
        try:
            # Get metadata
            # GET /api/2.1/unity-catalog/tables/main.ecommerce.users (Databricks Unity Catalog internally)
            meta = self.unity_catalog_client.get_table_metadata(table)
            
            # Mock: Read all data
            # In production: df = spark.read.format("iceberg").table(table)
            # Internally, uses Metdata to fetch the real files (parquet/CSVs) from S3
            all_records = self.unity_catalog_client.read_table_data(meta)
            total_records = len(all_records)
            print(f"Total records: {total_records:,}")
            
            if not all_records:
                print("No Data Found")
                return
            
            # Mock spark partitioning
            # df = df.repartition(self.parallelism)
            print(f"Repartitioning into {self.parallelism} partitions")
            partitions = self.create_partitions(all_records, self.parallelism)
            
            # Process each partition (mocking Spark's foreachPartition)
            # df.foreachPartition(lambda partition: self._process_partition(table, partition))
            print(f"Processing {len(partitions)} partitions in micro-batches")
            for partition_id, partition_data in enumerate(partitions):
                self.process_partition_in_microbatches(table, partition_id, partition_data)
            
            self.stats['tables'] += 1
            
        except Exception as e:
            print(f"Error: {e}")
            self.stats['errors'].append(f"{table}: {e}")
    
    def create_partitions(self, records: List[Dict], num_partitions: int):
        # Creating partitions based on set parrelelism and total records
        partition_size = (len(records) + num_partitions - 1) // num_partitions
        partitions = []
        
        for i in range(0, len(records), partition_size):
            partitions.append(records[i:i + partition_size])
        
        return partitions
    
    def process_partition_in_microbatches(self, table: str, partition_id: int, partition_data: List[Dict]):
        #Process a single spark partition in micro-batches
        partition_size = len(partition_data)

        # Process partition in micro-batches        
        for batch_num, batch_start in enumerate(range(0, partition_size, self.micro_batch_size)):
            batch_records = partition_data[batch_start:batch_start + self.micro_batch_size]

            try:
                # Transform vertices and edges to graph format
                print(f"Iceberg Data: {batch_records}")
                graph_data = self.mapper.transform_records(table, batch_records)
                print(f"TigerGraph Compatible Data: {graph_data}")
                # Push to TigerGraph
                vertices_count = len(graph_data['vertices'])
                edges_count = len(graph_data['edges'])

                # if vertices_count > 0 and edges_count > 0:
                # these two can also be combined into one REST++ API call
                # as shown in https://docs.tigergraph.com/tigergraph-server/4.2/api/upsert-rest#_endpoint_url
                if vertices_count > 0:
                    self.push_vertices_to_tigergraph(graph_data['vertices'])

                elif edges_count > 0:
                    self.push_edges_to_tigergraph(graph_data['edges'])
                
                # Update stats
                self.stats['vertices'] += vertices_count
                self.stats['edges'] += edges_count
                self.stats['batches'] += 1
                
                print(f"Batch pushed to TigerGraph")
                
            except Exception as e:
                print(f"Batch error: {e}")
                self.stats['errors'].append(f"Partition {partition_id}, Batch {batch_num}: {e}")
    
    def push_vertices_to_tigergraph(self, vertices: List[Dict]):
        if vertices:
            vertices_by_type = {}
            for v in vertices:
                vertices_by_type.setdefault(v['v_type'], []).append(v)
            
            for vtype, vlist in vertices_by_type.items():
                # REST++ API call to push new vertices to TigerGraphDB
                # POST http://localhost:9000/graph/ecommerce
                result = self.tigergraph_client.upsert_vertices(vtype, vlist, batch_size=1000)
                print(f"{vtype}: {result['total_accepted']} vertices")
        
    def push_edges_to_tigergraph(self, edges: List[Dict]):
        if edges:
            edges_by_type = {}
            for e in edges:
                edges_by_type.setdefault(e['e_type'], []).append(e)
            
            for etype, elist in edges_by_type.items():
                # POST http://localhost:9000/graph/ecommerce
                result = self.tigergraph_client.upsert_edges(etype, elist, batch_size=1000)
                print(f"{etype}: {result['total_accepted']} edges")

"""
In production, replace the mock with real PySpark:

from pyspark.sql import SparkSession

spark = SparkSession.builder \\
    .appName("IcebergToTigerGraph") \\
    .config("spark.sql.catalog.unity", "org.apache.iceberg.spark.SparkCatalog") \\
    .config("spark.sql.catalog.unity.uri", databricks_url) \\
    .getOrCreate()

# Read Iceberg table
df = spark.read.format("iceberg").table("main.ecommerce.users")

# Repartition for parallelism
df = df.repartition(32)  # 32 partitions = 32 parallel executors

# Process each partition
def process_partition(partition_iterator):
    # This runs on each Spark executor
    tg_client = TigerGraphClient(...)  # Create client per executor
    
    for batch in chunks(partition_iterator, 10000):  # Micro-batches
        graph_data = transform_to_graph(batch)
        tg_client.upsert_vertices(graph_data['vertices'])
        tg_client.upsert_edges(graph_data['edges'])

df.foreachPartition(process_partition)

Benefits:
- Parallel processing across Spark cluster
- Fault tolerance (Spark retries failed tasks)
- Memory efficient (streaming processing)
- Scalable to billions of records
    """