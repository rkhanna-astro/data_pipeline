# Data Pipeline: Iceberg to TigerGraph Integration

In this project, I have designed a skeleton of data-pipeline that continously migrates data from Apache Iceberg and ingests with appropriate data-format into TigerGraphDB. This documentation briefly covers the how to run the code, what each file do, design decision implemented, tradeoffs of the current implementation and future work.

---

## Project Structure

```
data_pipeline/
├── config/
│   ├── table_mappings.yaml          # Table to graph mapping definitions
├── schemas/
│   └── tigergraph_schema.gsql         # TigerGraph graph schema (DDL)
├── models/
│   └── iceberg_table_models.py      # Iceberg Data classes
│   └── tigergraph_models.py         # TigerGraph Data classes
├── unity_catalog_framework.py       # Unity Catalog client (Iceberg metadata)
├── tigergraph_client_framework.py   # TigerGraph client (graph loading)
├── mapping_framework.py             # Iceberg → Graph transformation logic
├── spark_batching_framework.py      # Spark batch/micro-batch processing
├── main.py                          # Application entry point
├── design_document.pdf              # Detailed architecture documentation
├── .env.example                     # Environment variables template
└── README.md                        # This file
```

![Pipeline Architecture](pipeline_architecture.png)

---

## What This Pipeline Does

The pipeline Ideveloped solves the problem of **transforming data (tables) stored in Apache Iceberg into graph structures (vertices and edges) in TigerGraph**, potentially enabling advanced graph analytics.

**Key Features:**
- **Scalable**: Processes millions of records using Apache Spark and pushes data to TigerGraphDB
- **Flexible**: Two loading methods: REST++ API (Implemented) and GSQL Loading Jobs (Ongoing)
- **Governed**: Unity Catalog provides access control and lineage tracking
- **Configuration-driven**: Add new tables without code changes

---

## Component Overview

### Core Components

#### 1. `unity_catalog_framework.py` - Data Source Interface

**Purpose**: Connects to Databricks Unity Catalog to access Iceberg table metadata and read data.

**Main Component**:
```python
# Fetches table schema, location, columns
# metadata = uc_client.get_table_metadata("main.ecommerce.users")

# Reads actual data from Iceberg (Parquet files on S3)
# records = uc_client.read_table_data(metadata, limit=1000000)

def get_table_metadata(self, full_table_name: str) -> IcebergTableMetadata:
        print(f"Fetching metadata: {full_table_name}")
        data = self._mock_api_response(full_table_name)
        
        metadata = IcebergTableMetadata(
            catalog_name=data["catalog_name"],
            schema_name=data["schema_name"],
            table_name=data["name"],
            table_type=data["table_type"],
            data_source_format=data["data_source_format"],
            storage_location=data["storage_location"],
            columns=[
                IcebergColumn(name=col["name"], type_name=col["type_name"],
                            nullable=col.get("nullable", True), comment=col.get("comment"))
                for col in data["columns"]
            ],
            properties=data.get("properties", {})
        )
        return metadata
```

**Capabilities**:
- Retrieves table metadata (schema, S3 location, column types)
- Reads Iceberg data files
- Handles Unity Catalog authentication and permissions
- Supports incremental reads via Iceberg snapshots

---

#### 2. `tigergraph_client_framework.py` - Graph Database Interface

**Purpose**: Communicates with TigerGraph to load graph data using two optimized methods.

**Two Loading Methods**:

**Method 1: REST++ API** - For incremental updates
```python
# Good for real-time or micro-batch uploads
# throughput: ~10,000 records/second
# best for: Hourly updates, real-time ingestion

# client.upsert_vertices("User", vertices_list, batch_size=1000)
# client.upsert_edges("PURCHASED", edges_list, batch_size=1000)

def upsert_vertices(self, vertex_type: str, vertices: List[Dict], batch_size: int = 1000) -> Dict:
        print(f"Upserting {len(vertices)} {vertex_type} vertices")
        
        if vertices:
            sample = vertices[0]
            print(f"Sample: {vertex_type}({sample['v_id']}) = {sample['attributes']}")
            objects = []

            for vertice in vertices:
                if vertex_type == 'User':
                    user = User()
                    user.user_id = vertice['v_id']

                    attributes = vertice['attributes']
                    user.username = attributes['username']
                    user.created_at = attributes['created_at']
                    user.email = attributes['email']
                    user.country = attributes['country']

                    objects.append(user)
                
                if vertex_type == 'Product':
                    product = Product()
                    product.product_id = vertice['v_id']

                    attributes = vertice['attributes']
                    product.category = attributes['category']
                    product.price = attributes['price']
                    product.name = attributes['name']

                    objects.append(product)
        
        json_response = json.dumps(objects)
        print(f"This is JSON response", json_response)
        return {
            "vertices": json_response,
            "vertex_type": vertex_type,
            "total_accepted": len(vertices),
            "total_skipped": 0
        }
```

**Key capabilities**:
- REST++ API for real-time/incremental loading
- Automatic batching (and retry logic in future)
- Transform and saves data in TigerGraph DB client

**Method 2: GSQL Loading Jobs** - Batch load data loads

Working on a method that will allow us to batch upload vertices and edges through GSQL
loading jobs provided by TigerGraph DB clusters

```python
create_loading_job_for_edges(
    edge_type=edge_type,
    from_vertex_type=from_vertex_type,
    to_vertex_type=to_vertex_type,
    job_name=job_name,
    s3_file_path=s3_path,
    columns={
        'from_id': 'from_id',
        'to_id': 'to_id',
        'attributes': [k for k in columns if k not in ['from_id', 'to_id']]
    }
)
```

**Key capabilities**:
- Allow batch-loading with millions of rows per second
- Strongly supported by TigerGraph DB client
- No need worry about duplicacy
---

#### 3. `mapping_framework.py` - Data Transformation Engine

**Purpose**: Transforms Iceberg data (rows/columns) into graph data (vertices/edges).

**Example transformation**:
```python
# Relational record
{
    "user_id": 12345,
    "username": "alice",
    "email": "alice@example.com",
    "created_at": "2024-01-15T10:30:00Z"
}

# Transformed to Graph vertex
{
    "v_type": "User",
    "v_id": "12345",           
    "attributes": {
        "username": "alice",
        "email": "alice@example.com",
        "created_at": "2024-01-15 10:30:00"  # datetime
    }
}
```
**Key capabilities**:
- Type conversion (e.g., `int` → `string` for IDs)
- Timestamp formatting (`ISO8601` → `YYYY-MM-DD HH:MM:SS`)
- Configuration-driven (dependent on )

**Why do we need strict mapping conversions?**:
- TigerGraph IDs should be strings (flexible, no overflow)
- Timestamps need specific datetime format (datetime in Python)
- Decimal types converted to double

---

#### 4. `spark_batching_framework.py` - Pipeline Orchestration

**Purpose**: Using Apache Spark we perform end-to-end ETL job incorporating parallel, scalable processing given by Apache Spark.

**SparkRestPipeline** - For incremental updates
```
Process:
1. Read Iceberg table via Spark
2. Partition data for parallel processing
3. Transform in micro-batches (10K records)
4. Push to TigerGraph via REST++ API

Best for:
- Hourly/real-time updates
- small number of reacords (~10000)
- Simple setup (no S3 staging)
```

```python
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
```

**SparkGSQLPipeline** - For bulk loads
```
Under Construction
```

**Advantages of using Spark**:
- Parallel processing (multiple executors)
- Memory-efficient micro-batching
- Automatic retries on failure
- Horizontal scalability

---

#### 5. `main.py` - Application Entry Point

**Purpose**: Initializes all components and runs the pipeline.

**What it does**:
```
Load environment variables (.env)
Initialize Unity Catalog client
Initialize TigerGraph client
Configure mappings from YAML
Choose pipeline method (REST++)
Execute pipeline
```

**Usage**:
```bash
python main.py

# The pipeline in the end create Python Objects in memory (User, Product) which acts as 
# vertices and edges created in TigerGraphDB.
```

```python
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
```
---

### Configuration Files

#### `config/table_mappings.yaml`

**Purpose**: Defines how Iceberg tables map to TigerGraph graph elements.

**Example**:
```yaml
version: "1.0"

mappings:
  # Vertex mapping: users table → User vertices
  - table: "main.ecommerce.users"
    type: vertex
    vertex_type: User
    primary_id: user_id
    attributes:
      - username
      - email
      - created_at
      - country
    type_conversions:
      user_id: string        # Convert bigint to string
      created_at: datetime   # Format timestamp
```

**Advantage**: Just add a new mapping in this yaml and no further code changes needed

---

#### `schemas/ecommerce_graph.gsql`

**Purpose**: Defines the TigerGraph graph structure (vertices, edges, graph). This needs to be created and exectued to create a graph before pushing data into TigerGraph DB clusters.

**Example**:
```sql
-- create vertex
CREATE VERTEX User (
    PRIMARY_ID user_id STRING,
    username STRING,
    email STRING,
    created_at DATETIME,
    country STRING
) WITH primary_id_as_attribute="true"

-- create edge
CREATE DIRECTED EDGE PURCHASED (
    FROM User,
    TO Product,
    transaction_id STRING,
    amount DOUBLE,
    timestamp DATETIME
) WITH REVERSE_EDGE="PURCHASED_BY"

-- create graph
CREATE GRAPH EcommerceGraph (User, Product, PURCHASED)
```
---

## How to Run

### Prerequisites

- **Python 3.2+**
- **Desired Access (needed in production or live environments)**:
  - Databricks workspace with Unity Catalog
  - TigerGraph instance (v3.0+)
  - AWS S3 (for GSQL staging)
- **Credentials** for all services

### Step 1: Install Dependencies

```bash
# Clone repository
git clone https://github.com/rkhanna-astro/data_pipeline.git
cd data_pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install packages (NO packages as of now)
# pip install -r requirements.txt
```

### Step 2: Configure Environment

```bash
# Copy environment template
cp .env.example .env

# Edit with your credentials
nano .env  # or use your preferred editor
```

**Required environment variables**:
```bash
# databricks Unity Catalog
DATABRICKS_WORKSPACE_URL=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi-your-token

# tigerGraph
TIGERGRAPH_HOST=https://your-tigergraph.com
TIGERGRAPH_GRAPH_NAME=EcommerceGraph
TIGERGRAPH_USERNAME=tigergraph
```

### Step 3: Configure Mappings

Esnure `config/table_mappings.yaml` match tables in both Iceberg and also in TigerGraph (type conversions).

### Step 4: Run Pipeline

```bash
# Basic run
python main.py
```

### Step 5: Monitor Progress

Confirm that all the rows from Iceberg Tables have been transformed into vertices and edges in TigerGraph DB clusters.

```python
TigerGraph Compatible Data: {'vertices': [], 'edges': [{'e_type': 'PURCHASED', 'from_type': 'User', 'from_id': '1', 'to_type': 'Product', 'to_id': '5003', 'attributes': {'transaction_id': '103', 'amount': 15.99, 'timestamp': '2024-03-02 09:20:00'}}, {'e_type': 'PURCHASED', 'from_type': 'User', 'from_id': '3', 'to_type': 'Product', 'to_id': '5001', 'attributes': {'transaction_id': '104', 'amount': 1299.99, 'timestamp': '2024-03-03 16:00:00'}}]}

Upserting 2 PURCHASED edges

Sample: User(1) -[PURCHASED]-> Product(5003)

PURCHASED: 2 edges

Batch pushed to TigerGraph
```

## Design Overview

### Architecture Diagram


### Data Flow (Step-by-Step)

```
1. Unity Catalog Query
   └─> Fetch metadata: schema, S3 location, columns

2. Iceberg Read (Spark)
   └─> Read (Parquet/CSV) files from S3 in parallel

3. Transform (Mapping Engine)
   ├─> Convert types
   ├─> Create vertices (User, Product)
   └─> Create edges (PURCHASED)

4. Load to TigerGraph
   ├─> Option A: REST++ API (< 10M records)
   │   └─> Direct HTTP POST
   └─> Option B: GSQL Job (under construction)
       ├─> Write CSV to S3
       ├─> Create GSQL loading job
       └─> Execute bulk load (100x faster!)

5. Validation
   └─> Try to query TigerGraph and match total count
```
---

## Architecture Tradeoffs

### 1. Batch vs. Streaming

**my Choice: Batch Processing**

I have used micro-batch and batch processing implementation frameworks. This means that the pipeline does not support live-streaming or real-time update that maybe required in many cases.
However, given the ease of implementation, Iceberg's event-driven limitations, TigerGraph push based APIs and better performance I have chosen this approach. Theferore, my latencies are on higher-sides but achieves good data-ingestion performance with minimal network overheads.

---

### 2. REST++ API vs. GSQL Loading Jobs

**My Choice: Both (if possible)**

**Decision rule**:
- Go with REST ++ if needed almost real-time updates through micro-batching.
- Go with GSQL loading jobs for first time migration, or migration on large scales (daily, monthly or yearly)
---

### 3. Push (Spark) vs. Pull (Kafka Connect)

**my Choice: Push with Spark**

**Why push wins**:
- Hard to implement Iceberg-Kafka Connect connector
- Kafka would require custom connector development
- TigerGraph is highly-performant when using batch uploads
- Spark natively connects well with both Iceberg and TigerGraph
---

### 5. Micro-Batching vs. Full Load

**my Choice: both (if possible)**

**Why I implemented micro-batching**:
- Memory-efficient
- Almost Real Time
- Enables parallelization

---

## Additional Documentation

- **Design Document** - See `design_document.pdf` for detailed architecture analysis
- **TigerGraph Docs** - [https://docs.tigergraph.com](https://docs.tigergraph.com)
- **Iceberg Docs** - [https://iceberg.apache.org](https://iceberg.apache.org)
- **Unity Catalog** - [Databricks Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/)

---

