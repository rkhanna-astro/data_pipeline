# Data Pipeline: Iceberg to TigerGraph Integration

In this project, I have designed a skeleton of data-pipeline that continously migrates data from Apache Iceberg and ingests with appropriate data-format into TigerGraphDB. This documentation briefly covers the how to run the code, what each file do, design decision implemented, tradeoffs of the current implementation and future work.

---

## Table of contents

- Data Pipeline Project
- Data Architecture Diagram
- Functionality 
- Core Components
- API workflow (endpoints)
- Files & locations
- Quick usage examples
- Testing
- Setup
- Design Decision
- Development notes & next steps
- Additional Documentation

---

## Data Architecture Diagram

![Pipeline Architecture](pipeline_architecture.png)

---

## Functionality 

The pipeline I developed solves the problem of **transforming data (tables) stored in Apache Iceberg into graph structures (vertices and edges) in TigerGraph**, potentially enabling advanced graph analytics.

**Key Features:**
- **Scalable**: Processes millions of records using Apache Spark and pushes data to TigerGraphDB
- **Flexible**: Two loading methods: REST++ API (Implemented) and GSQL Loading Jobs (Ongoing)
- **Governed**: Unity Catalog provides access control and lineage tracking
- **Configuration-driven**: Add new tables without code changes


---

## Core Components

Below are the main modules, their responsibilities, inputs and outputs.

1. API views (HTTP entry points through apache_spark application)
    - Files: 
        - `apache_spark_pipeline/views.py` 
        - `apache_spark_pipeline/urls.py`
    - Responsibilities:
        - Expose endpoints described above.
        - Validate micro-batch `last_timestamp` (via `datetime.fromisoformat` in views).
        - Invoke the central `run_sync` spark job
        - Return JSON responses and graph data from the TigerGraph client

2. Sync orchestration
    - Files: 
        - `apache_spark_pipeline/services/sync_service.py`
    - Responsibilities:
        - Handle a full pipeline run: read tables, transform, and load.
        - Registers mapping definitions (USER_VERTEX, PRODUCT_VERTEX, PURCHASE_EDGE).
        - Converts SparkDataFrame rows into TigerGraph (in-memory) records.
        - Calls MappingEngine to produce TigerGraph compatible data format.
        - Calls TigerGraph client to upsert vertices and edges.
    - Inputs: mode ("batch" or "micro") and optional timestamp for micro.

3. Spark service
    - Files: 
        - `apache_spark_pipeline/services/apache_spark_service.py`
    - Responsibilities:
        - Provide `read_batch(table)` and `read_microbatch(table, last_ts)` methods that return `SparkDataFrame` mock.
        - Wrap UnityCatalog and Spark (dataframe) behavior behind one interface.
        - `stop_service()` upon the job completion.

4. Unity Catalog + Spark mocks
    - Files:
        - `apache_spark_pipeline/helpers/unity_data_catalog.py` (UnityCatalog)
        - `apache_spark_pipeline/helpers/spark.py` (Spark)
        - `apache_spark_pipeline/helpers/spark_data_frame.py` (SparkDataFrame)
    - Responsibilities:
        - UnityCatalog returns a SparkDataFrame for predefined mock table names.
        - Spark wraps datasets into list of objects of SparkDataFrame; SparkDataFrame supports `.filter(expr)` and `.collect()`.
        - Filter supports simple ISO timestamp comparison on `updated_at`

5. Mapping layer (transformation engine)
    - Files:
        - `apache_spark_pipeline/services/mapping_service.py` (MappingEngine)
        - `apache_spark_pipeline/helpers/tigergraph_models.py` (VertexMapping, EdgeMapping dataclasses)
        - `apache_spark_pipeline/helpers/mappers.py` (predefined USER, PRODUCT, PURCHASE mappings)
    - Responsibilities:
        - Maintain a registry of mappings by source table.
        - Convert records into TigerGraph-compatible payloads:
        - Vertex payload: { "vertices": { VertexType: { id: attributes } } }
        - Edge payload: { "edges": { EdgeType: [ { from_type, from_id, to_type, to_id, attributes } ] } }
        - Perform type conversions: string, int, double, bool, datetime (datetime normalized to ISO).
        - per-record error handling.

6. TigerGraph mock and singleton client
    - Files:
        - `apache_spark_pipeline/services/tigergraph_service.py` (in-memory client)
        - `apache_spark_pipeline/services/tigergraph_singleton.py` (TIGERGRAPH_CLIENT)
    - Responsibilities:
        - Store `vertices` as mapping vertex_type -> list of vertex objects
        - Store `edges` as mapping edge_type -> list of edge objects
        - Provide `upsert_vertices(payload)`, `upster_edges(payload)`, `fetch_vertices(vtype)`, `fetch_edges(etype)`

7. Mock datasets and utilities
    - Files:
        - `apache_spark_pipeline/helpers/mock_iceberg_data.py`
        - `apache_spark_pipeline/helpers/mock_users.py`, `mock_products.py`, `mock_purchases.py`
    - Responsibilities:
        - Provide test users, products, purchaes datasets (with ISO timestamps) used for tests/demo runs.

8. TigerGraph Data Schema
    - Files:
        - `schemas/ecommerce_graph.gsql`
    - Responsibilities:
        - Defines the TigerGraph graph structure (vertices, edges, graph). 
        - Note: This needs to be created and exectued to create a graph before pushing data into TigerGraph DB clusters.
---

## API Workflow (endpoints)

The API format is highly influenced from TigerGraph Documentation. The JSON responses for fetching or listing vertices and
edges is inspired from [TigerGraph List Vertices API](https://docs.tigergraph.com/tigergraph-server/4.2/api/built-in-endpoints#_list_vertices) and [TigerGraph List Edges API](https://docs.tigergraph.com/tigergraph-server/4.2/api/built-in-endpoints#_list_edges_of_a_vertex).

- GET /api/sync/batch/
  - Handler: [`apache_spark_pipeline.views.sync_batch`](apache_spark_pipeline/views.py)
  - Triggers a full batch pipeline run: [`apache_spark_pipeline.services.sync_service.run_sync`](apache_spark_pipeline/services/sync_service.py) with mode `"batch"`.
  - Response example: `{ "status": "Batch sync completed" }` (200).

- GET /api/sync/micro/?last_timestamp
  - Handler: [`apache_spark_pipeline.views.sync_micro`](apache_spark_pipeline/views.py)
  - Requires query param `last_timestamp` (ISO 8601). The view validates via Python `datetime.fromisoformat`.
  - On success runs [`run_sync`](apache_spark_pipeline/services/sync_service.py) with mode `"micro"` and the provided timestamp.
  - Responses:
    - 200 `{ "status": "Micro-batch sync completed" }`
    - 400 when `last_timestamp` missing or invalid.

- GET /api/graph/users/
  - Handler: [`apache_spark_pipeline.views.users`](apache_spark_pipeline/views.py)
  - Return a list of `User` vertices as JSON response.
  - Response Example:
```json 
[
  {
    "v_type": "User",
    "v_id": "u19",
    "attributes": {
      "name": "User-19",
      "email": "user19@example.com",
      "updated_at": "2026-01-20T10:00:00"
    }
  },
  {
    "v_type": "User",
    "v_id": "u20",
    "attributes": {
      "name": "User-20",
      "email": "user20@example.com",
      "updated_at": "2026-01-21T10:00:00"
    }
  },
]
```

- GET /api/graph/products/
  - Handler: [`apache_spark_pipeline.views.products`](apache_spark_pipeline/views.py)
  - Returns a list of `Product` vertices as JSON response.
```json
[
  {
    "v_type": "Product",
    "v_id": "p10",
    "attributes": {
      "name": "Product-10",
      "price": 25,
      "updated_at": "2026-01-21T09:00:00"
    }
  },
  {
    "v_type": "Product",
    "v_id": "p11",
    "attributes": {
      "name": "Product-11",
      "price": 26.5,
      "updated_at": "2026-01-23T09:00:00"
    }
  },
  {
    "v_type": "Product",
    "v_id": "p12",
    "attributes": {
      "name": "Product-12",
      "price": 28,
      "updated_at": "2026-01-25T09:00:00"
    }
  },
]
```

- GET /api/graph/purchases/
  - Handler: [`apache_spark_pipeline.views.purchases`](apache_spark_pipeline/views.py)
  - Returns list of `PURCHASED` edges as JSON response.
```json
[
  {
    "e_type": "PURCHASED",
    "directed": false,
    "from_type": "User",
    "from_id": "u99",
    "to_type": "Product",
    "to_id": "p48",
    "attributes": {
      "amount": 186.01,
      "updated_at": "2026-01-20T11:00:00",
      "ordered_at": "2026-01-20T11:00:00"
    }
  },
  {
    "e_type": "PURCHASED",
    "directed": false,
    "from_type": "User",
    "from_id": "u150",
    "to_type": "Product",
    "to_id": "p4",
    "attributes": {
      "amount": 228.68,
      "updated_at": "2026-01-21T11:00:00",
      "ordered_at": "2026-01-21T11:00:00"
    }
  },
]

```

---

## Quick usage examples

Trigger a batch sync:
```bash
curl -s -X GET http://localhost:8000/api/sync/batch/
# -> {"status":"Batch sync completed"}
```

Trigger a micro-batch sync basted on a certain timestamp:
```bash
curl -s -G http://localhost:8000/api/sync/micro/ --data-urlencode "last_timestamp=2026-01-20"
# -> {"status":"Micro-batch sync completed"}
```

Query loaded user vertices:
```bash
curl -s http://localhost:8000/api/graph/users/
# JSON object mapping user_id - attributes
```

Query loaded products vertices:
```bash
curl -s http://localhost:8000/api/graph/products/
# JSON object mapping product_id - attributes
```

Query loaded purchases edges:
```bash
curl -s http://localhost:8000/api/graph/purchases/
# JSON object containing list of purchase edges mapped to attributes (ordered_at)
```

## Testing & Coverage

Currently, the test coverage has achieved **94%** coverage [Refer: htmlcov/index.html].

Run tests with pytest:
```bash
pip install -r requirements.txt   # if present
python3 -m pytest --cov=apache_spark_pipeline --cov-report=html
```

Tests cover:
- Mapping behavior (`tests/test_mapping_service.py`)
- Spark service mock & filtering (`tests/test_apache_spark_service.py`)
- TigerGraph mock load/fetch (`tests/test_tigergraph_service.py`)
- End-to-end sync orchestration (`tests/test_sync_service.py`)
- Views / API behavior (`tests/test_views.py`)
---

## Setup

```bash
git clone git@github.com:rkhanna-astro/data_pipeline.git
cd data_pipeline
python -m venv venv
source venv/bin/activate 
# source venv/Scripts/activate for Windows if needed
pip install -r requirements.txt
python manage.py runserver
```

## Design Decision

### Data Flow (Step-by-Step)

```
1. Unity Catalog Query
   └─> Fetch table metadata (schema, partitions, snapshot IDs)

2. Iceberg Read (Spark)
   └─> Parallel read from S3 using snapshot isolation

3. Transform (Mapping Engine)
   ├─> Enforce type compatibility through maping (Iceberg → TigerGraph)
   ├─> Vertex generation (User, Product)
   └─> Edge generation (PURCHASED)

4. Load to TigerGraph
   ├─> REST++ API (micro-batch or batch)
   └─> GSQL Loading Jobs (bulk ingestion / backfills)

5. Validation & Monitoring
   ├─> Count reconciliation
   ├─> Checkpointing / snapshot validation
   └─> Error logging & retries
```
---

## Architecture Tradeoffs and Design Decisions

This section explains the key architectural decisions made while designing the Iceberg-to-TigerGraph data pipeline, along with the tradeoffs involved.

---

### 1. Batch vs Streaming

**Chosen Approach: Batch and Micro-Batching**

The pipeline supports batch and micro-batch processing instead of true real-time streaming.

**Rationale:**
- Apache Iceberg is snapshot-based and optimized for analytical workloads.
- Strict streaming requires Kafka-based connectors, which significantly increase development complexity.
- TigerGraph ingestion is optimized for high-throughput batch or bulk uploads.

**Tradeoff:**
- Much better throughput, control over fault tolerance, predictability, and operational simplicity.
- Higher end-to-end latency compared to streaming systems.

**Outcome:**
- Near-real-time ingestion but tradeoffed with latency.
- Strong consistency through snapshot-based reads.

---

### 2. REST++ API vs GSQL Loading Jobs

**Chosen Approach: Hybrid (REST++ and GSQL)**

The ingestion method depends on data volume and latency requirements.

**Decision Rule:**
- Use REST++ APIs for micro-batches and low-latency updates.
- Use GSQL loading jobs for initial loads, backfills, and large-scale ingestion.

**Why this works:**
- REST++ APIs are simple and suitable for small to medium payloads.
- GSQL loading jobs are significantly faster (10x–100x) for bulk ingestion (> 10 Million).
- Enable large-scale migrations.

---

### 3. Push-Based Ingestion vs Pull-Based (Kafka)

**Chosen Approach: Push-Based Ingestion using Spark**

**Why push-based ingestion was selected:**
- Iceberg-to-Kafka CDC connectors are complex and not natively available.
- Kafka-based solutions require custom connector development.
- Spark integrates natively with Iceberg, S3, and supports parallel execution.
- TigerGraph performs best when data is pushed in bulk.

**Planned Enhancements:**
- Spark jobs are centrally orchestrated and monitored.
- Retry and failure handling are managed at the Spark job level.

---

### 4. Micro-Batching vs Full Load

**Chosen Approach: Both**

**Micro-Batching:**
- Enables near-real-time updates.
- Reduces memory pressure.
- Allows parallel processing of incremental changes.

**Full Loads:**
- Used for initial ingestion and backfills.
- Required when schemas change significantly.
- Guarantees a clean rebuild of the graph.
- Scales much better computationally for extremely large datasets (> 10 Million)

---

### 5. Fault Tolerance (Mainly Spark Layer)

**Design Considerations:**
- Spark retries failed tasks automatically.
- Iceberg snapshot isolation prevents partial reads.
- Job failures do not corrupt source data.
- Spark recreates the state upon job or node failures using lineage graphs.
- For complex task dependencies, disk persistency can be utilized.

**Planned Enhancements:**
- Persist last processed snapshot ID or timestamp through checkpoints (S3 uploads).
- Resume ingestion from the last successful checkpoint.

---

### 6. Idempotency Guarantees

**How idempotency is achieved:**
- Vertex IDs are derived from primary keys ensuring uniqueness.
- Edge uniqueness is enforced using (from_id, to_id).
- Reprocessing the same snapshot produces identical graph state.

**Result:**
- Safe retries without creating duplicate vertices or edges.
- May lead to stale tigergraph DB data in case of failures.

---

### 7. Scalability Strategy

**Scaling by Layer:**

- Spark:
  - Horizontal scaling using multiple executors.
  - Partitioning based on Iceberg metadata.
  - Parallel reads from Iceberg and writes to TigerGraph.
  - Strong native support for Apache Iceberg.

- Iceberg:
  - Snapshot-based incremental reads.
  - Efficient partitioned storage in S3.

- TigerGraph:
  - REST++ for bounded ingestion (parallelization possible).
  - GSQL loading jobs for large-scale ingestion.

---

### 8. Data Consistency and Validation

**Consistency Guarantees:**
- Snapshot-based reads ensure consistent views of data.

**Validation Steps:**
- Count validation between Iceberg and Tigergraph.
- Post-ingestion graph queries for verification.

---

### 9. Known Limitations

- No true real-time streaming support.
- REST++ APIs are not suitable for very large datasets.

---

## Development notes & next steps

- Migrate mapping definitions to YAML (e.g., `config/table_mappings.yaml`) so new tables can be added without code changes.
- Replace mocks with real clients:
  - Unity Catalog & Databricks Spark integration (use databricks-connect or a cluster job)
  - Replace TigerGraph mock with REST++ or gsql loading jobs (consider batching and bulk loaders)
- Check and implement idempotency and deduplication for edges.
- Add retries, dead-letter queues and exponential backoff when calling external services.
- Improve error handling and structured logging (replace prints with a logger).

---

## Additional Documentation

- **Design Document** - See `design_document.pdf` for detailed architecture analysis
- **TigerGraph Docs** - [https://docs.tigergraph.com](https://docs.tigergraph.com)
- **Iceberg Docs** - [https://iceberg.apache.org](https://iceberg.apache.org)
- **Unity Catalog** - [Databricks Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/)

---

docker load -i ./tigergraph-4.2.2-community-docker-image.tar.gz
docker run -d --name tigergraph -p 14240:14240 -p 9000:9000 -p 8123:8123 tigergraph/community:4.2.2
https://github.com/tigergraph/ecosys/blob/master/demos/guru_scripts/docker/README.md
docker cp /home/player1/data_pipeline/schemas/tigergraph_schema.gsql tigergraph:/tmp/schema.gsql
docker exec -it tigergraph /bin/bash
gsql /tmp/schema.gsql

