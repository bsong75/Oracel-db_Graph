# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This repository contains the **Oracle Graph PGX Pest Analysis Application** - a Docker-based system for analyzing pest inspection data using Oracle Database with Oracle Graph PGX.

## Architecture

### Two-Container System
1. **oracle-db**: Oracle Database Free with Graph capabilities
2. **python-app**: Python application using Oracle Graph PGX (pypgx) for graph analytics

## Running the Application

### Start Services
```bash
docker-compose up -d
```

### View Logs
```bash
docker-compose logs -f python-app
```

### Stop Services
```bash
docker-compose down
```

## Code Structure

### Main Application (`main_app/pest_analysis_classes.py`)
The `PestDataAnalyzer` class is the core orchestrator:
- **Database Connection**: Uses `oracledb` for Oracle connectivity and `pypgx` for PGX server
- **Graph Building**: Inserts vertices and edges into Oracle property graph tables
- **PGX Integration**: Creates PGX sessions for running graph algorithms

Key methods:
- `connect()`: Establishes Oracle DB and PGX server connections
- `create_nodes()`: Inserts vertices into entity, country, month, target_proxy, country_month tables
- `create_inspections_and_relationships()`: Creates inspection records and edge tables
- `create_projection()`: Loads graph into PGX memory for analysis
- `cleanup()` / `close()`: Proper resource cleanup

### Graph Schema

**Vertex Tables** (in Oracle DB):
- `entities`: Primary entities (entity_id, entity_type)
- `countries`: Country nodes (country_code, country_name)
- `months`: Month nodes (month_name, month_number)
- `target_proxies`: Binary targets (target_value, target_label)
- `country_months`: Composite nodes (cm_id, country_code, month_name)
- `inspections`: Inspection records with metrics

**Edge Tables**:
- `shipped_in_edges`: Entity → Month
- `is_from_edges`: Entity → Country
- `has_weather_edges`: Entity → CountryMonth
- `has_inspection_result_edges`: Entity → TargetProxy

### Algorithm Modules

#### `algo1_structural.py`
Extracts graph topology features using PGX Analyst API:
- Centrality: PageRank, Betweenness, Closeness, Eigenvector, Degree
- Triangle count and local clustering coefficient
- Community detection via conductance minimization
- K-Core decomposition

**Usage**: These are class methods meant to be added to `PestDataAnalyzer`

#### `algo2_embeddings_node2vec.py`
Generates learned node representations:
- `extract_node_embeddings()`: Uses DeepWalk (PGX's equivalent to Node2Vec)
- `extract_node_embeddings_alternative()`: Structural feature-based embeddings
- Configurable embedding dimensions (default 64)

**Note**: Oracle PGX doesn't have Node2Vec; DeepWalk is the closest equivalent (Node2Vec with p=1, q=1)

#### `algo3_graphsage.py`
Supervised graph learning for pest prediction:
- `run_graphsage_pgx()`: Uses DeepWalk + labels for feature extraction
- `run_graphsage_alternative()`: Structural features + labels without deep learning
- Outputs CSV files with entity embeddings and pest labels

**Note**: Oracle PGX doesn't have GraphSAGE directly; uses GraphWise model or DeepWalk as alternatives

## Oracle PGX vs Neo4j GDS Mapping

| Neo4j GDS | Oracle PGX |
|-----------|------------|
| `gds.run_cypher()` | `cursor.execute()` (SQL) or `graph.query_pgql()` (PGQL) |
| `gds.graph.project()` | `pgx_session.read_graph_by_name()` |
| `gds.pageRank.stream()` | `analyst.pagerank()` |
| `gds.node2vec.stream()` | `DeepWalkModel().fit_model()` |
| `gds.beta.graphSage.train()` | `SupervisedGraphWiseModel()` or DeepWalk |
| Cypher queries | PGQL queries or SQL |
| Bolt protocol | Oracle TNS (oracledb) |

## Database Initialization

The [oracle_db/init/01_setup_graph.sql](oracle_db/init/01_setup_graph.sql) script:
- Creates `graphuser` with necessary privileges
- Creates all vertex and edge tables
- Sets up foreign keys and indexes
- Grants GRAPH_DEVELOPER and PGX_SESSION_CREATE privileges

This runs automatically when the Oracle container first starts.

## Environment Configuration

Set via `docker-compose.yml`:
- `ORACLE_HOST`: Database hostname (oracle-db in Docker network)
- `ORACLE_PORT`: 1521
- `ORACLE_SERVICE`: GRAPHDB
- `ORACLE_USER/PASSWORD`: graphuser / GraphPassword123
- `PGX_BASE_URL`: http://oracle-db:7007
- `CSV_FILE_PATH`: /app/data/pest_data.csv

## Development Notes

### Adding New Algorithms
1. Create method in `PestDataAnalyzer` or separate module
2. Use `self.graph` (PGX graph object) and `self.pgx_session`
3. Import from `pypgx.api.analyst` or `pypgx.api.mllib`
4. Query graph using PGQL: `self.graph.query_pgql("SELECT ... FROM MATCH ...")`

### Querying the Graph
```python
# PGQL query
result = self.graph.query_pgql("""
    SELECT v.entity_id, COUNT(e) as edge_count
    FROM MATCH (v:entities)-[e]->(t)
    GROUP BY v.entity_id
""").to_pandas()

# Get vertex properties
vertex = self.graph.get_vertex(entity_id)
degree = vertex.degree()
```

### Running Algorithms
```python
from pypgx.api.analyst import Analyst
analyst = Analyst(self.pgx_session)

# PageRank
pagerank = analyst.pagerank(self.graph, tol=0.001, max_iter=100)
score = pagerank.get(vertex)

# DeepWalk embeddings
from pypgx.api.mllib import DeepWalkModel
model = DeepWalkModel()
model.set_embedding_dimension(64)
embeddings = model.fit_model(self.graph, self.pgx_session)
```

## Key Differences from Neo4j Version

1. **Connection**: `oracledb` + `pypgx` instead of `graphdatascience`
2. **Data Model**: Relational tables with property graph mapping vs native graph
3. **Query Language**: PGQL (similar to Cypher) + SQL
4. **Graph Loading**: Explicit table → PGX projection vs native storage
5. **Algorithms**: PGX Analyst API vs Neo4j GDS procedures
6. **Embeddings**: DeepWalk instead of Node2Vec
7. **GNN**: GraphWise/DeepWalk instead of GraphSAGE

## Backup Files

Neo4j versions preserved as:
- `pest_analysis_classes_neo4j.py`
- `algo3_graphsage_neo4j.py`

## Important Implementation Notes

- PGX graph projections are in-memory; rebuild after Oracle restarts
- Use `cleanup()` to destroy graphs and free memory
- Algorithm methods expect `self.graph` to be loaded
- PGQL queries return PgxFrame objects (convert with `.to_pandas()`)
- DeepWalk training can be slow on large graphs; adjust epochs/walks accordingly
