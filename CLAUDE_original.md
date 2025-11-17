# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Graph database analysis system for pest inspection prediction using Neo4j and Graph Data Science (GDS) library. The system loads pest inspection data, builds a knowledge graph, and applies various graph algorithms and machine learning techniques to predict pest occurrences.

## Core Architecture

### Main Analysis Class: PestDataAnalyzer

The `PestDataAnalyzer` class ([pest_analysis_classes.py](pest_analysis_classes.py)) is the central orchestrator that:
- Connects to Neo4j database using GraphDataScience client
- Loads CSV data and builds a multi-type knowledge graph
- Creates graph projections for analysis
- Runs feature extraction and ML algorithms
- Manages resource cleanup (models, graphs, connections)

### Graph Schema

**Node Types:**
- `Entity`: Primary entities being inspected (using ENTY_ID)
- `Country`: Country codes (CTRY_CODE)
- `Month`: Month names
- `TargetProxy`: Binary target values (0=no_pest, 1=pest)
- `CountryMonth`: Composite nodes combining country and month
- `Inspection`: Individual inspection records with historical metrics

**Key Relationships:**
- `Entity-[:SHIPPED_IN]->Month`
- `Entity-[:IS_FROM]->Country`
- `Entity-[:HAS_WEATHER]->CountryMonth`
- `Entity-[:HAS_INSPECTION_RESULT]->TargetProxy`
- `Inspection-[:IN_COUNTRY]->Country`
- `Inspection-[:IN_MONTH]->Month`
- `Inspection-[:FOR_ENTITY]->Entity`
- `Inspection-[:HAS_TARGET]->TargetProxy`

## Algorithm Pipeline

The system implements three complementary feature extraction approaches:

### 1. Structural Features ([algo1_structural.py](algo1_structural.py))
Extract graph topology features using centrality and community detection:
- Centrality measures: PageRank, Betweenness, Closeness, Eigenvector, Degree
- Triangle count and local clustering coefficient
- Community detection (Louvain algorithm)
- Article Rank and K-Core decomposition

### 2. Node Embeddings ([algo2_embeddings_node2vec.py](algo2_embeddings_node2vec.py))
Generate learned node representations:
- Node2Vec (64 dimensions, walk length 10, 10 walks per node)
- FastRP alternative (faster but less sophisticated)
- Returns embeddings merged with entity data

### 3. GraphSAGE Prediction ([algo3_graphsage.py](algo3_graphsage.py))
Supervised learning for pest prediction:
- Adds node properties (degree, pest_value, entity_degree)
- Creates separate "pest_prediction" graph projection
- Trains GraphSAGE model with projected features
- Generates 64-dimensional embeddings
- Outputs predictions with pest labels (has_pest_ever, pest_rate)
- Saves results to `graphsage_entity_features.csv`

## Key Design Patterns

### Modular Algorithm Functions
The three algorithm files ([algo1_structural.py](algo1_structural.py), [algo2_embeddings_node2vec.py](algo2_embeddings_node2vec.py), [algo3_graphsage.py](algo3_graphsage.py)) are designed as methods to be added to the `PestDataAnalyzer` class, not standalone modules. They expect `self.gds`, `self.graph`, and `self.logger` to be available.

### Resource Management
The analyzer includes cleanup methods to prevent memory leaks:
- `cleanup()`: Drops models and intermediate graphs
- `close()`: Comprehensive cleanup including connection closure
- Graph projections are dropped before recreation to avoid conflicts

### Logging
Structured logging with levels (INFO, DEBUG, WARNING) throughout the pipeline. Logs include:
- Connection events
- Data loading statistics
- Algorithm progress
- Resource cleanup status

## Running the Analysis

**Prerequisites:**
- Neo4j instance running (default: `bolt://neo4j:7687`)
- Input CSV file: `pest_data.csv` with columns:
  - ENTY_ID, CTRY_CODE, MONTH, TARGET_PROXY
  - ENTY_EXAMS_30D, ENTY_PESTS_30D
  - ENTY_EXAMS_90D, ENTY_PESTS_90D
  - ENTY_EXAMS_1YR, ENTY_PESTS_1YR

**Configuration:**
Edit [pest_analysis_classes.py](pest_analysis_classes.py) `main()` function:
```python
CSV_FILE_PATH = "pest_data.csv"
NEO4J_URI = "bolt://neo4j:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"
```

**Execute:**
```bash
python pest_analysis_classes.py
```

This runs the full pipeline:
1. Connects to Neo4j
2. Loads CSV data
3. Clears existing database (WARNING: destructive)
4. Creates nodes and relationships
5. Builds graph projection
6. Extracts features (FastRP, centrality)
7. Trains GraphSAGE model
8. Outputs feature files

## Important Implementation Notes

- The `run_full_analysis()` method references `create_inspections()`, `run_fastrp()`, and `run_centrality_algorithms()` which are not present in the provided code - these methods need to be implemented or the existing algorithm functions need to be integrated
- [algo3_graphsage.py](algo3_graphsage.py) contains duplicate code (lines 93-148) that should be removed
- Graph projections must be explicitly dropped before recreation to avoid "already exists" errors
- The GraphSAGE algorithm uses a separate projection ("pest_prediction") from the main analysis ("pest_graph")
