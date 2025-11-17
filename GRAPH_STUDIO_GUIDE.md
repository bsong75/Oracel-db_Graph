# Graph Studio UI Guide

## Access
- URL: https://localhost:7007/ui/
- Username: `graphuser`
- Password: `Welcome1`

## Step 1: Query Your Data with PGQL

In Graph Studio, go to the **Query** or **Notebook** section and run PGQL queries:

### Check Available Tables
```sql
SELECT table_name
FROM user_tables
WHERE table_name LIKE '%EDGES' OR table_name LIKE 'ENTITIES%';
```

### Count Entities
```sql
SELECT COUNT(*) FROM entities;
```

### Count Edges
```sql
SELECT COUNT(*) FROM shipped_in_edges;
SELECT COUNT(*) FROM is_from_edges;
SELECT COUNT(*) FROM has_weather_edges;
SELECT COUNT(*) FROM has_inspection_result_edges;
```

## Step 2: Create Property Graph (UI Method)

If there's a **Models** or **Graphs** menu:

1. Look for "Create" or "New Graph" button
2. Select **From Existing Tables**
3. Choose your vertex tables:
   - ENTITIES (key: ENTITY_ID)
   - COUNTRIES (key: COUNTRY_ID)
   - MONTHS (key: MONTH_ID)
4. Choose your edge tables:
   - SHIPPED_IN_EDGES (source: SOURCE_ID, destination: TARGET_ID)
   - IS_FROM_EDGES (source: SOURCE_ID, destination: TARGET_ID)
   - HAS_WEATHER_EDGES (source: SOURCE_ID, destination: TARGET_ID)
   - HAS_INSPECTION_RESULT_EDGES (source: SOURCE_ID, destination: TARGET_ID)

## Step 3: Create Property Graph (SQL Method)

If UI doesn't work, run this SQL in the query window:

```sql
-- Create property graph
CREATE PROPERTY GRAPH pest_graph
  VERTEX TABLES (
    entities KEY (entity_id)
      PROPERTIES (entity_id, name),
    countries KEY (country_id)
      PROPERTIES (country_id, name),
    months KEY (month_id)
      PROPERTIES (month_id, name, year)
  )
  EDGE TABLES (
    shipped_in_edges
      KEY (edge_id)
      SOURCE KEY (source_id) REFERENCES entities(entity_id)
      DESTINATION KEY (target_id) REFERENCES months(month_id)
      PROPERTIES (edge_id),
    is_from_edges
      KEY (edge_id)
      SOURCE KEY (source_id) REFERENCES entities(entity_id)
      DESTINATION KEY (target_id) REFERENCES countries(country_id)
      PROPERTIES (edge_id)
  );
```

## Step 4: Run PGQL Queries

Once the graph is created, run PGQL queries:

### Find all entities from a specific country
```pgql
SELECT e.name, c.name AS country
FROM MATCH (e:entities)-[:is_from]->(c:countries)
WHERE c.name = 'MEXICO'
LIMIT 10
```

### Find shipping patterns
```pgql
SELECT e.name AS entity, m.name AS month, m.year
FROM MATCH (e:entities)-[:shipped_in]->(m:months)
ORDER BY m.year DESC, m.month_id DESC
LIMIT 20
```

### Count connections per entity
```pgql
SELECT e.entity_id, e.name, COUNT(*) AS connection_count
FROM MATCH (e:entities)-[r]->()
GROUP BY e.entity_id, e.name
ORDER BY connection_count DESC
LIMIT 10
```

## Step 5: Run Graph Algorithms (if available)

Look for an **Algorithms** or **Analytics** section in the UI menu. Common algorithms:

- **PageRank**: Find most important entities
- **Community Detection**: Find clusters of related entities
- **Shortest Path**: Find connections between entities
- **Centrality**: Identify key entities in the network

## Alternative: Use Notebooks

Some versions of Graph Studio have a **Notebooks** feature where you can:
1. Write PGQL queries
2. Visualize results as graphs
3. Export results as CSV or JSON
