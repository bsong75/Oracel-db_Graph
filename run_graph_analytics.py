"""
Run Graph Analytics using Oracle Database directly (without PGX REST API)

This script performs graph analytics using SQL queries on the loaded graph data.
"""

import oracledb
import pandas as pd
import os

# Database connection info
ORACLE_HOST = os.getenv("ORACLE_HOST", "localhost")
ORACLE_PORT = int(os.getenv("ORACLE_PORT", "1521"))
ORACLE_SERVICE = os.getenv("ORACLE_SERVICE", "FREE")
ORACLE_USER = os.getenv("ORACLE_USER", "graphuser")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "GraphPassword123")

def connect_to_db():
    """Connect to Oracle database"""
    dsn = oracledb.makedsn(ORACLE_HOST, ORACLE_PORT, service_name=ORACLE_SERVICE)
    connection = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=dsn)
    return connection

def analyze_node_degrees(connection):
    """Analyze node degrees (entity connectivity)"""
    print("\n" + "="*60)
    print("Node Degree Analysis (Entity Connectivity)")
    print("="*60)

    query = """
        SELECT
            e.entity_id,
            (SELECT COUNT(*) FROM shipped_in_edges WHERE entity_id = e.entity_id) as shipped_in_count,
            (SELECT COUNT(*) FROM is_from_edges WHERE entity_id = e.entity_id) as is_from_count,
            (SELECT COUNT(*) FROM has_weather_edges WHERE entity_id = e.entity_id) as has_weather_count,
            (SELECT COUNT(*) FROM has_inspection_result_edges WHERE entity_id = e.entity_id) as has_result_count,
            (SELECT COUNT(*) FROM shipped_in_edges WHERE entity_id = e.entity_id) +
            (SELECT COUNT(*) FROM is_from_edges WHERE entity_id = e.entity_id) +
            (SELECT COUNT(*) FROM has_weather_edges WHERE entity_id = e.entity_id) +
            (SELECT COUNT(*) FROM has_inspection_result_edges WHERE entity_id = e.entity_id) as total_degree
        FROM entities e
        ORDER BY total_degree DESC
        FETCH FIRST 20 ROWS ONLY
    """

    df = pd.read_sql(query, connection)
    print(f"\nTop 20 Entities by Degree (Connectivity):\n")
    print(df.to_string(index=False))

    return df

def analyze_pest_by_country(connection):
    """Analyze pest distribution by country"""
    print("\n" + "="*60)
    print("Pest Distribution by Country")
    print("="*60)

    query = """
        SELECT
            c.country_code,
            COUNT(DISTINCT e.entity_id) as total_entities,
            SUM(CASE WHEN tp.target_value = 1 THEN 1 ELSE 0 END) as pest_entities,
            ROUND(SUM(CASE WHEN tp.target_value = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pest_percentage
        FROM entities e
        JOIN is_from_edges ife ON e.entity_id = ife.entity_id
        JOIN countries c ON ife.country_code = c.country_code
        LEFT JOIN has_inspection_result_edges hire ON e.entity_id = hire.entity_id
        LEFT JOIN target_proxies tp ON hire.target_value = tp.target_value
        GROUP BY c.country_code
        HAVING COUNT(DISTINCT e.entity_id) > 5
        ORDER BY pest_percentage DESC
    """

    df = pd.read_sql(query, connection)
    print(f"\nPest Distribution by Country:\n")
    print(df.to_string(index=False))

    return df

def analyze_temporal_patterns(connection):
    """Analyze pest patterns by month"""
    print("\n" + "="*60)
    print("Temporal Patterns (Pest by Month)")
    print("="*60)

    query = """
        SELECT
            m.month_name,
            COUNT(DISTINCT e.entity_id) as total_entities,
            SUM(CASE WHEN tp.target_value = 1 THEN 1 ELSE 0 END) as pest_count,
            ROUND(SUM(CASE WHEN tp.target_value = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pest_percentage
        FROM entities e
        JOIN shipped_in_edges sie ON e.entity_id = sie.entity_id
        JOIN months m ON sie.month_name = m.month_name
        LEFT JOIN has_inspection_result_edges hire ON e.entity_id = hire.entity_id
        LEFT JOIN target_proxies tp ON hire.target_value = tp.target_value
        GROUP BY m.month_name, m.month_number
        ORDER BY m.month_number
    """

    df = pd.read_sql(query, connection)
    print(f"\nPest Patterns by Month:\n")
    print(df.to_string(index=False))

    return df

def find_entity_paths(connection, entity_id, max_depth=2):
    """Find paths from a specific entity"""
    print("\n" + "="*60)
    print(f"Paths from Entity: {entity_id}")
    print("="*60)

    # Find connected entities
    query = f"""
        SELECT DISTINCT
            e.entity_id as source_entity,
            c.country_code,
            m.month_name,
            tp.target_label
        FROM entities e
        LEFT JOIN is_from_edges ife ON e.entity_id = ife.entity_id
        LEFT JOIN countries c ON ife.country_code = c.country_code
        LEFT JOIN shipped_in_edges sie ON e.entity_id = sie.entity_id
        LEFT JOIN months m ON sie.month_name = m.month_name
        LEFT JOIN has_inspection_result_edges hire ON e.entity_id = hire.entity_id
        LEFT JOIN target_proxies tp ON hire.target_value = tp.target_value
        WHERE e.entity_id = '{entity_id}'
    """

    df = pd.read_sql(query, connection)
    print(f"\nConnections for Entity {entity_id}:\n")
    print(df.to_string(index=False))

    return df

def export_graph_to_csv(connection):
    """Export graph data for external analysis"""
    print("\n" + "="*60)
    print("Exporting Graph Data to CSV")
    print("="*60)

    # Export nodes
    entities_df = pd.read_sql("SELECT * FROM entities", connection)
    entities_df.to_csv('data/entities_export.csv', index=False)
    print(f"✓ Exported {len(entities_df)} entities to data/entities_export.csv")

    # Export edges
    edges_query = """
        SELECT
            'shipped_in' as edge_type,
            entity_id as source,
            month_name as target
        FROM shipped_in_edges
        UNION ALL
        SELECT
            'is_from' as edge_type,
            entity_id as source,
            country_code as target
        FROM is_from_edges
        UNION ALL
        SELECT
            'has_weather' as edge_type,
            entity_id as source,
            cm_id as target
        FROM has_weather_edges
        UNION ALL
        SELECT
            'has_result' as edge_type,
            entity_id as source,
            CAST(target_value AS VARCHAR2(10)) as target
        FROM has_inspection_result_edges
    """

    edges_df = pd.read_sql(edges_query, connection)
    edges_df.to_csv('data/edges_export.csv', index=False)
    print(f"✓ Exported {len(edges_df)} edges to data/edges_export.csv")

    print("\nGraph exported! You can now:")
    print("  - Import into NetworkX for advanced analytics")
    print("  - Import into Gephi for visualization")
    print("  - Import into PyTorch Geometric for GNN training")

    return entities_df, edges_df

def main():
    print("="*60)
    print("Oracle Graph Analytics - SQL-Based Analysis")
    print("="*60)

    # Connect to database
    print("\nConnecting to Oracle Database...")
    connection = connect_to_db()
    print("✓ Connected successfully")

    try:
        # Run analytics
        degree_df = analyze_node_degrees(connection)
        country_df = analyze_pest_by_country(connection)
        temporal_df = analyze_temporal_patterns(connection)

        # Get sample entity for path analysis
        if len(degree_df) > 0:
            sample_entity = degree_df.iloc[0]['entity_id']
            path_df = find_entity_paths(connection, sample_entity)

        # Export for external tools
        entities_df, edges_df = export_graph_to_csv(connection)

        print("\n" + "="*60)
        print("Analysis Complete!")
        print("="*60)
        print("\nNext Steps:")
        print("  1. View Graph Studio UI: http://localhost:7007/ui/")
        print("     (Login with graphuser / GraphPassword123)")
        print("  2. Import exported CSVs into NetworkX/Gephi")
        print("  3. Use PGX algorithms via Graph Studio UI")

    finally:
        connection.close()
        print("\n✓ Database connection closed")

if __name__ == "__main__":
    main()
