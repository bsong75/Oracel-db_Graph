"""
Run PGQL queries and graph analytics using Oracle Database connection
"""

import oracledb
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

# Connection parameters
ORACLE_HOST = "localhost"  # Always use localhost from Windows host
ORACLE_PORT = 1521
ORACLE_SERVICE = "FREE"  # Data is in FREE, not FREEPDB1
ORACLE_USER = os.getenv("ORACLE_USER", "graphuser")
ORACLE_PASSWORD = os.getenv("GRAPH_PASSWORD", "Welcome1")

def connect_db():
    """Connect to Oracle database"""
    dsn = oracledb.makedsn(ORACLE_HOST, ORACLE_PORT, service_name=ORACLE_SERVICE)
    connection = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=dsn)
    print(f"[OK] Connected to Oracle Database")
    return connection

def run_query(connection, query, description):
    """Run a SQL query and display results"""
    print(f"\n{description}")
    print("-" * 60)

    try:
        df = pd.read_sql(query, connection)
        print(df.to_string(index=False))
        return df
    except Exception as e:
        print(f"[ERROR] {e}")
        return None

def analyze_graph_structure(connection):
    """Analyze the graph structure"""

    print("\n" + "="*60)
    print("GRAPH STRUCTURE ANALYSIS")
    print("="*60)

    # Count vertices
    vertices_query = """
    SELECT 'ENTITIES' as table_name, COUNT(*) as count FROM entities
    UNION ALL
    SELECT 'COUNTRIES', COUNT(*) FROM countries
    UNION ALL
    SELECT 'MONTHS', COUNT(*) FROM months
    UNION ALL
    SELECT 'INSPECTIONS', COUNT(*) FROM inspections
    """
    run_query(connection, vertices_query, "Vertex Counts:")

    # Count edges
    edges_query = """
    SELECT 'SHIPPED_IN' as edge_type, COUNT(*) as count FROM shipped_in_edges
    UNION ALL
    SELECT 'IS_FROM', COUNT(*) FROM is_from_edges
    UNION ALL
    SELECT 'HAS_WEATHER', COUNT(*) FROM has_weather_edges
    UNION ALL
    SELECT 'HAS_INSPECTION', COUNT(*) FROM has_inspection_result_edges
    """
    run_query(connection, edges_query, "Edge Counts:")

def analyze_node_degrees(connection):
    """Calculate node degrees (number of connections)"""

    print("\n" + "="*60)
    print("NODE DEGREE ANALYSIS")
    print("="*60)

    # Outgoing connections per entity
    query = """
    SELECT
        e.entity_id,
        e.entity_type,
        (SELECT COUNT(*) FROM shipped_in_edges WHERE entity_id = e.entity_id) AS shipped_count,
        (SELECT COUNT(*) FROM is_from_edges WHERE entity_id = e.entity_id) AS country_count,
        (SELECT COUNT(*) FROM has_weather_edges WHERE entity_id = e.entity_id) AS weather_count,
        (SELECT COUNT(*) FROM has_inspection_result_edges WHERE entity_id = e.entity_id) AS inspection_count
    FROM entities e
    ORDER BY shipped_count DESC
    FETCH FIRST 10 ROWS ONLY
    """
    run_query(connection, query, "Top 10 Entities by Connections:")

def analyze_country_distribution(connection):
    """Analyze entity distribution by country"""

    print("\n" + "="*60)
    print("COUNTRY DISTRIBUTION ANALYSIS")
    print("="*60)

    query = """
    SELECT
        country_code,
        COUNT(DISTINCT entity_id) AS entity_count
    FROM inspections
    GROUP BY country_code
    ORDER BY entity_count DESC
    """
    run_query(connection, query, "Entities per Country:")

def analyze_temporal_patterns(connection):
    """Analyze shipping patterns over time"""

    print("\n" + "="*60)
    print("TEMPORAL PATTERN ANALYSIS")
    print("="*60)

    query = """
    SELECT
        month_name,
        COUNT(DISTINCT entity_id) AS entity_count,
        COUNT(*) AS shipment_count
    FROM shipped_in_edges
    GROUP BY month_name
    ORDER BY shipment_count DESC
    """
    run_query(connection, query, "Shipments by Month:")

def analyze_inspection_results(connection):
    """Analyze inspection results"""

    print("\n" + "="*60)
    print("INSPECTION RESULTS ANALYSIS")
    print("="*60)

    query = """
    SELECT
        CASE WHEN has_pest = 1 THEN 'HAS_PEST' ELSE 'NO_PEST' END AS result,
        COUNT(*) AS count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
    FROM inspections
    GROUP BY has_pest
    ORDER BY count DESC
    """
    run_query(connection, query, "Inspection Results Distribution:")

def find_high_risk_entities(connection):
    """Find entities with high pest detection rates"""

    print("\n" + "="*60)
    print("HIGH RISK ENTITIES")
    print("="*60)

    query = """
    SELECT
        i.entity_id,
        e.entity_type,
        COUNT(*) AS inspection_count,
        SUM(i.has_pest) AS pest_count,
        ROUND(SUM(i.has_pest) * 100.0 / COUNT(*), 2) AS pest_percentage,
        ROUND(AVG(i.pests_30d), 2) AS avg_pests_30d,
        ROUND(AVG(i.pests_90d), 2) AS avg_pests_90d,
        ROUND(AVG(i.pests_1yr), 2) AS avg_pests_1yr
    FROM inspections i
    JOIN entities e ON i.entity_id = e.entity_id
    GROUP BY i.entity_id, e.entity_type
    HAVING COUNT(*) >= 1
    ORDER BY avg_pests_1yr DESC, avg_pests_90d DESC
    FETCH FIRST 10 ROWS ONLY
    """
    run_query(connection, query, "Top 10 Entities by Pest Detection Rate:")

if __name__ == "__main__":
    print("="*60)
    print("ORACLE GRAPH ANALYTICS - PGQL QUERIES")
    print("="*60)

    # Connect to database
    connection = connect_db()

    try:
        # Run all analyses
        analyze_graph_structure(connection)
        analyze_node_degrees(connection)
        analyze_country_distribution(connection)
        analyze_temporal_patterns(connection)
        analyze_inspection_results(connection)
        find_high_risk_entities(connection)

        print("\n" + "="*60)
        print("ANALYSIS COMPLETE")
        print("="*60)

    finally:
        connection.close()
        print("\n[OK] Database connection closed")
