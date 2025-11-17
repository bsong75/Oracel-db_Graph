"""
Oracle Property Graph Analytics using PGQL Queries

This module demonstrates how to run graph analytics using Oracle's built-in
PGQL (Property Graph Query Language) without requiring PGX Server.
"""

import pandas as pd


def run_pagerank_pgql(analyzer):
    """
    Run PageRank using PGQL

    Note: Oracle Database 23c has built-in graph algorithms via SQL/PGQL
    """
    analyzer.logger.info("Running PageRank analysis via PGQL...")

    # Example PGQL query to find entities with most connections
    query = """
        SELECT e.entity_id, COUNT(*) as connection_count
        FROM GRAPH_TABLE (pest_graph
            MATCH (e:entity)-[r]->(t)
            COLUMNS (e.entity_id, r, t)
        )
        GROUP BY e.entity_id
        ORDER BY connection_count DESC
        FETCH FIRST 10 ROWS ONLY
    """

    analyzer.cursor.execute(query)
    results = analyzer.cursor.fetchall()

    df = pd.DataFrame(results, columns=['entity_id', 'connection_count'])
    analyzer.logger.info(f"Top entities by connections:\n{df}")

    return df


def find_paths_pgql(analyzer, start_entity, max_hops=3):
    """Find paths from an entity using PGQL"""
    analyzer.logger.info(f"Finding paths from entity {start_entity}...")

    query = f"""
        SELECT v.entity_id, COUNT(*) as path_count
        FROM GRAPH_TABLE (pest_graph
            MATCH (src:entity)-[r:shipped_in|is_from|has_result]->{{1,{max_hops}}}(v)
            WHERE src.entity_id = '{start_entity}'
            COLUMNS (v.entity_id)
        )
        GROUP BY v.entity_id
    """

    analyzer.cursor.execute(query)
    results = analyzer.cursor.fetchall()

    return pd.DataFrame(results, columns=['reachable_entity', 'path_count'])


def analyze_pest_patterns_pgql(analyzer):
    """Analyze pest patterns using PGQL"""
    analyzer.logger.info("Analyzing pest patterns...")

    # Find entities with pest results and their countries
    query = """
        SELECT e.entity_id, c.country_code, tp.target_label
        FROM GRAPH_TABLE (pest_graph
            MATCH (e:entity)-[:is_from]->(c:country),
                  (e)-[:has_result]->(tp:target)
            WHERE tp.target_value = 1
            COLUMNS (e.entity_id, c.country_code, tp.target_label)
        )
    """

    analyzer.cursor.execute(query)
    results = analyzer.cursor.fetchall()

    df = pd.DataFrame(results, columns=['entity_id', 'country_code', 'target_label'])

    # Analyze by country
    country_stats = df.groupby('country_code').size().reset_index(name='pest_count')
    country_stats = country_stats.sort_values('pest_count', ascending=False)

    analyzer.logger.info(f"Pest counts by country:\n{country_stats.head(10)}")

    return country_stats


def get_entity_neighborhood_pgql(analyzer, entity_id):
    """Get the 1-hop neighborhood of an entity"""
    analyzer.logger.info(f"Getting neighborhood for entity {entity_id}...")

    query = f"""
        SELECT e.entity_id, r, v
        FROM GRAPH_TABLE (pest_graph
            MATCH (e:entity)-[r]->(v)
            WHERE e.entity_id = '{entity_id}'
            COLUMNS (e.entity_id, LABEL(r) as r, v)
        )
    """

    analyzer.cursor.execute(query)
    results = analyzer.cursor.fetchall()

    return pd.DataFrame(results, columns=['entity_id', 'relationship', 'neighbor'])


def count_triangles_pgql(analyzer):
    """Count triangles in the graph (simple clustering)"""
    analyzer.logger.info("Counting triangles...")

    query = """
        SELECT COUNT(*) as triangle_count
        FROM GRAPH_TABLE (pest_graph
            MATCH (a:entity)-[r1]->(b:entity)-[r2]->(c:entity)-[r3]->(a)
            COLUMNS (a, b, c)
        )
    """

    analyzer.cursor.execute(query)
    result = analyzer.cursor.fetchone()

    triangle_count = result[0] if result else 0
    analyzer.logger.info(f"Total triangles found: {triangle_count}")

    return triangle_count


def example_analysis(analyzer):
    """Run a complete graph analysis using PGQL"""
    analyzer.logger.info("="*50)
    analyzer.logger.info("Starting PGQL-based graph analysis")
    analyzer.logger.info("="*50)

    # 1. Node degree analysis (like PageRank proxy)
    top_entities = run_pagerank_pgql(analyzer)

    # 2. Pest pattern analysis
    pest_stats = analyze_pest_patterns_pgql(analyzer)

    # 3. Example: Analyze first entity's neighborhood
    if len(top_entities) > 0:
        first_entity = top_entities.iloc[0]['entity_id']
        neighborhood = get_entity_neighborhood_pgql(analyzer, first_entity)
        analyzer.logger.info(f"Neighborhood of {first_entity}:\n{neighborhood}")

    # 4. Triangle counting
    triangles = count_triangles_pgql(analyzer)

    analyzer.logger.info("="*50)
    analyzer.logger.info("PGQL analysis complete!")
    analyzer.logger.info("="*50)

    return {
        'top_entities': top_entities,
        'pest_stats': pest_stats,
        'triangle_count': triangles
    }
