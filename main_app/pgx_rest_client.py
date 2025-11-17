"""
Oracle PGX Server REST API Client

This module provides a simple interface to interact with Oracle Graph Server (PGX)
via its REST API, enabling graph analytics and machine learning without pypgx.
"""

import requests
import json
import time


class PGXRestClient:
    """Client for Oracle PGX Server REST API"""

    def __init__(self, base_url, username=None, password=None):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.session = requests.Session()
        if username and password:
            self.session.auth = (username, password)

    def get_version(self):
        """Get PGX server version"""
        response = self.session.get(f"{self.base_url}/version")
        response.raise_for_status()
        return response.json()

    def load_graph_from_database(self, graph_name, jdbc_url, username, password):
        """
        Load a property graph from Oracle Database into PGX

        Args:
            graph_name: Name for the graph in PGX
            jdbc_url: JDBC connection string
            username: Database username
            password: Database password
        """
        config = {
            "name": graph_name,
            "format": "pgql",
            "jdbc_url": jdbc_url,
            "username": username,
            "password": password,
            "pgql_query": f"SELECT * FROM GRAPH_TABLE ({graph_name} MATCH (v) -[e]-> (w) COLUMNS (v, e, w))"
        }

        response = self.session.post(
            f"{self.base_url}/graphs",
            json=config,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        return response.json()

    def run_pagerank(self, graph_name, max_iterations=100, tolerance=0.001):
        """Run PageRank algorithm on a graph"""
        payload = {
            "algorithm": "pagerank",
            "graph": graph_name,
            "params": {
                "max_iterations": max_iterations,
                "tolerance": tolerance
            }
        }

        response = self.session.post(
            f"{self.base_url}/analytics/algorithms/run",
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        return response.json()

    def run_betweenness_centrality(self, graph_name):
        """Run Betweenness Centrality algorithm"""
        payload = {
            "algorithm": "betweenness_centrality",
            "graph": graph_name
        }

        response = self.session.post(
            f"{self.base_url}/analytics/algorithms/run",
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        return response.json()

    def run_community_detection(self, graph_name, algorithm="louvain"):
        """Run community detection (Louvain or Label Propagation)"""
        payload = {
            "algorithm": algorithm,
            "graph": graph_name
        }

        response = self.session.post(
            f"{self.base_url}/analytics/algorithms/run",
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        return response.json()

    def run_deepwalk(self, graph_name, dimensions=64, walk_length=10, walks_per_node=10):
        """Run DeepWalk algorithm for node embeddings"""
        payload = {
            "algorithm": "deepwalk",
            "graph": graph_name,
            "params": {
                "dimension": dimensions,
                "walk_length": walk_length,
                "walks_per_node": walks_per_node
            }
        }

        response = self.session.post(
            f"{self.base_url}/analytics/ml/deepwalk",
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        return response.json()

    def execute_pgql(self, graph_name, query):
        """Execute a PGQL query on a loaded graph"""
        payload = {
            "graph": graph_name,
            "query": query
        }

        response = self.session.post(
            f"{self.base_url}/pgql/query",
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        return response.json()

    def list_graphs(self):
        """List all loaded graphs"""
        response = self.session.get(f"{self.base_url}/graphs")
        response.raise_for_status()
        return response.json()

    def get_graph_info(self, graph_name):
        """Get information about a specific graph"""
        response = self.session.get(f"{self.base_url}/graphs/{graph_name}")
        response.raise_for_status()
        return response.json()

    def delete_graph(self, graph_name):
        """Delete a graph from PGX"""
        response = self.session.delete(f"{self.base_url}/graphs/{graph_name}")
        response.raise_for_status()
        return response.json()


# Example usage functions
def example_pgx_analysis(pgx_client, graph_name="pest_graph"):
    """Example of running PGX analytics via REST API"""

    print("=" * 50)
    print("PGX Graph Analytics Example")
    print("=" * 50)

    # 1. Check server version
    version = pgx_client.get_version()
    print(f"PGX Server Version: {version}")

    # 2. List loaded graphs
    graphs = pgx_client.list_graphs()
    print(f"Loaded graphs: {graphs}")

    # 3. Run PageRank
    print("\nRunning PageRank...")
    pagerank_result = pgx_client.run_pagerank(graph_name)
    print(f"PageRank result: {pagerank_result}")

    # 4. Run Community Detection
    print("\nRunning Community Detection...")
    community_result = pgx_client.run_community_detection(graph_name)
    print(f"Community Detection result: {community_result}")

    # 5. Execute PGQL query
    print("\nExecuting PGQL query...")
    pgql_result = pgx_client.execute_pgql(
        graph_name,
        "SELECT v.entity_id, v.entity_type FROM MATCH (v) LIMIT 10"
    )
    print(f"PGQL query result: {pgql_result}")

    print("=" * 50)
