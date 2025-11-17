import pandas as pd
from pypgx.api.analyst import Analyst


def extract_structural_features(self):
    """Extract comprehensive structural features from the graph using Oracle PGX"""
    self.logger.info("Extracting structural features from graph...")

    if not self.graph:
        raise ValueError("Graph must be loaded before extracting features")

    analyst = Analyst(self.pgx_session)

    # Start with entity base - get all entity vertices
    vertices = self.graph.query_pgql("""
        SELECT v.entity_id
        FROM MATCH (v:entities)
    """).to_pandas()

    entity_df = vertices.rename(columns={'entity_id': 'entity_id'})
    entity_df['nodeId'] = range(len(entity_df))

    # Centrality measures
    self.logger.info("Computing centrality measures...")

    # PageRank
    self.logger.debug("Computing PageRank centrality...")
    pagerank = analyst.pagerank(self.graph, tol=0.001, max_iter=100)
    pr_values = []
    for idx, entity_id in enumerate(entity_df['entity_id']):
        vertex = self.graph.get_vertex(entity_id)
        pr_values.append(pagerank.get(vertex) if vertex else 0.0)
    entity_df['pageRank'] = pr_values

    # Degree centrality
    self.logger.debug("Computing degree centrality...")
    degree_values = []
    for entity_id in entity_df['entity_id']:
        vertex = self.graph.get_vertex(entity_id)
        degree_values.append(vertex.degree() if vertex else 0)
    entity_df['degree'] = degree_values

    # Betweenness centrality
    self.logger.debug("Computing betweenness centrality...")
    betweenness = analyst.betweenness_centrality(self.graph)
    bc_values = []
    for entity_id in entity_df['entity_id']:
        vertex = self.graph.get_vertex(entity_id)
        bc_values.append(betweenness.get(vertex) if vertex else 0.0)
    entity_df['betweenness'] = bc_values

    # Closeness centrality
    self.logger.debug("Computing closeness centrality...")
    closeness = analyst.closeness_centrality_unit_length(self.graph)
    cc_values = []
    for entity_id in entity_df['entity_id']:
        vertex = self.graph.get_vertex(entity_id)
        cc_values.append(closeness.get(vertex) if vertex else 0.0)
    entity_df['closeness'] = cc_values

    # Eigenvector centrality (similar to PageRank but different algorithm)
    self.logger.debug("Computing eigenvector centrality...")
    eigenvector = analyst.eigenvector_centrality(self.graph, max_iter=100, tol=0.001)
    ec_values = []
    for entity_id in entity_df['entity_id']:
        vertex = self.graph.get_vertex(entity_id)
        ec_values.append(eigenvector.get(vertex) if vertex else 0.0)
    entity_df['eigenvector'] = ec_values

    # Additional structural features
    self.logger.info("Computing additional structural features...")

    # Triangle count and clustering coefficient
    self.logger.debug("Computing triangle count...")
    triangles = analyst.count_triangles(self.graph)
    triangle_values = []
    for entity_id in entity_df['entity_id']:
        vertex = self.graph.get_vertex(entity_id)
        triangle_values.append(triangles.get(vertex) if vertex else 0)
    entity_df['triangleCount'] = triangle_values

    # Local clustering coefficient
    self.logger.debug("Computing local clustering coefficient...")
    clustering = analyst.local_clustering_coefficient(self.graph)
    lcc_values = []
    for entity_id in entity_df['entity_id']:
        vertex = self.graph.get_vertex(entity_id)
        lcc_values.append(clustering.get(vertex) if vertex else 0.0)
    entity_df['localClusteringCoefficient'] = lcc_values

    # Community detection (Louvain-like - PGX uses label propagation or conductance minimization)
    self.logger.debug("Computing community detection...")
    communities = analyst.partition_conductance_minimization(self.graph)
    comm_values = []
    for entity_id in entity_df['entity_id']:
        vertex = self.graph.get_vertex(entity_id)
        comm_values.append(communities.get(vertex) if vertex else -1)
    entity_df['communityId'] = comm_values

    # K-Core decomposition
    self.logger.debug("Computing k-core decomposition...")
    kcore = analyst.k_core(self.graph, min_core=1)
    kcore_values = []
    for entity_id in entity_df['entity_id']:
        vertex = self.graph.get_vertex(entity_id)
        kcore_values.append(kcore.get(vertex) if vertex else 0)
    entity_df['coreValue'] = kcore_values

    self.logger.info(f"Structural features extracted. Final shape: {entity_df.shape}")
    return entity_df
