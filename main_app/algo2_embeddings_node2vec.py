import pandas as pd
import numpy as np
from pypgx.api.mllib import DeepWalkModel


def extract_node_embeddings(self, method='deepwalk', embedding_dim=64):
    """
    Extract node embeddings using Oracle PGX graph learning methods

    Note: Oracle PGX does not have a direct Node2Vec implementation.
    This uses DeepWalk, which is similar to Node2Vec with parameters p=1, q=1.
    DeepWalk performs random walks and learns node embeddings using skip-gram.

    Args:
        method: 'deepwalk' (PGX doesn't support node2vec directly)
        embedding_dim: Dimension of embeddings (default 64)
    """
    self.logger.info(f"Extracting node embeddings using {method}...")

    if not self.graph:
        raise ValueError("Graph must be loaded before extracting embeddings")

    # Get all entity vertices
    vertices = self.graph.query_pgql("""
        SELECT v.entity_id
        FROM MATCH (v:entities)
    """).to_pandas()

    entity_df = vertices.rename(columns={'entity_id': 'entity_id'})
    entity_df['nodeId'] = range(len(entity_df))

    if method == 'deepwalk':
        # DeepWalk - similar to Node2Vec with p=1, q=1
        self.logger.debug(f"Training DeepWalk model with {embedding_dim} dimensions...")

        # Create DeepWalk model
        model = DeepWalkModel()
        model.set_walk_length(10)
        model.set_walks_per_vertex(10)
        model.set_embedding_dimension(embedding_dim)
        model.set_window_size(5)
        model.set_learning_rate(0.025)
        model.set_min_learning_rate(0.0001)
        model.set_num_epochs(5)
        model.set_batch_size(128)

        # Train the model
        self.logger.debug("Training DeepWalk model...")
        embeddings = model.fit_model(self.graph, self.pgx_session)

        # Extract embeddings for each vertex
        self.logger.debug("Extracting vertex embeddings...")
        embedding_vectors = []
        for entity_id in entity_df['entity_id']:
            vertex = self.graph.get_vertex(entity_id)
            if vertex:
                emb = embeddings.get(vertex)
                embedding_vectors.append(emb if emb is not None else [0.0] * embedding_dim)
            else:
                embedding_vectors.append([0.0] * embedding_dim)

        # Convert to DataFrame
        embedding_df = pd.DataFrame(embedding_vectors)
        embedding_df.columns = [f'{method}_dim_{i}' for i in range(embedding_dim)]

    else:
        raise ValueError(f"Unknown embedding method: {method}. PGX supports 'deepwalk'.")

    # Combine with entity data
    final_df = pd.concat([
        entity_df[['nodeId', 'entity_id']],
        embedding_df
    ], axis=1)

    self.logger.info(f"{method} embeddings extracted. Shape: {final_df.shape}")
    return final_df


def extract_node_embeddings_alternative(self, embedding_dim=64):
    """
    Alternative embedding extraction using PGX's supervised learning capabilities

    This method uses PGX's built-in vertex embedding based on graph structure.
    It's a simpler approach that doesn't require training but provides structural embeddings.
    """
    self.logger.info("Extracting structural node embeddings using PGX...")

    if not self.graph:
        raise ValueError("Graph must be loaded before extracting embeddings")

    from pypgx.api.analyst import Analyst
    analyst = Analyst(self.pgx_session)

    # Get all entity vertices
    vertices = self.graph.query_pgql("""
        SELECT v.entity_id
        FROM MATCH (v:entities)
    """).to_pandas()

    entity_df = vertices.rename(columns={'entity_id': 'entity_id'})
    entity_df['nodeId'] = range(len(entity_df))

    # Use multiple graph metrics as embedding features
    self.logger.debug("Computing graph-based features for embeddings...")

    features = {}

    # PageRank as feature
    pagerank = analyst.pagerank(self.graph, tol=0.001, max_iter=100)
    features['pagerank'] = []

    # Degree
    features['degree'] = []

    # Betweenness
    betweenness = analyst.betweenness_centrality(self.graph)
    features['betweenness'] = []

    # Closeness
    closeness = analyst.closeness_centrality_unit_length(self.graph)
    features['closeness'] = []

    # Extract feature values
    for entity_id in entity_df['entity_id']:
        vertex = self.graph.get_vertex(entity_id)
        if vertex:
            features['pagerank'].append(pagerank.get(vertex) or 0.0)
            features['degree'].append(float(vertex.degree()))
            features['betweenness'].append(betweenness.get(vertex) or 0.0)
            features['closeness'].append(closeness.get(vertex) or 0.0)
        else:
            features['pagerank'].append(0.0)
            features['degree'].append(0.0)
            features['betweenness'].append(0.0)
            features['closeness'].append(0.0)

    # Create embedding DataFrame (these are structural features, not learned embeddings)
    embedding_df = pd.DataFrame(features)
    embedding_df.columns = [f'struct_emb_{col}' for col in features.keys()]

    # Combine with entity data
    final_df = pd.concat([
        entity_df[['nodeId', 'entity_id']],
        embedding_df
    ], axis=1)

    self.logger.info(f"Structural embeddings extracted. Shape: {final_df.shape}")
    return final_df
