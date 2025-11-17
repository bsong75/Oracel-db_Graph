import pandas as pd
import logging
import oracledb
import os
from typing import Optional
import pypgx as pgx


class PestDataAnalyzer:
    """Oracle Graph PGX-based pest data analyzer"""

    def __init__(self, csv_path: str, oracle_host: str, oracle_port: int,
                 oracle_service: str, oracle_user: str, oracle_password: str,
                 pgx_base_url: Optional[str] = None, log_level=logging.INFO):
        self.csv_path = csv_path
        self.oracle_host = oracle_host
        self.oracle_port = oracle_port
        self.oracle_service = oracle_service
        self.oracle_user = oracle_user
        self.oracle_password = oracle_password
        self.pgx_base_url = pgx_base_url or f"http://{oracle_host}:7007"

        self.connection = None
        self.cursor = None
        self.pgx_instance = None
        self.pgx_session = None
        self.graph = None
        self.df = None

        # Setup logger
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(log_level)

        # Create console handler if no handlers exist
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def connect(self):
        """Connect to Oracle database and PGX server with retry logic"""
        import time

        self.logger.info(f"Connecting to Oracle database at {self.oracle_host}:{self.oracle_port}...")

        # Connect to Oracle database with retry logic
        dsn = oracledb.makedsn(self.oracle_host, self.oracle_port, service_name=self.oracle_service)

        max_retries = 30
        retry_delay = 5

        for attempt in range(1, max_retries + 1):
            try:
                self.logger.info(f"Connection attempt {attempt}/{max_retries}...")
                self.connection = oracledb.connect(
                    user=self.oracle_user,
                    password=self.oracle_password,
                    dsn=dsn
                )
                self.cursor = self.connection.cursor()
                self.logger.info("Successfully connected to Oracle database")
                break
            except oracledb.DatabaseError as e:
                if attempt < max_retries:
                    self.logger.warning(f"Connection failed: {str(e)[:100]}. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    self.logger.error("Failed to connect to Oracle after all retries")
                    raise

        # Connect to PGX server
        self.logger.info(f"Connecting to PGX server at {self.pgx_base_url}...")
        try:
            import requests
            # Check if PGX server is available
            response = requests.get(f"{self.pgx_base_url}/version", timeout=5)
            if response.status_code == 200:
                self.logger.info(f"PGX server available: {response.json()}")
                # TODO: Set up pypgx connection when available
                # For now, we'll use REST API directly
                self.pgx_instance = None
                self.pgx_session = None
                self.graph = None
                self.logger.info("PGX REST API ready for use")
            else:
                raise Exception(f"PGX server returned status {response.status_code}")
        except Exception as e:
            self.logger.warning(f"PGX server not available: {e}")
            self.logger.info("Will use Oracle Database built-in graph capabilities (PGQL)")
            self.pgx_instance = None
            self.pgx_session = None
            self.graph = None

    def load_data(self):
        """Load CSV data"""
        self.logger.info(f"Loading data from {self.csv_path}")
        self.df = pd.read_csv(self.csv_path)
        self.logger.info(f"Loaded {len(self.df)} rows")

        # Data validation and cleaning
        self.logger.info("Validating and cleaning data...")

        # Check for 'TP' header row and skip if present
        if self.df.iloc[0]['TARGET_PROXY'] == 'TP' or str(self.df.iloc[0]['ENTY_ID']) == 'ENTY_ID':
            self.logger.warning("Detected header row in data, removing it...")
            self.df = self.df.iloc[1:].reset_index(drop=True)

        # Convert numeric columns
        numeric_columns = ['TARGET_PROXY', 'ENTY_EXAMS_30D', 'ENTY_PESTS_30D',
                          'ENTY_EXAMS_90D', 'ENTY_PESTS_90D', 'ENTY_EXAMS_1YR', 'ENTY_PESTS_1YR']
        for col in numeric_columns:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce').fillna(0).astype(int)

        self.logger.info(f"After cleaning: {len(self.df)} rows")
        unique_count = self.df['ENTY_ID'].nunique()
        self.logger.info(f"Number of distinct ENTY_ID: {unique_count}")

    def clear_database(self):
        """Clear existing data from all tables"""
        self.logger.warning("Clearing all graph data from database...")

        tables = [
            'has_inspection_result_edges',
            'has_weather_edges',
            'is_from_edges',
            'shipped_in_edges',
            'inspections',
            'country_months',
            'target_proxies',
            'months',
            'countries',
            'entities'
        ]

        for table in tables:
            try:
                self.cursor.execute(f"DELETE FROM {table}")
                self.logger.debug(f"Cleared table: {table}")
            except Exception as e:
                self.logger.debug(f"Could not clear {table}: {str(e)[:100]}")

        self.connection.commit()
        self.logger.info("Database cleared")

    def create_nodes(self):
        """Create all nodes in the graph (insert into vertex tables)"""
        self.logger.info("Creating graph nodes...")

        # Create Entity nodes
        entities = self.df['ENTY_ID'].unique()
        self.logger.debug(f"Creating {len(entities)} entity nodes")
        for entity_id in entities:
            self.cursor.execute(
                "INSERT INTO entities (entity_id, entity_type) VALUES (:1, :2)",
                (str(entity_id), 'inspection_entity')
            )

        # Create Country nodes
        countries = self.df['CTRY_CODE'].unique()
        self.logger.debug(f"Creating {len(countries)} country nodes")
        for country_code in countries:
            self.cursor.execute(
                "INSERT INTO countries (country_code, country_name) VALUES (:1, :2)",
                (str(country_code), str(country_code))
            )

        # Create Month nodes
        months = self.df['MONTH'].unique()
        self.logger.debug(f"Creating {len(months)} month nodes")
        for idx, month in enumerate(months):
            self.cursor.execute(
                "INSERT INTO months (month_name, month_number) VALUES (:1, :2)",
                (str(month), int(idx + 1))
            )

        # Create TargetProxy nodes
        targets = self.df['TARGET_PROXY'].unique()
        self.logger.debug(f"Creating {len(targets)} target proxy nodes")
        for target in targets:
            label = 'no_pest' if int(target) == 0 else 'pest'
            self.cursor.execute(
                "INSERT INTO target_proxies (target_value, target_label) VALUES (:1, :2)",
                (int(target), label)
            )

        # Create CountryMonth composite nodes
        self.logger.debug("Creating country-month composite nodes")
        country_month_set = set()
        for _, row in self.df.iterrows():
            cm_id = f"{row['CTRY_CODE']}_{row['MONTH']}"
            if cm_id not in country_month_set:
                country_month_set.add(cm_id)
                self.cursor.execute(
                    "INSERT INTO country_months (cm_id, country_code, month_name) VALUES (:1, :2, :3)",
                    (cm_id, str(row['CTRY_CODE']), str(row['MONTH']))
                )

        self.connection.commit()
        self.logger.info("All nodes created successfully")

    def create_inspections_and_relationships(self):
        """Create inspection nodes and all relationships (edges)"""
        self.logger.info(f"Creating {len(self.df)} inspection nodes and relationships...")

        for _, row in self.df.iterrows():
            cm_id = f"{row['CTRY_CODE']}_{row['MONTH']}"

            # Insert inspection
            self.cursor.execute("""
                INSERT INTO inspections (
                    entity_id, country_code, month_name, target_proxy,
                    exams_30d, pests_30d, exams_90d, pests_90d, exams_1yr, pests_1yr, has_pest
                ) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)
            """, (
                str(row['ENTY_ID']), str(row['CTRY_CODE']), str(row['MONTH']),
                int(row['TARGET_PROXY']), int(row['ENTY_EXAMS_30D']), int(row['ENTY_PESTS_30D']),
                int(row['ENTY_EXAMS_90D']), int(row['ENTY_PESTS_90D']),
                int(row['ENTY_EXAMS_1YR']), int(row['ENTY_PESTS_1YR']),
                1 if row['TARGET_PROXY'] == 1 else 0
            ))

        self.connection.commit()

        # Create entity relationship edges (SHIPPED_IN, IS_FROM, HAS_WEATHER, HAS_INSPECTION_RESULT)
        self.logger.info("Creating entity relationship edges...")

        # Get unique entity relationships
        entity_months = self.df[['ENTY_ID', 'MONTH']].drop_duplicates()
        for _, row in entity_months.iterrows():
            self.cursor.execute(
                "INSERT INTO shipped_in_edges (entity_id, month_name) VALUES (:1, :2)",
                (str(row['ENTY_ID']), str(row['MONTH']))
            )

        entity_countries = self.df[['ENTY_ID', 'CTRY_CODE']].drop_duplicates()
        for _, row in entity_countries.iterrows():
            self.cursor.execute(
                "INSERT INTO is_from_edges (entity_id, country_code) VALUES (:1, :2)",
                (str(row['ENTY_ID']), str(row['CTRY_CODE']))
            )

        entity_weather = self.df[['ENTY_ID', 'CTRY_CODE', 'MONTH']].drop_duplicates()
        for _, row in entity_weather.iterrows():
            cm_id = f"{row['CTRY_CODE']}_{row['MONTH']}"
            self.cursor.execute(
                "INSERT INTO has_weather_edges (entity_id, cm_id) VALUES (:1, :2)",
                (str(row['ENTY_ID']), cm_id)
            )

        entity_targets = self.df[['ENTY_ID', 'TARGET_PROXY']].drop_duplicates()
        for _, row in entity_targets.iterrows():
            self.cursor.execute(
                "INSERT INTO has_inspection_result_edges (entity_id, target_value) VALUES (:1, :2)",
                (str(row['ENTY_ID']), int(row['TARGET_PROXY']))
            )

        self.connection.commit()
        self.logger.info("Inspections and relationships created successfully")

    def get_stats(self):
        """Get database statistics"""
        self.logger.info("Gathering database statistics...")

        # Count nodes
        self.cursor.execute("SELECT COUNT(*) FROM entities")
        entity_count = self.cursor.fetchone()[0]

        self.cursor.execute("SELECT COUNT(*) FROM inspections")
        inspection_count = self.cursor.fetchone()[0]

        # Count edges
        edge_tables = ['shipped_in_edges', 'is_from_edges', 'has_weather_edges', 'has_inspection_result_edges']
        total_edges = 0
        for table in edge_tables:
            self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = self.cursor.fetchone()[0]
            total_edges += count
            self.logger.info(f"  {table}: {count}")

        self.logger.info(f"Database contains {entity_count} entities, {inspection_count} inspections")
        self.logger.info(f"Total edges: {total_edges}")

    def create_projection(self):
        """
        Skip property graph DDL creation - Oracle Database Free might not support it.
        Graph data is already in tables and can be analyzed with SQL queries.
        """
        self.logger.info("Graph data loaded in tables - ready for analysis")
        self.logger.info("Note: Oracle Database Free Edition may not support CREATE PROPERTY GRAPH DDL")
        self.logger.info("Will use PGX Server for graph analytics instead")

        # The data is already in the tables - no property graph DDL needed for PGX
        # PGX Server will load the graph from tables when we connect to it
        self.graph = None

    def run_pgql_query(self, query):
        """Execute a PGQL query on the property graph"""
        pgql_sql = f"""
            SELECT *
            FROM GRAPH_TABLE (pest_graph
                MATCH {query}
                COLUMNS (*)
            )
        """
        self.cursor.execute(pgql_sql)
        return self.cursor.fetchall()

    def cleanup(self):
        """Clean up PGX resources"""
        self.logger.info("Cleaning up resources...")

        if self.graph:
            try:
                self.graph.destroy()
                self.logger.debug("Graph destroyed successfully")
            except Exception as e:
                self.logger.debug(f"Graph cleanup: {str(e)}")

        self.logger.info("Resource cleanup completed")

    def close(self):
        """Close database connection and PGX session"""
        if self.pgx_session:
            try:
                self.cleanup()
                self.pgx_session.close()
                self.logger.info("PGX session closed")
            except Exception as e:
                self.logger.error(f"Error closing PGX session: {e}")

        if self.cursor:
            try:
                self.cursor.close()
            except Exception as e:
                self.logger.debug(f"Cursor close error: {e}")

        if self.connection:
            try:
                self.connection.close()
                self.logger.info("Database connection closed successfully")
            except Exception as e:
                self.logger.error(f"Error closing connection: {e}")

    def run_full_analysis(self):
        """Run the complete analysis pipeline"""
        self.logger.info("Starting full pest data analysis pipeline")

        try:
            # Setup
            self.connect()
            self.load_data()
            self.clear_database()

            # Build graph
            self.create_nodes()
            self.create_inspections_and_relationships()
            self.get_stats()

            # Analysis
            self.create_projection()

            # TODO: Add algorithm implementations
            # df = self.extract_structural_features()
            # df = self.extract_node_embeddings()
            # final_df = self.run_graphsage()

            self.logger.info("Full analysis pipeline completed successfully")
            self.logger.info("Graph data ready - use PGX Server for advanced analytics")

            return None

        except Exception as e:
            self.logger.error(f"Analysis pipeline failed: {str(e)}")
            raise
        finally:
            # Always close connection
            self.close()


def main():
    # Configuration from environment variables or defaults
    CSV_FILE_PATH = os.getenv("CSV_FILE_PATH", "/app/data/pest_data.csv")
    ORACLE_HOST = os.getenv("ORACLE_HOST", "oracle-db")
    ORACLE_PORT = int(os.getenv("ORACLE_PORT", "1521"))
    ORACLE_SERVICE = os.getenv("ORACLE_SERVICE", "GRAPHDB")
    ORACLE_USER = os.getenv("ORACLE_USER", "graphuser")
    ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "GraphPassword123")
    PGX_BASE_URL = os.getenv("PGX_BASE_URL", "http://oracle-db:7007")

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('/app/pest_analysis.log'),
            logging.StreamHandler()
        ]
    )

    logger = logging.getLogger(__name__)
    logger.info("Starting pest analysis application with Oracle Graph PGX")

    analyzer = None
    try:
        # Create analyzer and run analysis
        analyzer = PestDataAnalyzer(
            CSV_FILE_PATH, ORACLE_HOST, ORACLE_PORT,
            ORACLE_SERVICE, ORACLE_USER, ORACLE_PASSWORD,
            PGX_BASE_URL
        )
        final_df = analyzer.run_full_analysis()

        logger.info("Analysis completed successfully")
        return final_df

    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
        raise
    finally:
        # Ensure cleanup always happens
        if analyzer:
            analyzer.close()
        logger.info("Application shutdown complete")


if __name__ == "__main__":
    main()
