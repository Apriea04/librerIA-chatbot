import os
import pickle
from py2neo import Graph
from transformers import AutoModel, AutoTokenizer
import torch
from utils import db
from tqdm import tqdm
from models.embedding_manager import EmbeddingManager
from utils.env_loader import EnvLoader

env_loader = EnvLoader()
NEO4J_URI = env_loader.neo4j_uri
NEO4J_USER = env_loader.neo4j_user
NEO4J_PASSWORD = env_loader.neo4j_password
BATCH_SIZE = env_loader.batch_size


def connect_to_graph():
    return Graph(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


class DBManager:
    """
    dbManager is a class that manages graph projections and algorithms in a Neo4j database using the Graph Data Science (GDS) library.

    Methods:
        __init__():

        project_graph(projection_name: str, nodes: str | list[str] = "*", relations: str | list[str] = "*"):

        drop_projection(projection_name: str):

        get_projection(projection_name: str):
    """

    def __init__(self):
        """
        Initializes the ProjectionManager instance and establishes a connection to the Neo4j database.

        Attributes:
            db_connection (neo4j.GraphDatabase.driver): The connection object to interact with the Neo4j database.
        """
        self.db_connection = db.connect()
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.graph = None
        self.model = None
        self.embedding_manager = EmbeddingManager()

    def project_graph(
        self,
        projection_name: str,
        nodes: str | list[str] = "*",
        relations: str | list[str] = "*",
    ):
        """
        Projects a graph in the database using the specified projection name, nodes, and relations.
        Args:
            projection_name (str): The name of the graph projection.
            nodes (str | list[str], optional): The nodes to include in the projection. Defaults to "*".
            relations (str | list[str], optional): The relationships to include in the projection. Defaults to "*".
        Returns:
            None
        """

        with self.db_connection.session() as session:
            session.run(
                """
                CALL gds.graph.project(
                    $projection_name,
                    $nodes,
                    $relations
                )
                """,
                projection_name=projection_name,
                nodes=nodes,
                relations=relations,
            )

    def drop_projection(self, projection_name: str):
        """
        Drops a graph projection from the database.

        Args:
            projection_name (str): The name of the projection to be dropped.

        Returns:
            None
        """
        with self.db_connection.session() as session:
            session.run(
                """CALL gds.graph.drop($projection_name) YIELD graphName""",
                projection_name=projection_name,
            )

    def get_projection(self, projection_name: str):
        """
        Retrieves a projection from the database by its name.

        Args:
            projection_name (str): The name of the projection to retrieve.

        Returns:
            The result of the query, typically a single record containing the projection name.

        Raises:
            Any exceptions raised by the database connection or query execution.
        """
        with self.db_connection.session() as session:
            result = session.run(
                """
                       CALL gds.graph.list() YIELD graphName
                       WHERE graphName = $projection_name
                       RETURN graphName
                       """,
                projection_name=projection_name,
            )
            return result.single()

    def node2vec_write(
        self,
        projection_name: str,
        dimensions: int = 128,
        walk_length: int = 100,
        walks_per_node: int = 1000,
        iterations: int = 10,
        min_learning_rate: float = 0.000001,
    ) -> float:
        """
        Generates embeddings from a projection using Node2Vec in Neo4j and writes them to the DDBB
        adding to each node a new property called 'embedding'
        """

        with self.db_connection.session() as session:
            result = session.run(
                """CALL gds.node2vec.write($projection, { embeddingDimension: $dimensions, walkLength: $walk_length, walksPerNode: $walks_per_node, iterations: $iterations, minLearningRate: $min_learning_rate, writeProperty: 'embedding' })
                YIELD nodePropertiesWritten""",
                projection=projection_name,
                dimensions=dimensions,
                walk_length=walk_length,
                walks_per_node=walks_per_node,
                iterations=iterations,
                min_learning_rate=min_learning_rate,
            )

            record = result.single()
            return record["nodePropertiesWritten"] if record is not None else 0.0

    def fetch_data(self, query: str):
        """
        Fetches data from the database using the provided query.

        Args:
            query (str): The query to execute.

        Returns:
            The result of the query, typically a list of records.
        """
        with self.db_connection.session() as session:
            result = session.run(query)  # type: ignore
            return [record.data() for record in result]

    def generate_embeddings_for(self, node_label: str, node_property: str, node_id_property: str, model_name: str, batch_size: int=32):
        if node_id_property:
            query = f"MATCH (n:{node_label}) WHERE n.{node_property} IS NOT NULL RETURN n.{node_id_property} as nodeId, n.{node_property} as text"
        else:
            query = f"MATCH (n:{node_label}) WHERE n.{node_property} IS NOT NULL RETURN elementId(n) as nodeId, n.{node_property} as text"
        data = self.fetch_data(query)
        
        embeddings = []
        texts = [row["text"] for row in data]
        node_ids = [row["nodeId"] for row in data]
        
        with tqdm(total=len(texts), desc="Generating embeddings") as pbar:
            for i in range(0, len(texts), batch_size):
                batch_texts = texts[i:i + batch_size]
                batch_node_ids = node_ids[i:i + batch_size]
                batch_embeddings = self.embedding_manager.generate_text_embedding(batch_texts)
                embeddings.extend(zip(batch_node_ids, batch_embeddings))
                pbar.update(len(batch_texts))
                
                if len(embeddings) >= BATCH_SIZE * 100:
                    self._save_embeddings_to_db(embeddings, node_label, node_property, node_id_property)
                    embeddings = []

        if embeddings:
            self._save_embeddings_to_db(embeddings, node_label, node_property, node_id_property)

        vector_dimension = len(embeddings[0][1]) if embeddings else 0
        self.create_vector_index(node_label, f"{node_property}_embedding", vector_dimension)

    def _save_embeddings_to_db(self, embeddings, node_label, node_property, node_id_property):
        with tqdm(total=len(embeddings), desc="Writing to db") as pbar:
            for i in range(0, len(embeddings), BATCH_SIZE):
                batch = embeddings[i:i + BATCH_SIZE]
                if node_id_property:
                    query = f"""
                    UNWIND $batch AS row
                    MATCH (n:{node_label} {{{node_id_property}: row.nodeId}})
                    SET n.{node_property}_embedding = row.embedding
                    """
                else:
                    query = f"""
                    UNWIND $batch AS row
                    MATCH (n:{node_label}) WHERE elementId(n) = row.nodeId
                    SET n.{node_property}_embedding = row.embedding
                    """
                with self.db_connection.session() as session:
                    session.run(query, batch=[{"nodeId": node_id, "embedding": embedding} for node_id, embedding in batch]) # type: ignore
                pbar.update(len(batch))

    def export_property_to_pickle(self, node_label: str, node_property: str, node_id_property: str):
        if node_id_property:
            query = f"MATCH (n:{node_label}) WHERE n.{node_property} IS NOT NULL RETURN n.{node_id_property} as nodeId, n.{node_property} as text"
        else:
            query = f"MATCH (n:{node_label}) WHERE n.{node_property} IS NOT NULL RETURN elementId(n) as nodeId, n.{node_property} as text"
        data = self.fetch_data(query)
        
        output_file = f"{node_label}_{node_property}_texts.pkl"
        with open(output_file, mode='wb') as file:
            pickle.dump(data, file)
        
        print(f"Exported {len(data)} records to {output_file}")

    def create_vector_index(self, node_label: str, vector_property: str, vector_dimensions: int):
        query = f"""CREATE VECTOR INDEX {node_label}_{vector_property}_index IF NOT EXISTS FOR (n:{node_label}) ON (n.{vector_property}) OPTIONS {{ indexConfig: {{
            `vector.dimensions`: {vector_dimensions},
            `vector.similarity_function`: 'cosine'
            }}}}"""
        with self.db_connection.session() as session:
            session.run(query) # type: ignore
