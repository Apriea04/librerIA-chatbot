import os
from dotenv import load_dotenv
import pandas as pd
from py2neo import Graph
from transformers import AutoModel, AutoTokenizer
import torch
from utils import db
from utils import graph_to_pytorch as gtp

from sentence_transformers import SentenceTransformer

load_dotenv(override=True)
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")


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
        self.tokenizer = None
        self.model = None

    def _load_tokenizer(self, model_name: str):
        if not self.graph:
            self.graph = Graph(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

        # Load the model and tokenizer
        if self.tokenizer is not None:
            del self.tokenizer
        if self.model is not None:
            del self.model
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name).to(self.device)

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

    def _generate_text_embedding(self, text: str):

        inputs = self.tokenizer(
            text, return_tensors="pt", padding=True, truncation=True
        )
        inputs = {key: val.to(self.device) for key, val in inputs.items()}
        with torch.no_grad():
            outputs = self.model(**inputs)
            embedding = outputs.last_hidden_state.mean(dim=1)
        return embedding.cpu().numpy().tolist()

    def generate_embeddings_for(self, node_label: str, node_property:str, model_name: str):
        self._load_tokenizer(model_name)
        return self._generate_text_embedding("Había una vez un barco chiquito")