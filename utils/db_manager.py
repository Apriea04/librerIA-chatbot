import os
from dotenv import load_dotenv
import pandas as pd
import torch
from utils import db
from utils import graph_to_pytorch as gtp
from torch_geometric.nn import GCNConv

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

    def generate_torch_embeddings(self, save_path: str):
        """
        Generates embeddings for all nodes in the graph using PyTorch Geometric
        and saves them to the specified file.

        Args:
            save_path (str): Path to the file where embeddings will be saved.
        """
        # 1. Fetch data from Neo4j and process it
        query = """
            MATCH (n)-[r]->(m)
            RETURN n AS source_node, m AS target_node, properties(n) AS node_props
        """
        data = self.fetch_data(query)

        # Create mappings for nodes
        node_to_idx = {}
        edge_index = []
        node_features = []

        for record in data:
            source, target = record["source_node"], record["target_node"]

            # Assign unique index to source node
            if source not in node_to_idx:
                node_to_idx[source] = len(node_to_idx)
                node_props = record["node_props"]
                node_features.append([float(v) for v in node_props.values() if isinstance(v, (int, float))])

            # Assign unique index to target node
            if target not in node_to_idx:
                node_to_idx[target] = len(node_to_idx)
                target_props = record.get("node_props", {})
                node_features.append([float(v) for v in target_props.values() if isinstance(v, (int, float))])

            # Add edge
            edge_index.append([node_to_idx[source], node_to_idx[target]])

        # Convert edge index and node features to tensors
        edge_index = torch.tensor(edge_index, dtype=torch.long).t().contiguous()
        node_features = torch.tensor(node_features, dtype=torch.float)

        # Create PyTorch Geometric data object
        from torch_geometric.data import Data
        graph_data = Data(x=node_features, edge_index=edge_index)

        # 2. Define GCN model for embedding generation
        class GCN(torch.nn.Module):
            def __init__(self, input_dim, hidden_dim, output_dim):
                super(GCN, self).__init__()
                self.conv1 = GCNConv(input_dim, hidden_dim)
                self.conv2 = GCNConv(hidden_dim, output_dim)

            def forward(self, data):
                x, edge_index = data.x, data.edge_index
                x = self.conv1(x, edge_index)
                x = torch.relu(x)
                x = self.conv2(x, edge_index)
                return x

        # Use GPU if available
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        graph_data = graph_data.to(device)

        # Initialize model
        input_dim = graph_data.x.size(1)
        hidden_dim = 64
        output_dim = 128  # Embedding size
        model = GCN(input_dim, hidden_dim, output_dim).to(device)

        # 3. Train the model
        optimizer = torch.optim.Adam(model.parameters(), lr=0.01, weight_decay=5e-4)
        model.train()
        for epoch in range(200):  # Adjust epochs as needed
            optimizer.zero_grad()
            embeddings = model(graph_data)
            loss = torch.nn.functional.mse_loss(embeddings, graph_data.x)  # Example loss
            loss.backward()
            optimizer.step()
            print(f"Epoch {epoch+1}, Loss: {loss.item()}")

        # 4. Save embeddings
        embeddings = embeddings.cpu().detach().numpy()
        node_ids = {v: k for k, v in node_to_idx.items()}  # Reverse mapping
        embeddings_df = pd.DataFrame(embeddings, index=[node_ids[i] for i in range(len(node_to_idx))])
        embeddings_df.to_csv(save_path, index=True)
        print(f"Embeddings saved to {save_path}")
