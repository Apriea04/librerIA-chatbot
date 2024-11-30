from neo4j import GraphDatabase, Driver
from utils.env_loader import EnvLoader

env_loader = EnvLoader()

def connect() -> Driver:
    """
    Connect to the Neo4j database for the project
    """
    uri = env_loader.neo4j_uri
    user = env_loader.neo4j_user
    password = env_loader.neo4j_password

    return GraphDatabase.driver(uri=uri, auth=(user, password))  # type: ignore


def restart():
    """
    Deletes everything in the database: nodes, relationships and indexes
    """
    with connect() as driver:
        with driver.session() as session:
            session.run("""
                CALL apoc.periodic.iterate(
                    'MATCH (n) RETURN n',
                    'DETACH DELETE n',
                    {batchSize: 100000}
                )
            """)
            session.run("CALL apoc.schema.assert({}, {})")
    print("Everything deleted")
