from neo4j import GraphDatabase, Driver
import os
from dotenv import load_dotenv

def connect() -> Driver:
    '''
    Connect to the Neo4j database for the project
    '''
    load_dotenv()

    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")

    return GraphDatabase.driver(uri=uri, auth=(user, password)) # type: ignore

def restart():
    '''
    Deletes everything in the database: nodes, relationships and indexes
    '''
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