from neo4j import ManagedTransaction
import pandas as pd
from utils import db

import ast
import os
import dotenv

dotenv.load_dotenv()
BOOKS_PATH = os.getenv('BOOKS_PATH')
RATINGS_PATH = os.getenv('RATINGS_PATH')
BATCH_SIZE = int(os.getenv("BATCH_SIZE"))

def create_author_nodes(tx: ManagedTransaction, df: pd.DataFrame):
    """
    Función para crear nodos de autores en Neo4j a partir de un DataFrame.
    Evita duplicados utilizando MERGE y optimiza con UNWIND, procesando en bloques.
    
    Args:
        tx: Sesión de transacción de Neo4j.
        df: DataFrame que contiene la columna 'authors' con listas de autores.
    """
    all_authors = []
    total_rows = len(df)
    print(f"Procesando {total_rows} filas en bloques de {BATCH_SIZE}...")

    # Procesar fila por fila
    for idx, authors_str in enumerate(df['authors']):
        if pd.notna(authors_str):
            try:
                authors_list = ast.literal_eval(authors_str)
                if isinstance(authors_list, list):
                    all_authors.extend(authors_list)
            except (ValueError, SyntaxError):
                continue

        # Ejecutar la query cada vez que alcancemos un múltiplo del tamaño del bloque (5000 filas)
        if (idx + 1) % BATCH_SIZE == 0 or (idx + 1) == total_rows:
            unique_authors = list(set(all_authors))  # Remover duplicados dentro del bloque
            print(f"Procesando bloque de filas {idx + 1 - BATCH_SIZE + 1} a {idx + 1} (autores únicos en este bloque: {len(unique_authors)})")
            
            if unique_authors:  # Solo ejecutar la query si hay autores en el bloque
                query = """
                UNWIND $authors AS author_name
                MERGE (a:Author {name: author_name})
                """
                
                # Ejecutar la query con el bloque de autores
                tx.run(query, authors=unique_authors)
                print(f"Nodos insertados para filas {idx + 1 - BATCH_SIZE + 1} a {idx + 1}.")
            
            all_authors = []  # Limpiar la lista para el siguiente bloque

    print("Procesamiento completado.")

def main():
    """
    Main function to load the dataset, extract unique nodes, and create nodes and relationships in the database.

    Steps:
    1. Create nodes in the database.
    2. Create relationships in the database.

    Prints:
        "Nodos creados" after nodes are created.
        "Relaciones creadas" after relationships are created.
    """
    # Cargar el dataset
    df = pd.read_csv(BOOKS_PATH)
    
    # Crear nodos
    with db.connect() as driver:
        with driver.session() as session:
            session.execute_write(create_author_nodes, df)
    print("Autores creados")

if __name__ == "__main__":
    main()
