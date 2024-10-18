from neo4j import ManagedTransaction
import pandas as pd
from utils import db
import ast
import os
import dotenv

dotenv.load_dotenv()
BOOKS_PATH = os.getenv('BOOKS_PATH')
RATINGS_PATH = os.getenv('RATINGS_PATH')
BATCH_SIZE = int(os.getenv("BATCH_SIZE")) # type: ignore

def create_author_nodes(tx: ManagedTransaction, df: pd.DataFrame):
    """
    Función para crear nodos de autores en Neo4j a partir de un DataFrame.
    Evita duplicados utilizando MERGE y optimiza con UNWIND, procesando en bloques de 5000 filas.
    
    Args:
        tx: Sesión de transacción de Neo4j.
        df: DataFrame que contiene la columna 'authors' con listas de autores.
    """
    all_authors: list[str] = []
    total_rows = len(df)
    print(f"Procesando {total_rows} filas en bloques de {BATCH_SIZE}...")
    counter = 0

    # Procesar fila por fila
    for idx, authors_str in enumerate(df['authors']): # type: ignore
        if pd.notna(authors_str): # type: ignore
            try:
                authors_list = ast.literal_eval(authors_str) # type: ignore
                if isinstance(authors_list, list):
                    all_authors.extend(authors_list) # type: ignore
            except (ValueError, SyntaxError):
                continue

        # Ejecutar la query cada vez que alcancemos un múltiplo del tamaño del bloque (5000 filas)
        if (idx + 1) % BATCH_SIZE == 0 or (idx + 1) == total_rows:
            unique_authors: list[str] = list(set(all_authors))  # Remover duplicados dentro del bloque
            
            if unique_authors:  # Solo ejecutar la query si hay autores en el bloque
                query = """
                UNWIND $authors AS author_name
                MERGE (a:Author {name: author_name})
                """
                
                # Ejecutar la query con el bloque de autores
                tx.run(query, authors=unique_authors)
                print(f"Autores creados para filas {0 + BATCH_SIZE * counter} a {idx + 1} (autores únicos en este bloque: {len(unique_authors)})") # type: ignore
                counter += 1
            
            all_authors = []  # Limpiar la lista para el siguiente bloque
    print("Autores creados.")


def create_book_nodes(tx: ManagedTransaction, df: pd.DataFrame):
    '''
    Función para crear nodos de libros en Neo4j a partir de un DataFrame.
    Evita duplicados utilizando MERGE y optimiza con UNWIND, procesando en bloques.
    
    Args:
        tx: Sesión de transacción de Neo4j.
        df: DataFrame que contiene un libro por fila con las columnas 'Title', 'description', 'image'.
        BATCH_SIZE: Tamaño del bloque (batch) de filas que se procesarán en una sola query.
    '''
    print("Creando nodos de libros...")
    total_rows = len(df)
    
    # Inicializa una lista para acumular las filas en batch
    batch_list: list[dict[str, str]] = []

    for idx, row in df.iterrows(): # type: ignore
        title = row['Title'] # type: ignore
        description = row['description'] # type: ignore
        image = row['image'] # type: ignore

        # Agregar cada fila al batch_list
        batch_list.append({
            "title": title,
            "description": description,
            "image": image
        })
        
        # Ejecuta la query en bloque cuando se alcanza el tamaño del batch o es la última fila
        if (idx + 1) % BATCH_SIZE == 0 or (idx + 1) == total_rows: # type: ignore
            query = """
            UNWIND $batch_list AS row
            MERGE (b:Book {title: row.title})
            SET b.description = row.description,
                b.image = row.image
            """
            
            # Ejecuta la query con el lote actual
            tx.run(query, batch_list=batch_list)
            
            # Imprimir información de progreso
            print(f"Libros creados para filas {idx + 1 - len(batch_list) + 1} a {idx + 1}.") # type: ignore
            
            # Limpia el batch_list después de ejecutar la query
            batch_list = []
            
    print("Libros creados.")


def create_category_nodes(tx: ManagedTransaction, df: pd.DataFrame):
    # TODO: Implementar función para crear nodos de categorías
    pass

def create_publisher_nodes(tx: ManagedTransaction, df: pd.DataFrame):
    # TODO: Implementar función para crear nodos de editoriales
    pass

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
    df: pd.DataFrame = pd.read_csv(BOOKS_PATH) # type: ignore
    
    # Crear nodos
    with db.connect() as driver:
        with driver.session() as session:
            session.execute_write(create_author_nodes, df)
            session.execute_write(create_book_nodes, df)
if __name__ == "__main__":
    main()
