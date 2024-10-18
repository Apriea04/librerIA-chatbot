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

def create_nodes_from_col_list(tx: ManagedTransaction, df: pd.DataFrame, column_name: str, label: str, printable_name: str) -> None:
    """
    Función generalizada para crear nodos en Neo4j a partir de un DataFrame.
    Evita duplicados utilizando MERGE y optimiza con UNWIND, procesando en bloques de BATCH_SIZE filas.
    
    Args:
        tx (ManagedTransaction): Sesión de transacción de Neo4j.
        df (pd.DataFrame): DataFrame que contiene los datos a procesar.
        column_name (str): El nombre de la columna que contiene los datos (ej. 'authors', 'genres').
        label (str): El tipo de nodo que se creará en Neo4j (ej. 'Author', 'Genre').
        printable_name (str): El nombre del nodo que se mostrará en los mensajes de progreso (ej. 'Autores', 'Géneros').
    """
    all_elements: list[str] = []
    total_rows = len(df)
    print(f"Procesando {total_rows} filas en bloques de {BATCH_SIZE}...")
    counter = 0

    # Procesar fila por fila
    for idx, element_str in enumerate(df[column_name]):  # type: ignore # Obtener los elementos de la columna
        if pd.notna(element_str): # type: ignore
            try:
                # Interpretar la cadena como una lista
                element_list = ast.literal_eval(element_str) if isinstance(element_str, str) else element_str # type: ignore
                if isinstance(element_list, list):
                    all_elements.extend(element_list)  #type:ignore # Agregar los elementos al lote actual
            except (ValueError, SyntaxError):
                continue

        # Ejecutar la query en bloques (cada BATCH_SIZE filas o al final del DataFrame)
        if (idx + 1) % BATCH_SIZE == 0 or (idx + 1) == total_rows:
            unique_elements: list[str] = list(set(all_elements))  # Remover duplicados dentro del bloque
            
            if unique_elements:  # Ejecutar solo si hay elementos en el bloque
                query = f"""
                UNWIND $elements AS element_name
                MERGE (e:{label} {{name: element_name}})
                """
                
                # Ejecutar la query con el bloque actual de elementos
                tx.run(query, elements=unique_elements) # type: ignore
                print(f"{printable_name.capitalize()} creados para filas {0 + BATCH_SIZE * counter} a {idx + 1} ({printable_name} únicos en este bloque: {len(unique_elements)})")
                counter += 1
            
            all_elements = []  # Limpiar la lista para el siguiente bloque

    print(f"{printable_name.capitalize()} creados.")

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
            session.execute_write(create_nodes_from_col_list, df, 'authors', 'Author', 'Autores')
            session.execute_write(create_book_nodes, df)
            session.execute_write(create_nodes_from_col_list, df, 'categories', 'Category', 'Cateogrías')
if __name__ == "__main__":
    main()
