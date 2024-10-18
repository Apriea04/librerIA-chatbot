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
    print(f"Creando nodos de {printable_name}...")
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
    '''
    Función para crear nodos de editoriales en Neo4j a partir de un DataFrame.
    Evita duplicados utilizando MERGE y optimiza con UNWIND, procesando en bloques.
    
    Args:
        tx: Sesión de transacción de Neo4j.
        df: DataFrame que contiene filas de libros con la columna 'publisher'.
    '''
    print("Creando nodos de libros...")
    total_rows = len(df)
    
    # Inicializa una lista para acumular las filas en batch
    batch_list: list[str] = []

    for idx, row in df.iterrows(): # type: ignore
        publisher = str(row['publisher']) # type: ignore

        # Agregar cada fila al batch_list
        batch_list.append(publisher)
        
        # Ejecuta la query en bloque cuando se alcanza el tamaño del batch o es la última fila
        if (idx + 1) % BATCH_SIZE == 0 or (idx + 1) == total_rows: # type: ignore
            query = """
            UNWIND $batch_list AS publisher
            MERGE (p:Publisher {name: publisher})
            """
            
            # Ejecuta la query con el lote actual
            tx.run(query, batch_list=batch_list)
            
            # Imprimir información de progreso
            print(f"Editoriales creadas para filas {idx + 1 - len(batch_list) + 1} a {idx + 1}.") # type: ignore
            
            # Limpia el batch_list después de ejecutar la query
            batch_list = []
            
    print("Editoriales creadas.") 
    
def create_books_authors_relations(tx: ManagedTransaction, df: pd.DataFrame) -> None:
    """
    Función para crear relaciones entre libros y autores en Neo4j.
    Cada relación es del tipo (:Book)-[:WRITTEN_BY]->(:Author).
    
    Args:
        tx (ManagedTransaction): Sesión de transacción de Neo4j.
        df (pd.DataFrame): DataFrame que contiene un libro por fila con las columnas 'Title' y 'authors'.
    """
    print("Creando relaciones entre libros y autores...")
    total_rows = len(df)
    batch_list: list[dict[str, str]] = []
    
    for idx, row in df.iterrows():  #type:ignore # Iterar sobre las filas del DataFrame
        title = row['Title'] #type:ignore  # Título del libro
        authors_str = row['authors'] #type:ignore  # Autores del libro (cadena con lista)
        
        if pd.notna(authors_str): #type:ignore # Verificar si hay autores en esta fila
            try:
                authors = ast.literal_eval(authors_str) #type:ignore # Convertir la cadena en lista
                if isinstance(authors, list):
                    for author in authors: #type:ignore
                        batch_list.append({
                            "title": title,
                            "author": author
                        })
            except (ValueError, SyntaxError):
                continue

        # Ejecutar la query en bloques (cada BATCH_SIZE filas o al final del DataFrame)
        if (idx + 1) % BATCH_SIZE == 0 or (idx + 1) == total_rows: #type:ignore
            if batch_list:  # Solo ejecutar si hay datos en el batch
                query = """
                UNWIND $batch_list AS row
                MATCH (b:Book {title: row.title})
                MATCH (a:Author {name: row.author})
                MERGE (b)-[:WRITTEN_BY]->(a)
                """
                tx.run(query, batch_list=batch_list)
                print(f"Relaciones creadas para filas {idx + 1 - len(batch_list) + 1} a {idx + 1}.") #type:ignore
                batch_list = []  # Limpiar el batch_list para el siguiente bloque

    print("Relaciones entre libros y autores creadas.")

def create_books_categories_relations(tx: ManagedTransaction, df: pd.DataFrame) -> None:
    """
    Función para crear relaciones entre libros y categorías en Neo4j.
    Cada relación es del tipo (:Book)-[:BELONGS_TO]->(:Category).
    
    Args:
        tx (ManagedTransaction): Sesión de transacción de Neo4j.
        df (pd.DataFrame): DataFrame que contiene un libro por fila con las columnas 'Title' y 'categories'.
    """
    print("Creando relaciones entre libros y categorías...")
    total_rows = len(df)
    batch_list: list[dict[str, str]] = []
    
    for idx, row in df.iterrows(): #type:ignore # Iterar sobre las filas del DataFrame
        title = row['Title'] #type:ignore # Título del libro
        categories_str = row['categories'] #type:ignore # Categorías del libro (cadena con lista)
        
        if pd.notna(categories_str): #type:ignore # Verificar si hay categorías en esta fila
            try:
                categories = ast.literal_eval(categories_str) #type:ignore # Convertir la cadena en lista
                if isinstance(categories, list):
                    for category in categories: #type:ignore
                        batch_list.append({
                            "title": title,
                            "category": category
                        })
            except (ValueError, SyntaxError):
                continue

        # Ejecutar la query en bloques (cada BATCH_SIZE filas o al final del DataFrame)
        if (idx + 1) % BATCH_SIZE == 0 or (idx + 1) == total_rows: #type:ignore
            if batch_list:  # Solo ejecutar si hay datos en el batch
                query = """
                UNWIND $batch_list AS row
                MATCH (b:Book {title: row.title})
                MATCH (c:Category {name: row.category})
                MERGE (b)-[:BELONGS_TO]->(c)
                """
                tx.run(query, batch_list=batch_list)
                print(f"Relaciones creadas para filas {idx + 1 - len(batch_list) + 1} a {idx + 1}.") #type:ignore
                batch_list = []  # Limpiar el batch_list para el siguiente bloque

    print("Relaciones entre libros y categorías creadas.")

def create_books_publishers_relations(tx: ManagedTransaction, df: pd.DataFrame) -> None:
    """
    Función para crear relaciones entre libros y editoriales en Neo4j.
    Cada relación es del tipo (:Book)-[:PUBLISHED_BY {publishDate: ...}]->(:Publisher).
    
    Args:
        tx (ManagedTransaction): Sesión de transacción de Neo4j.
        df (pd.DataFrame): DataFrame que contiene un libro por fila con las columnas 'Title', 'publisher' y 'publishedDate'.
    """
    print("Creando relaciones entre libros y editoriales...")
    total_rows = len(df)
    batch_list: list[dict[str, str]] = []
    
    for idx, row in df.iterrows(): #type:ignore # Iterar sobre las filas del DataFrame
        title = row['Title'] #type:ignore # Título del libro
        publisher = row['publisher'] #type:ignore # Editorial del libro
        publish_date = row['publishedDate'] #type:ignore # Fecha de publicación del libro
        
        if pd.notna(publisher) and pd.notna(publish_date): #type:ignore # Verificar si hay editorial y fecha de publicación en esta fila
            batch_list.append({
                "title": title,
                "publisher": publisher,
                "publishDate": publish_date
            })

        # Ejecutar la query en bloques (cada BATCH_SIZE filas o al final del DataFrame)
        if (idx + 1) % BATCH_SIZE == 0 or (idx + 1) == total_rows: #type:ignore
            if batch_list:  # Solo ejecutar si hay datos en el batch
                query = """
                UNWIND $batch_list AS row
                MATCH (b:Book {title: row.title})
                MATCH (p:Publisher {name: row.publisher})
                MERGE (b)-[r:PUBLISHED_BY]->(p)
                SET r.publishDate = row.publishDate
                """
                tx.run(query, batch_list=batch_list)
                print(f"Relaciones creadas para filas {idx + 1 - len(batch_list) + 1} a {idx + 1}.") #type:ignore
                batch_list = []  # Limpiar el batch_list para el siguiente bloque

    print("Relaciones entre libros y editoriales creadas.")


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
            session.execute_write(create_publisher_nodes, df)
            session.execute_write(create_books_authors_relations, df)
            session.execute_write(create_books_categories_relations, df)
            session.execute_write(create_books_publishers_relations, df)
if __name__ == "__main__":
    main()
