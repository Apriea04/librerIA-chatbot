from neo4j import ManagedTransaction
import pandas as pd
from utils import db
import ast
import os
import dotenv
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor

dotenv.load_dotenv(override=True)
BOOKS_PATH = os.getenv('BOOKS_PATH')
RATINGS_PATH = os.getenv('RATINGS_PATH')
BATCH_SIZE = int(os.getenv("BATCH_SIZE"))  # type: ignore
NUM_WORKERS = int(os.getenv("NUM_WORKERS", 4))  # Número de procesos para paralelización

# Nueva función para manejar la conexión dentro de cada proceso
def execute_in_new_session(func, df, *args):
    """
    Crea una nueva sesión de Neo4j dentro de cada proceso.
    Esto evita el problema de serialización de las conexiones compartidas.
    """ 
    with db.connect() as driver:  # Cada proceso crea su propia conexión
        with driver.session() as session:
            return session.execute_write(func, df, *args)

def parallel_executor(func, df, *args):
    """
    Ejecuta la función pasada como argumento en paralelo usando un ProcessPoolExecutor.
    
    Args:
        func (callable): Función a ejecutar.
        df (pd.DataFrame): DataFrame con los datos a procesar.
        *args: Argumentos adicionales que la función puede necesitar.
    """
    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        future = executor.submit(func, df, *args)
        return future.result()  # Espera que se complete la tarea
    

def create_nodes_from_col_list(tx: ManagedTransaction, df: pd.DataFrame, column_name: str, label: str, printable_name: str) -> None:
    """
    Función generalizada para crear nodos en Neo4j a partir de un DataFrame.
    Evita duplicados utilizando MERGE y optimiza con UNWIND, procesando en bloques de BATCH_SIZE filas.
    """
    all_elements: list[str] = []
    total_rows = len(df)
    print(f"Creando nodos de {printable_name}...")
    counter = 0

    for idx, element_str in enumerate(df[column_name]):  # type: ignore
        if pd.notna(element_str):  # type: ignore
            try:
                element_list = ast.literal_eval(element_str) if isinstance(element_str, str) else element_str  # type: ignore
                if isinstance(element_list, list):
                    all_elements.extend(element_list)  # type: ignore
            except (ValueError, SyntaxError):
                continue

        # Ejecutar la query en bloques
        if (idx + 1) % BATCH_SIZE == 0 or (idx + 1) == total_rows:
            unique_elements: list[str] = list(set(all_elements))
            if unique_elements:
                query = f"""
                UNWIND $elements AS element_name
                MERGE (e:{label} {{name: element_name}})
                """
                tx.run(query, elements=unique_elements)  # type: ignore
                print(f"{printable_name.capitalize()} creados para filas {0 + BATCH_SIZE * counter} a {idx + 1} (Elementos únicos en este bloque: {len(unique_elements)})")
                counter += 1
            all_elements = []

    print(f"{printable_name.capitalize()} creados.")

def create_book_nodes(tx: ManagedTransaction, df: pd.DataFrame, batch_size: int = 10000):
    '''
    Función optimizada para crear nodos de libros en Neo4j a partir de un DataFrame con barra de progreso.
    
    Parámetros:
    - tx: Transacción gestionada de Neo4j.
    - df: DataFrame de pandas que contiene los datos de los libros.
    - batch_size: Tamaño de cada lote de inserción.
    '''
    total_rows = len(df)
    batch_list = []

    # Uso de itertuples para una iteración más rápida
    for row in tqdm(df.itertuples(index=False), total=total_rows, desc="Creando nodos de libros"):
        title = row.Title
        description = row.description
        image = row.image

        if pd.notna(title):
            batch_list.append({
                "title": title,
                "description": description,
                "image": image
            })

        # Ejecutar la consulta cada 'batch_size' filas
        if len(batch_list) >= batch_size:
            query = """
            UNWIND $batch_list AS row
            MERGE (b:Book {title: row.title})
            SET b.description = row.description,
                b.image = row.image
            """
            tx.run(query, batch_list=batch_list)
            batch_list = []  # Resetear la lista de lotes

    # Ejecutar cualquier lote restante que no haya alcanzado el tamaño de 'batch_size'
    if batch_list:
        query = """
        UNWIND $batch_list AS row
        MERGE (b:Book {title: row.title})
        SET b.description = row.description,
            b.image = row.image
        """
        tx.run(query, batch_list=batch_list)

def create_publisher_nodes(tx: ManagedTransaction, df: pd.DataFrame):
    '''
    Función para crear nodos de editoriales en Neo4j a partir de un DataFrame.
    '''
    print("Creando nodos de editoriales...")
    total_rows = len(df)
    batch_list: list[str] = []

    for idx, row in df.iterrows():  # type: ignore
        publisher = str(row['publisher'])  # type: ignore

        batch_list.append(publisher)

        if (idx + 1) % BATCH_SIZE == 0 or (idx + 1) == total_rows:  # type: ignore
            query = """
            UNWIND $batch_list AS publisher
            MERGE (p:Publisher {name: publisher})
            """
            tx.run(query, batch_list=batch_list)
            print(f"Editoriales creadas para filas {idx + 1 - len(batch_list) + 1} a {idx + 1}.")
            batch_list = []

    print("Editoriales creadas.")

def create_books_authors_relations(tx: ManagedTransaction, df: pd.DataFrame) -> None:
    """
    Función para crear relaciones entre libros y autores en Neo4j.
    """
    print("Creando relaciones entre libros y autores...")
    total_rows = len(df)
    batch_list: list[dict[str, str]] = []

    for idx, row in df.iterrows():  # type: ignore
        title = row['Title']  # type: ignore
        authors_str = row['authors']  # type: ignore

        if pd.notna(authors_str):  # type: ignore
            try:
                authors = ast.literal_eval(authors_str)  # type: ignore
                if isinstance(authors, list):
                    for author in authors:  # type: ignore
                        batch_list.append({
                            "title": title,
                            "author": author
                        })
            except (ValueError, SyntaxError):
                continue

        if (idx + 1) % BATCH_SIZE == 0 or (idx + 1) == total_rows:  # type: ignore
            if batch_list:
                query = """
                UNWIND $batch_list AS row
                MATCH (b:Book {title: row.title})
                MATCH (a:Author {name: row.author})
                MERGE (b)-[:WRITTEN_BY]->(a)
                """
                tx.run(query, batch_list=batch_list)
                print(f"Relaciones creadas para filas {idx + 1 - len(batch_list) + 1} a {idx + 1}.")
                batch_list = []

    print("Relaciones entre libros y autores creadas.")

def create_books_categories_relations(tx: ManagedTransaction, df: pd.DataFrame) -> None:
    """
    Función para crear relaciones entre libros y categorías en Neo4j.
    """
    print("Creando relaciones entre libros y categorías...")
    total_rows = len(df)
    batch_list: list[dict[str, str]] = []

    for idx, row in df.iterrows():  # type: ignore
        title = row['Title']  # type: ignore
        categories_str = row['categories']  # type: ignore

        if pd.notna(categories_str):  # type: ignore
            try:
                categories = ast.literal_eval(categories_str)  # type: ignore
                if isinstance(categories, list):
                    for category in categories:  # type: ignore
                        batch_list.append({
                            "title": title,
                            "category": category
                        })
            except (ValueError, SyntaxError):
                continue

        if (idx + 1) % BATCH_SIZE == 0 or (idx + 1) == total_rows:  # type: ignore
            if batch_list:
                query = """
                UNWIND $batch_list AS row
                MATCH (b:Book {title: row.title})
                MATCH (c:Category {name: row.category})
                MERGE (b)-[:BELONGS_TO]->(c)
                """
                tx.run(query, batch_list=batch_list)
                print(f"Relaciones creadas para filas {idx + 1 - len(batch_list) + 1} a {idx + 1}.")
                batch_list = []

    print("Relaciones entre libros y categorías creadas.")

def create_books_publishers_relations(tx: ManagedTransaction, df: pd.DataFrame) -> None:
    """
    Función para crear relaciones entre libros y editoriales en Neo4j.
    """
    print("Creando relaciones entre libros y editoriales...")
    total_rows = len(df)
    batch_list: list[dict[str, str]] = []

    for idx, row in df.iterrows():  # type: ignore
        title = row['Title']  # type: ignore
        publisher = row['publisher']  # type: ignore
        publish_date = row['publish_date']  # type: ignore

        batch_list.append({
            "title": title,
            "publisher": publisher,
            "publish_date": publish_date
        })

        if (idx + 1) % BATCH_SIZE == 0 or (idx + 1) == total_rows:  # type: ignore
            if batch_list:
                query = """
                UNWIND $batch_list AS row
                MATCH (b:Book {title: row.title})
                MATCH (p:Publisher {name: row.publisher})
                MERGE (b)-[:PUBLISHED_BY {date: row.publish_date}]->(p)
                """
                tx.run(query, batch_list=batch_list)
                print(f"Relaciones creadas para filas {idx + 1 - len(batch_list) + 1} a {idx + 1}.")
                batch_list = []

    print("Relaciones entre libros y editoriales creadas.")

def main():
    df: pd.DataFrame = pd.read_csv(BOOKS_PATH)  # Cargar el dataset

    # Llamada paralela a las funciones utilizando el parallel_executor
    parallel_executor(execute_in_new_session, create_book_nodes, df)
    parallel_executor(execute_in_new_session, create_nodes_from_col_list, df, 'authors', 'Author', 'Autores')
    parallel_executor(execute_in_new_session, create_nodes_from_col_list, df, 'categories', 'Category', 'Categorías')
    parallel_executor(execute_in_new_session, create_publisher_nodes, df)
    parallel_executor(execute_in_new_session, create_books_authors_relations, df)
    parallel_executor(execute_in_new_session, create_books_categories_relations, df)
    parallel_executor(execute_in_new_session, create_books_publishers_relations, df)

if __name__ == "__main__":
    main()
