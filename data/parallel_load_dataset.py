import ast
import neo4j
import pandas as pd
from utils import db
from typing import Any, Dict
import os
import dotenv
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

dotenv.load_dotenv(override=True)
BOOKS_PATH = os.getenv("BOOKS_PATH")
RATINGS_PATH = os.getenv("RATINGS_PATH")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10000))  # Ajuste por defecto
NUM_WORKERS = int(os.getenv("NUM_WORKERS", 4))

class NEO4jDatasetLoader:
    def __init__(self, books_path: str, max_workers: int = 4, batch_size: int = 10000):
        self.max_workers = max_workers
        self.books_path = books_path
        self.df = pd.read_csv(books_path)  # type: ignore
        # Filtrar libros sin 'Title' o 'description'
        self.df = self.df.dropna(subset=["Title"])  # type: ignore
        self.batch_size = batch_size
        self.driver = db.connect()
        self._ensure_indexes()

    def _ensure_indexes(self):
        '''Crea índices en Neo4j para los campos clave en los nodos Book y Author.'''
        with self.driver.session() as session:
            session.run("CREATE INDEX IF NOT EXISTS FOR (b:Book) ON (b.name)")
            session.run("CREATE INDEX IF NOT EXISTS FOR (a:Author) ON (a.name)")

    def close(self):
        self.driver.close()

    def create_nodes(self, df: pd.DataFrame, node_label: str, properties: Dict[str, str], primary_property: str):
        '''Carga nodos en Neo4j en paralelo para el tipo de nodo especificado.'''
        total_rows = len(df)
        print(f"Total de {node_label}s a cargar: {total_rows}")
        futures = []

        # Proceso de batch concurrente con transacciones únicas para múltiples lotes
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for i in tqdm(range(0, total_rows, self.batch_size), desc=f"Cargando {node_label}s"):
                batch_df = df.iloc[i:i + self.batch_size]
                futures.append(executor.submit(self._process_batch, batch_df, node_label, properties, primary_property)) # type: ignore

            for future in as_completed(futures): # type: ignore
                future.result()

    def _process_batch(self, batch_df: pd.DataFrame, node_label: str, properties: Dict[str, str], primary_property: str):
        '''Procesa un conjunto de lotes dentro de una única transacción para optimizar el rendimiento.'''
        with self.driver.session() as session:
            for i in range(0, len(batch_df), self.batch_size):
                small_batch_df = batch_df.iloc[i:i + self.batch_size]
                session.write_transaction(self._create_nodes_in_tx, small_batch_df, node_label, properties, primary_property)

    def _create_nodes_in_tx(self, tx: neo4j.ManagedTransaction, batch_df: pd.DataFrame, node_label: str, properties: Dict[str, str], primary_property: str):
        '''Inserta un lote de nodos en una transacción optimizada para el tipo de nodo especificado.'''
        batch_list: list[Dict[str, Any]] = []
        for row in batch_df.itertuples(index=False):
            node_data = {neo4j_prop: getattr(row, df_col, None) for df_col, neo4j_prop in properties.items()}
            node_data = {k: v for k, v in node_data.items() if pd.notna(v)}
            if node_data:
                batch_list.append(node_data)

        query = self._build_merge_query(node_label, properties, primary_property)
        
        try:
            tx.run(query, batch_list=batch_list) # type: ignore
            print(f"Batch insert successful. Inserted {len(batch_list)} {node_label} nodes.")
        except Exception as e:
            print(f"Error al procesar el batch de {node_label}: {e}")

    def _build_merge_query(self, node_label: str, properties: Dict[str, str], primary_property: str) -> str:
        '''Construye una consulta MERGE dinámica para el tipo de nodo y propiedades especificadas.'''
        set_clauses = ", ".join([f"{node_label.lower()}.{prop} = row.{prop}" for prop in properties.values() if prop != primary_property])

        query = f"""
        UNWIND $batch_list AS row
        MERGE ({node_label.lower()}:{node_label} {{ {primary_property}: row.{primary_property} }})
        {"SET " + set_clauses if set_clauses else ""}
        """
        return query

    def create_book_nodes(self):
        '''Carga nodos de libros en Neo4j utilizando la función genérica create_nodes.'''
        properties = {
            "Title": "name",
            "description": "description",
            "image": "image",
            "infoLink": "infoLink",
        }
        primary_property = "name"
        self.create_nodes(self.df, "Book", properties, primary_property)

    def create_author_nodes(self):
        '''Crea nodos de autores únicos en Neo4j evitando duplicados.'''
        # Extraer y limpiar autores únicos en memoria
        unique_authors: set[str] = set()
        for authors in self.df['authors'].dropna().astype(str):
            authors_list = ast.literal_eval(authors)
            unique_authors.update(authors_list)  # Añade cada autor en la lista al conjunto de autores únicos

        # Crear DataFrame temporal para los autores únicos
        authors_df = pd.DataFrame({"name": list(unique_authors)})

        # Definir propiedades y propiedad principal
        properties = {"name": "name"}
        primary_property = "name"

        # Cargar nodos de autores usando create_nodes
        self.create_nodes(authors_df, "Author", properties, primary_property)
        
    def create_category_nodes(self):
        '''Crea nodos de categorías únicas en Neo4j evitando duplicados.'''
        # Extraer y limpiar categorías únicas en memoria
        unique_authors: set[str] = set()
        for authors in self.df['categories'].dropna().astype(str):
            authors_list = ast.literal_eval(authors)
            unique_authors.update(authors_list)  # Añade cada autor en la lista al conjunto de autores únicos

        # Crear DataFrame temporal para los autores únicos
        authors_df = pd.DataFrame({"name": list(unique_authors)})

        # Definir propiedades y propiedad principal
        properties = {"name": "name"}
        primary_property = "name"

        # Cargar nodos de autores usando create_nodes
        self.create_nodes(authors_df, "Category", properties, primary_property)

    def p(self):
        pass
def main():
    if BOOKS_PATH is None:
        raise ValueError("BOOKS_PATH environment variable is not set")
    loader = NEO4jDatasetLoader(books_path=BOOKS_PATH, max_workers=NUM_WORKERS, batch_size=BATCH_SIZE)
    try:
        loader.create_book_nodes()
        loader.create_author_nodes()
        loader.create_category_nodes()
    finally:
        loader.close()

if __name__ == "__main__":
    main()
