from neo4j import ManagedTransaction
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
        self.df = self.df.dropna(subset=["Title"])
        self.batch_size = batch_size
        self.driver = db.connect()
        self._ensure_index()

    def _ensure_index(self):
        '''Crea un índice en Neo4j en el campo Title si no existe, para optimizar las consultas MERGE.'''
        with self.driver.session() as session:
            session.run("CREATE INDEX IF NOT EXISTS FOR (b:Book) ON (b.Title)")

    def close(self):
        self.driver.close()

    def create_book_nodes(self):
        '''Carga nodos de libros en Neo4j en paralelo.'''
        total_rows = len(self.df)
        print(f"Total de libros a cargar: {total_rows}")
        futures = []
        node_label = "Book"
        properties = {
            "Title": "Title",
            "description": "description",
            "image": "image"
        }

        # Proceso de batch concurrente con transacciones únicas para múltiples lotes
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for i in tqdm(range(0, total_rows, self.batch_size), desc="Cargando libros"):
                batch_df = self.df.iloc[i:i + self.batch_size]
                futures.append(executor.submit(self._process_batch, batch_df, node_label, properties))

            for future in as_completed(futures):
                future.result()

    def _process_batch(self, batch_df: pd.DataFrame, node_label: str, properties: Dict[str, str]):
        '''Procesa un conjunto de lotes dentro de una única transacción para optimizar el rendimiento.'''
        with self.driver.session() as session:
            for i in range(0, len(batch_df), self.batch_size):
                small_batch_df = batch_df.iloc[i:i + self.batch_size]
                session.write_transaction(self._create_nodes_in_tx, small_batch_df, node_label, properties)

    def _create_nodes_in_tx(self, tx: ManagedTransaction, batch_df: pd.DataFrame, node_label: str, properties: Dict[str, str]):
        '''Inserta un lote de nodos en una transacción optimizada.'''
        batch_list: list[Dict[str, Any]] = []
        for row in batch_df.itertuples(index=False):
            node_data = {neo4j_prop: getattr(row, df_col, None) for df_col, neo4j_prop in properties.items()}
            node_data = {k: v for k, v in node_data.items() if pd.notna(v)}
            if node_data:
                batch_list.append(node_data)

        primary_property = "Title"  # Propiedad clave única
        set_clauses = ", ".join([f"{node_label.lower()}.{prop} = row.{prop}" for prop in properties.values() if prop != primary_property])

        query = f"""
        UNWIND $batch_list AS row
        MERGE ({node_label.lower()}:{node_label} {{ {primary_property}: row.{primary_property} }})
        {"SET " + set_clauses if set_clauses else ""}
        """
        
        try:
            tx.run(query, batch_list=batch_list)
            print(f"Batch insert successful. Inserted {len(batch_list)} nodes.")
        except Exception as e:
            print(f"Error al procesar el batch: {e}")

def main():
    if BOOKS_PATH is None:
        raise ValueError("BOOKS_PATH environment variable is not set")
    loader = NEO4jDatasetLoader(books_path=BOOKS_PATH, max_workers=NUM_WORKERS, batch_size=BATCH_SIZE)
    try:
        loader.create_book_nodes()
    finally:
        loader.close()

if __name__ == "__main__":
    main()
