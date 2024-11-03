from neo4j import ManagedTransaction
import pandas as pd
from utils import db
import os
import dotenv
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

dotenv.load_dotenv(override=True)
BOOKS_PATH = os.getenv("BOOKS_PATH")
RATINGS_PATH = os.getenv("RATINGS_PATH")
BATCH_SIZE = int(os.getenv("BATCH_SIZE"))  # type: ignore
NUM_WORKERS = int(os.getenv("NUM_WORKERS", 4))  # Número de procesos para paralelización

class NEO4jDatasetLoader:
    def __init__(self, books_path: str, max_workers: int = 4, batch_size: int = 10000):
        self.max_workers = max_workers
        self.books_path = books_path
        self.df = pd.read_csv(books_path)  # type: ignore
        self.batch_size = batch_size
        self.driver = db.connect()

    def close(self):
        self.driver.close()

    def create_book_nodes(self):
        '''Método para cargar nodos de libros en Neo4j en paralelo.'''
        total_rows = len(self.df)
        print(f"Total de libros a cargar: {total_rows}")
        futures = []
        # Definir el label y el mapeo de propiedades aquí
        node_label = "Book"
        properties = {
            "book_id": "bookID",
            "title": "title",
            "author": "authors",
            "average_rating": "average_rating",
            "isbn": "isbn"
            # agregar las demás columnas que existan en el DataFrame que quieras mapear
        }

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for i in tqdm(range(0, total_rows, self.batch_size), desc="Cargando libros"):
                batch_df = self.df.iloc[i:i + self.batch_size]
                # Para cada batch, pasar una nueva sesión para evitar conflictos entre hilos
                futures.append(executor.submit(self._process_batch, batch_df, node_label, properties))

            # Asegurarse de que todas las tareas paralelas se completen
            for future in as_completed(futures):
                future.result()

    def _process_batch(self, batch_df, node_label: str, properties: dict):
        '''Método auxiliar para procesar un lote en una transacción de escritura, de forma genérica.'''
        with self.driver.session() as session:
            session.write_transaction(self._create_nodes_in_tx, batch_df, node_label, properties)

    def _create_nodes_in_tx(self, tx: ManagedTransaction, batch_df: pd.DataFrame, node_label: str, properties: dict):
        '''
        Inserta un lote de nodos en una transacción.
        :param node_label: Label del nodo en Neo4j.
        :param properties: Diccionario que mapea las columnas del DataFrame con las propiedades en Neo4j.
        '''
        batch_list = []
        for row in batch_df.itertuples(index=False):
            # Mapea las columnas del DataFrame a las propiedades de Neo4j
            node_data = {neo4j_prop: getattr(row, df_col, None) for df_col, neo4j_prop in properties.items()}
            node_data = {k: v for k, v in node_data.items() if pd.notna(v)}  # Filtra valores nulos

            if node_data:
                batch_list.append(node_data)

        # Construir la consulta Cypher de forma dinámica
        set_clauses = ", ".join([f"{node_label.lower()}.{prop} = coalesce(row.{prop}, {node_label.lower()}.{prop})"
                                 for prop in properties.values()])
        query = f"""
        UNWIND $batch_list AS row
        MERGE ({node_label.lower()}:{node_label} {{ {list(properties.values())[0]}: row.{list(properties.values())[0]} }})
        SET {set_clauses}
        """
        tx.run(query, batch_list=batch_list)

def main():
    loader = NEO4jDatasetLoader(books_path=BOOKS_PATH, max_workers=NUM_WORKERS, batch_size=BATCH_SIZE)
    try:
        loader.create_book_nodes()
    finally:
        loader.close()

if __name__ == "__main__":
    main()
