from utils.db import connect
from models.embedding_manager import EmbeddingManager

class RAGAgent:
    def __init__(self):
        self.neo4j_conn = connect()

    def recommend_similar_books_by_title(self, book_title: str, top_k: int=5, embedding_property: str="title_embedding") -> list:
        """
        Recommends similar books based on the title of a given book.
        This method uses the Neo4j graph database to find books that have similar embeddings
        to the specified book title. If the embedding for the specified book title is not found,
        it generates a new embedding using the EmbeddingManager.
        Args:
            book_title (str): The title of the book for which to find similar books.
            top_k (int, optional): The number of similar books to return. Defaults to 5.
            embedding_property (str, optional): The property name of the book node that contains
                                                the embedding. Defaults to "title_embedding".
        Returns:
            list: A list of tuples, where each tuple contains the title of a similar book and
                  the similarity score.
        """
    
        with self.neo4j_conn.session() as session:
            # Obtener el embedding del libro especificado
            query = f"""
            MATCH (b:Book {{title: $title}})
            RETURN b.{embedding_property} AS embedding
            """
            result = session.run(query, {"title": book_title}).single() # type: ignore
            book_embedding = result['embedding'] if result else None

            if book_embedding is None:
                book_embedding = EmbeddingManager().generate_text_embedding([book_title])[0]

            # Consulta para encontrar los libros más similares
            similar_books_query = f"""
            MATCH (b:Book)
            WHERE b.{embedding_property} IS NOT NULL
            WITH b, gds.similarity.cosine(b.{embedding_property}, $embedding) AS similarity
            RETURN b.title AS title, similarity
            ORDER BY similarity DESC
            LIMIT $top_k
            """
            similar_books = session.run(similar_books_query, {"embedding": book_embedding, "top_k": top_k}) # type: ignore

            return [(record['title'], record['similarity']) for record in similar_books]
