from utils.db import connect

class RAGAgent:
    """
    A class used to represent a Retrieval-Augmented Generation (RAG) Agent.
    Methods
    -------
    __init__():
        Initializes the RAGAgent with a connection to a Neo4j database.
    recommend_similar_books(book_title, top_k=5):
        Recommends similar books based on the embedding of the given book title.
        Parameters:
        book_title (str): The title of the book for which to find similar books.
        top_k (int): The number of similar books to return. Default is 5.
        Returns:
        list of tuples: A list of tuples where each tuple contains the title of a similar book and its similarity score.
    """
    def __init__(self):
        self.neo4j_conn = connect()

    def recommend_similar_books(self, book_title, top_k=5):
        """
        Recommends similar books based on the embedding of the given book title.
        This function queries a Neo4j database to find the embedding of the specified book title.
        It then uses the cosine similarity of embeddings to find and return the top K most similar books.
        Args:
            book_title (str): The title of the book for which to find similar books.
            top_k (int, optional): The number of similar books to return. Defaults to 5.
        Returns:
            list of tuple: A list of tuples where each tuple contains the title of a similar book and its similarity score.
        """
        with self.neo4j_conn.session() as session:
            query = f"""
            MATCH (b:Book) WHERE b.title = $title
            RETURN b.embedding AS embedding
            """
            book_embedding = session.run(query, {"title": book_title}).single()['embedding'] # type: ignore

            knn_query = f"""
            MATCH (b:Book)
            WHERE b.embedding IS NOT NULL
            WITH b, gds.alpha.similarity.cosine(b.embedding, $embedding) AS similarity
            RETURN b.title AS title, similarity
            ORDER BY similarity DESC
            LIMIT $top_k
            """
            similar_books = session.run(knn_query, {"embedding": book_embedding, "top_k": top_k})

            return [(record['title'], record['similarity']) for record in similar_books]
