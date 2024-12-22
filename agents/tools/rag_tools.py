from utils.db import connect
from models.embedding_manager import EmbeddingManager
from langchain_core.tools import tool

neo4j_conn = connect()


@tool
def recommend_similar_books_by_title(
    book_title: str,
    top_k: int = 5,
    embedding_property: str = "title_embedding",
) -> list:
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
    try:
        with neo4j_conn.session() as session:
            # Obtener el embedding del libro especificado
            query = f"""
            MATCH (b:Book {{title: $title}})
            RETURN b.{embedding_property} AS embedding
            """
            result = session.run(query, {"title": book_title}).single()  # type: ignore
            book_embedding = result["embedding"] if result else None

            if book_embedding is None:
                book_embedding = EmbeddingManager().generate_text_embedding(
                    [book_title]
                )[0]

            # Consulta para encontrar los libros más similares
            similar_books_query = f"""
            MATCH (b:Book)
            WITH b, 
                 CASE 
                     WHEN b.{embedding_property} IS NOT NULL THEN gds.similarity.cosine(b.{embedding_property}, $embedding)
                     ELSE -1 
                 END AS similarity
            WHERE b.title <> $title
            RETURN b.title AS title, similarity
            ORDER BY similarity DESC
            LIMIT $top_k
            """
            similar_books = session.run(similar_books_query, {"embedding": book_embedding, "top_k": top_k, "title": book_title})  # type: ignore

            return [(record["title"], record["similarity"]) for record in similar_books]
    finally:
        neo4j_conn.close()


@tool
def recommend_similar_books_by_description(
    book_description: str,
    top_k: int = 5,
    embedding_property: str = "description_embedding",
) -> list:
    """
    Recommends similar books based on the given description.
    This method uses the Neo4j graph database to find books that have similar embeddings
    to the specified description's embedding, which is generated using the EmbeddingManager.
    Args:
        book_description (str): The description of the book for which to find similar books.
        top_k (int, optional): The number of similar books to return. Defaults to 5.
        embedding_property (str, optional): The property name of the book node that contains
                                            the embedding. Defaults to "description_embedding".
    Returns:
        list: A list of tuples, where each tuple contains the title of a similar book and
              the similarity score.
    """
    try:
        descr_embedding = EmbeddingManager().generate_text_embedding(
            [book_description]
        )[0]

        with neo4j_conn.session() as session:
            # Consulta para encontrar los libros más similares
            similar_books_query = f"""
            MATCH (b:Book)
            WITH b, 
                 CASE 
                     WHEN b.{embedding_property} IS NOT NULL THEN gds.similarity.cosine(b.{embedding_property}, $embedding)
                     ELSE -1 
                 END AS similarity
            RETURN b.title AS title, similarity
            ORDER BY similarity DESC
            LIMIT $top_k
            """
            similar_books = session.run(similar_books_query, {"embedding": descr_embedding, "top_k": top_k})  # type: ignore

            return [(record["title"], record["similarity"]) for record in similar_books]
    finally:
        neo4j_conn.close()


@tool
def recommend_same_genre_as(
    book_title: str,
    top_k: int = 5,
    description_embedding_property: str = "description_embedding",
) -> list:
    """
    Recommends books of the same genre as the specified book title.
    If the book has a description embedding, it uses that to find similar books belonging to the same genre.
    Otherwise, it just finds books with the same genre.
    Args:
        book_title (str): The title of the book for which to find similar genre books.
        top_k (int, optional): The number of similar genre books to return. Defaults to 5.
        description_embedding_property (str, optional): The property name of the book node that contains
                                                        the description embedding. Defaults to "description_embedding".
    Returns:
        list: A list of tuples, where each tuple contains the title of a similar genre book and
              the similarity score.
    """
    try:
        book_query = f"""
        MATCH (b:Book {{title: $title}})-[:BELONGS_TO]->(g:Genre)
        RETURN g.name AS genre, b.{description_embedding_property} AS embedding
        """
        with neo4j_conn.session() as session:
            result = session.run(book_query, {"title": book_title}).single()  # type: ignore

            if result is None:
                return []

            genre = result["genre"]

            if result["embedding"] is not None:
                book_embedding = result["embedding"]
                similar_books_query = f"""
                MATCH (b:Book)-[:BELONGS_TO]->(g:Genre {{name: $genre}})
                WITH b, 
                     CASE 
                         WHEN b.{description_embedding_property} IS NOT NULL THEN gds.similarity.cosine(b.{description_embedding_property}, $embedding)
                         ELSE -1 
                     END AS similarity
                WHERE b.title <> $title
                RETURN b.title AS title, similarity
                ORDER BY similarity DESC
                LIMIT $top_k
                """
                similar_books = session.run(similar_books_query, {"embedding": book_embedding, "top_k": top_k, "title": book_title, "genre": genre})  # type: ignore

            else:
                similar_books_query = f"""
                MATCH (b:Book)-[:BELONGS_TO]->(g:Genre {{name: $genre}})
                WHERE b.title <> $title
                RETURN b.title AS title
                LIMIT $top_k
                """
                similar_books = session.run(
                    similar_books_query,
                    {"top_k": top_k, "title": book_title, "genre": genre},
                )

            return [(record["title"], record["similarity"]) for record in similar_books]
    finally:
        neo4j_conn.close()


@tool
def recommend_same_author_as(
    book_title: str,
    top_k: int = 5,
    description_embedding_property: str = "description_embedding",
) -> list:
    """
    Recommends books of the same author as the specified book title.
    If the book has a description embedding, it uses that to find similar books belonging to the same author.
    Otherwise, it just finds books with the same author.
    Args:
        book_title (str): The title of the book for which to find similar author books.
        top_k (int, optional): The number of similar author books to return. Defaults to 5.
        description_embedding_property (str, optional): The property name of the book node that contains
                                                        the description embedding. Defaults to "description_embedding".
    Returns:
        list: A list of tuples, where each tuple contains the title of a similar author book and
              the similarity score.
    """
    try:
        book_query = f"""
        MATCH (b:Book {{title: $title}})-[:WRITTEN_BY]->(a:Author)
        RETURN a.name AS author, b.{description_embedding_property} AS embedding
        """
        with neo4j_conn.session() as session:
            result = session.run(book_query, {"title": book_title}).single()  # type: ignore

            if result is None or result["author"] is None:
                return []

            author = result["author"]

            if result["embedding"] is not None:
                book_embedding = result["embedding"]
                similar_books_query = f"""
                MATCH (b:Book)-[:WRITTEN_BY]->(a:Author {{name: $author}})
                WITH b, 
                     CASE 
                         WHEN b.{description_embedding_property} IS NOT NULL THEN gds.similarity.cosine(b.{description_embedding_property}, $embedding)
                         ELSE -1 
                     END AS similarity
                WHERE b.title <> $title
                RETURN b.title AS title, similarity
                ORDER BY similarity DESC
                LIMIT $top_k
                """
                similar_books = session.run(similar_books_query, {"embedding": book_embedding, "top_k": top_k, "title": book_title, "author": author})  # type: ignore

            else:
                similar_books_query = f"""
                MATCH (b:Book)-[:WRITTEN_BY]->(a:Author {{name: $author}})
                WHERE b.title <> $title
                RETURN b.title AS title
                LIMIT $top_k
                """
                similar_books = session.run(
                    similar_books_query,
                    {"top_k": top_k, "title": book_title, "author": author},
                )

            return [(record["title"], record["similarity"]) for record in similar_books]
    finally:
        neo4j_conn.close()


# TODO: author and taking into account reviews.