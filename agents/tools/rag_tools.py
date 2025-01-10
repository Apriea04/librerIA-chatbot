from utils.db import connect
from models.embedding_manager import EmbeddingManager

neo4j_conn = connect()


def recommendSimilarBooks(
    input_text: str,
    top_k: int = 5,
    title_embedding_property: str = "title_embedding",
    description_embedding_property: str = "description_embedding",
) -> list:
    """
    Recommends similar books based on a given title or description.
    If the title exists in the database, it uses the book's description for recommendations.
    If the title does not exist or the book has no description, it interprets the input as a description
    and finds books with the most similar embeddings based on title or description embeddings.

    Args:
        input_text (str): The title or description of the book.
        top_k (int, optional): The number of similar books to return. Defaults to 5.
        title_embedding_property (str, optional): The property name for title embeddings.
        description_embedding_property (str, optional): The property name for description embeddings.

    Returns:
        list: A list of tuples, where each tuple contains the title of a similar book and the similarity score.
    """
    try:
        with neo4j_conn.session() as session:
            # Check if the input matches an existing book title
            query = """
            MATCH (b:Book {title: $title})
            RETURN b.description AS description
            """
            result = session.run(query, {"title": input_text}).single()  # type: ignore
            book_description = result["description"] if result else None

            if book_description:
                # If the book exists and has a description, generate embedding for the description
                embedding = EmbeddingManager().generate_text_embedding(
                    [book_description]
                )[0]
                embedding_property = description_embedding_property
            else:
                # If the book doesn't exist or has no description, generate embedding from the input text
                embedding = EmbeddingManager().generate_text_embedding([input_text])[0]
                embedding_property = (
                    title_embedding_property  # Use title embedding for input
                )

            # Query for similar books
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
            similar_books = session.run(
                similar_books_query, {"embedding": embedding, "top_k": top_k}
            )  # type: ignore

            return [(record["title"], record["similarity"]) for record in similar_books]
    finally:
        neo4j_conn.close()


def recommendSameGenreAs(
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


def recommendSameAuthorAs(
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


def getBookDescription(book: str) -> str:
    """
    Get the description of the specified book.

    Parameters:
        book (str): The title of the book.

    Returns:
        str: The description of the book or a message if not found.
    """
    try:
        with neo4j_conn.session() as session:
            query = """
            MATCH (b:Book {title: $book})
            RETURN b.description AS description
            """
            result = session.run(query, {"book": book}).single()
            if result is None:
                return "Book not found"
            return result["description"]
    except Exception as e:
        return f"An error occurred: {str(e)}"
    finally:
        neo4j_conn.close()


def getBooksInfo(books: list[str]) -> dict[str, dict]:
    """
    Get the information of the specified books. This includes the author, genre, description,
    published date, and image URL.
    """
    try:
        with neo4j_conn.session() as session:
            query = """
                MATCH (b:Book)-[:WRITTEN_BY]->(a:Author),
                    (b)-[:BELONGS_TO]->(g:Genre)
                WHERE b.title IN $books
                RETURN b.title AS title, b AS book, b.description AS description, a.name AS author, g.name AS genre, b.publishedDate AS published, b.image AS imageUrl
            """
            result = session.run(query, {"books": books})  # type: ignore
            return {
                record["title"]: {
                    "author": record["author"],
                    "genre": record["genre"],
                    "description": record["description"],
                    "published": record["book"]["published"],
                    "imageUrl": record["imageUrl"],
                }
                for record in result
            }
    finally:
        neo4j_conn.close()
        
def getBookAuthor(book: str) -> str:
    """
    Get the author of the specified book.

    Parameters:
        book (str): The title of the book.

    Returns:
        str: The author of the book or a message if not found.
    """
    try:
        with neo4j_conn.session() as session:
            query = """
            MATCH (b:Book {title: $book})-[:WRITTEN_BY]->(a:Author)
            RETURN a.name AS author
            """
            result = session.run(query, {"book": book}).single()
            if result is None:
                return "Book not found"
            return result["author"]
    except Exception as e:
        return f"An error occurred: {str(e)}"
    finally:
        neo4j_conn.close()
        
def getBookGenre(book: str) -> str:
    """
    Get the genre of the specified book.

    Parameters:
        book (str): The title of the book.

    Returns:
        str: The genre of the book or a message if not found.
    """
    try:
        with neo4j_conn.session() as session:
            query = """
            MATCH (b:Book {title: $book})-[:BELONGS_TO]->(g:Genre)
            RETURN g.name AS genre
            """
            result = session.run(query, {"book": book}).single()
            if result is None:
                return "Book not found"
            return result["genre"]
    except Exception as e:
        return f"An error occurred: {str(e)}"
    finally:
        neo4j_conn.close()
        
def getBookPublisher(book: str) -> str:
    """
    Get the publisher of the specified book.

    Parameters:
        book (str): The title of the book.

    Returns:
        str: The publisher of the book or a message if not found.
    """
    try:
        with neo4j_conn.session() as session:
            query = """
            MATCH (b:Book {title: $book})-[:PUBLISHED_BY]->(p:Publisher)
            RETURN p.name AS publisher
            """
            result = session.run(query, {"book": book}).single()
            if result is None:
                return "Book not found"
            return result["publisher"]
    except Exception as e:
        return f"An error occurred: {str(e)}"
    finally:
        neo4j_conn.close()


def getBookReviews(book: str) -> list[str]:
    """
    Get the reviews of the specified book.

    Parameters:
        book (str): The title of the book.

    Returns:
        list[str]: The reviews of the book or a message if not found.
    """
    try:
        with neo4j_conn.session() as session:
            query = """
            MATCH (r:Review)-[:REVIEWS]->(b:Book {title: $book})
            RETURN r.text AS review
            """
            result = session.run(query, {"book": book})  # type: ignore
            return [record["review"] for record in result]
    except Exception as e:
        return [f"An error occurred: {str(e)}"]
    finally:
        neo4j_conn.close()


def recommendBooksByReviews(review: str, k: int = 5) -> list:
    """
    Recommends books based on the specified text, which is used to find similar reviews.
    This method uses the Neo4j graph database to find books that have similar embeddings
    to the specified review's embedding, which is generated using the EmbeddingManager.
    Args:
        review (str): The review for which to find similar books.
        k (int, optional): The number of similar reviews to find. Defaults to 5.
    Returns:
        list: A list of tuples, where each tuple contains the title of a similar book and
              the similarity score.
    """
    try:
        review_embedding = EmbeddingManager().generate_text_embedding([review])[0]

        with neo4j_conn.session() as session:
            # Consulta para encontrar los libros más similares
            similar_books_query = f"""
            MATCH (r:Review)-[:REVIEWS]->(b:Book)
            WITH b,
                 CASE 
                     WHEN r.text_embedding IS NOT NULL THEN gds.similarity.cosine(r.text_embedding, $embedding)
                     ELSE -1 
                 END AS similarity
            RETURN b.title AS title, similarity
            ORDER BY similarity DESC
            LIMIT $k
            """
            similar_books = session.run(similar_books_query, {"embedding": review_embedding, "k": k})  # type: ignore

            return [(record["title"], record["similarity"]) for record in similar_books]
    finally:
        neo4j_conn.close()


def getBooksFromAuthor(author: str, k: int|None = None) -> list:
    """
    Get the books written by the specified author.

    Parameters:
        author (str): The name of the author.
        k (int, optional): The maximum number of books to return. Defaults to None.

    Returns:
        list: A list of books written by the author or a message if not found.
    """
    try:
        with neo4j_conn.session() as session:
            query = """
            MATCH (b:Book)-[:WRITTEN_BY]->(a:Author {name: $author})
            RETURN b.title AS title
            """
            if k is not None:
                query += " LIMIT $k"
            result = session.run(query, {"author": author, "k": k})  # type: ignore
            return [record["title"] for record in result]
    except Exception as e:
        return [f"An error occurred: {str(e)}"]
    finally:
        neo4j_conn.close()
        
# TODO: author and taking into account reviews.
