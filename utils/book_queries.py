from utils.db import connect
from neo4j.exceptions import ServiceUnavailable

class BookQueries:
    def __init__(self):
        self.driver = connect()

    def close(self):
        self.driver.close()

    def find_similar_book_by_embedding(self, embedding, top_n=5):
        try:
            with self.driver.session() as session:
                result = session.run(
                    "MATCH (b:Book) WHERE b.embedding = $embedding "
                    "RETURN b ORDER BY b.similarity DESC LIMIT $top_n",
                    embedding=embedding, top_n=top_n
                )
                return [record["b"] for record in result]
        except ServiceUnavailable as e:
            print(f"Error finding similar books by embedding: {e}")
            return []

    def find_similar_book_by_node(self, node, top_n=5):
        try:
            with self.driver.session() as session:
                result = session.run(
                    "MATCH (b:Book)-[:SIMILAR_TO]->(n) WHERE n.id = $node "
                    "RETURN b ORDER BY b.similarity DESC LIMIT $top_n",
                    node=node, top_n=top_n
                )
                return [record["b"] for record in result]
        except ServiceUnavailable as e:
            print(f"Error finding similar books by node: {e}")
            return []

    def find_books_by_author(self, author, top_n=5):
        try:
            with self.driver.session() as session:
                result = session.run(
                    "MATCH (b:Book)-[:WRITTEN_BY]->(a:Author) WHERE a.name = $author "
                    "RETURN b LIMIT $top_n",
                    author=author, top_n=top_n
                )
                return [record["b"] for record in result]
        except ServiceUnavailable as e:
            print(f"Error finding books by author: {e}")
            return []

    def find_books_by_genre(self, genre, top_n=5):
        try:
            with self.driver.session() as session:
                result = session.run(
                    "MATCH (b:Book)-[:BELONGS_TO]->(g:Genre) WHERE g.name = $genre "
                    "RETURN b LIMIT $top_n",
                    genre=genre, top_n=top_n
                )
                return [record["b"] for record in result]
        except ServiceUnavailable as e:
            print(f"Error finding books by genre: {e}")
            return []

    def find_books_by_author_and_genre(self, author, genre, top_n=5):
        try:
            with self.driver.session() as session:
                result = session.run(
                    "MATCH (b:Book)-[:WRITTEN_BY]->(a:Author), (b)-[:BELONGS_TO]->(g:Genre) "
                    "WHERE a.name = $author AND g.name = $genre "
                    "RETURN b LIMIT $top_n",
                    author=author, genre=genre, top_n=top_n
                )
                return [record["b"] for record in result]
        except ServiceUnavailable as e:
            print(f"Error finding books by author and genre: {e}")
            return []