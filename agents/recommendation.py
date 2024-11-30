from utils.book_queries import BookQueries

class RecommendationAgent:
    def __init__(self):
        self.book_queries = BookQueries()

    def close(self):
        self.book_queries.close()

    def find_similar_books_by_embedding(self, embedding, top_n=5):
        """
        Encuentra libros similares en la base de datos dado un embedding.
        
        :param embedding: El embedding del libro.
        :param top_n: Número de libros similares a retornar.
        :return: Lista de libros similares.
        """
        similar_books = self.book_queries.find_similar_book_by_embedding(embedding, top_n)
        return similar_books

    def find_similar_books_by_node(self, node, top_n=5):
        """
        Encuentra libros similares en la base de datos dado un nodo en el grafo.
        
        :param node: El nodo del grafo (libro, reseña, autor o género).
        :param top_n: Número de libros similares a retornar.
        :return: Lista de libros similares.
        """
        similar_books = self.book_queries.find_similar_book_by_node(node, top_n)
        return similar_books

    def find_similar_books(self, embedding_or_node, top_n=5):
        """
        Encuentra libros similares en la base de datos dado un embedding o nodo en el grafo.
        
        :param embedding_or_node: El embedding o nodo del grafo (libro, reseña, autor o género).
        :param top_n: Número de libros similares a retornar.
        :return: Lista de libros similares.
        """
        if isinstance(embedding_or_node, str):
            return self.find_similar_books_by_node(embedding_or_node, top_n)
        else:
            return self.find_similar_books_by_embedding(embedding_or_node, top_n)

    def find_books_by_author(self, author, top_n=5):
        """
        Encuentra libros en la base de datos que pertenezcan a un autor específico.
        
        :param author: El nombre del autor.
        :param top_n: Número de libros a retornar.
        :return: Lista de libros del autor.
        """
        books_by_author = self.book_queries.find_books_by_author(author, top_n)
        return books_by_author

    def find_books_by_genre(self, genre, top_n=5):
        """
        Encuentra libros en la base de datos que pertenezcan a un género específico.
        
        :param genre: El nombre del género.
        :param top_n: Número de libros a retornar.
        :return: Lista de libros del género.
        """
        books_by_genre = self.book_queries.find_books_by_genre(genre, top_n)
        return books_by_genre

    def find_books_by_author_and_genre(self, author, genre, top_n=5):
        """
        Encuentra libros en la base de datos que pertenezcan a un autor y género específicos.
        
        :param author: El nombre del autor.
        :param genre: El nombre del género.
        :param top_n: Número de libros a retornar.
        :return: Lista de libros del autor y género.
        """
        books_by_author_and_genre = self.book_queries.find_books_by_author_and_genre(author, genre, top_n)
        return books_by_author_and_genre