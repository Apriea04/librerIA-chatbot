import time
from utils.db_manager import DBManager
from agents.recommendation import RecommendationAgent

start_time = time.time()

# Se debe cargar el dataset manualmente.
dbManager = DBManager()

# Generate embeddings for the dataset:
dbManager.generate_embeddings_for("Book", "title", "title", "sentence-transformers/all-MiniLM-L6-v2", 32)
#dbManager.generate_embeddings_for("Book", "description", "title", "sentence-transformers/all-MiniLM-L6-v2", 32)
#dbManager.generate_embeddings_for("Review", "summary", "", "sentence-transformers/all-MiniLM-L6-v2", 32)
#dbManager.generate_embeddings_for("Review", "text", "", "sentence-transformers/all-MiniLM-L6-v2", 1)

# Crear una instancia de RecommendationAgent
recommendation_agent = RecommendationAgent()

# Encontrar libros similares dado un embedding o nodo
similar_books = recommendation_agent.find_similar_books("some_embedding_or_node")
print("Similar books:", similar_books)

# Cerrar la conexión a la base de datos
recommendation_agent.close()

end_time = time.time()
elapsed_time = end_time - start_time
minutes, seconds = divmod(elapsed_time, 60)
print(f"Execution time: {int(minutes)} minutes and {seconds:.2f} seconds")
