import time
from utils.db_manager import DBManager

start_time = time.time()

# Se debe cargar el dataset manualmente.
dbManager = DBManager()

# Generate embeddings for the dataset:
#dbManager.generate_embeddings_for("Book", "title", "title", "sentence-transformers/all-MiniLM-L6-v2", 32)
#dbManager.generate_embeddings_for("Book", "description", "title", "sentence-transformers/all-MiniLM-L6-v2", 32)
#dbManager.generate_embeddings_for("Review", "summary", "", "sentence-transformers/all-MiniLM-L6-v2", 32)
#dbManager.generate_embeddings_for("Review", "text", "", "sentence-transformers/all-MiniLM-L6-v2", 1)

end_time = time.time()
elapsed_time = end_time - start_time
minutes, seconds = divmod(elapsed_time, 60)
print(f"Execution time: {int(minutes)} minutes and {seconds:.2f} seconds")
