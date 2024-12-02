import time
from utils.db_manager import DBManager
from utils.env_loader import EnvLoader
from agents.rag_agent import RAGAgent

start_time = time.time()

env = EnvLoader()
# Se debe cargar el dataset manualmente.
dbManager = DBManager()

# Generate embeddings for the dataset:
dbManager.generate_embeddings_for("Book", "title", "title", env.embeddings_model, 32)
#dbManager.generate_embeddings_for("Book", "description", "title", env.embeddings_model, 32)
#dbManager.generate_embeddings_for("Review", "summary", "", env.embeddings_model, 32)
#dbManager.generate_embeddings_for("Review", "text", "", env.embeddings_model, 1)

ragAgent = RAGAgent()
results = ragAgent.recommend_similar_books_by_title("Santa Biblia", 10)
for idx, result in enumerate(results, start=1):
    print(f"{idx}. {result[0]} (Score: {result[1]:.8f})")


end_time = time.time()
elapsed_time = end_time - start_time
minutes, seconds = divmod(elapsed_time, 60)
print(f"Execution time: {int(minutes)} minutes and {seconds:.2f} seconds")
