import time
from utils.db_manager import DBManager
from utils.env_loader import EnvLoader
from agents import rag_tools
from agents.agent import Agent

start_time = time.time()

env = EnvLoader()
# Se debe cargar el dataset manualmente.
dbManager = DBManager()

# Generate embeddings for the dataset:
# dbManager.generate_embeddings_for("Book", "title", "title", env.embeddings_model, 32)
# dbManager.generate_embeddings_for("Book", "description", "title", env.embeddings_model, 32)
# dbManager.generate_embeddings_for("Review", "summary", "", env.embeddings_model, 32)
# dbManager.generate_embeddings_for("Review", "text", "", env.embeddings_model, 1)
"""
results = rag_tools.recommend_similar_books_by_title("Santa Biblia", 10)
for idx, result in enumerate(results, start=1):
    print(f"{idx}. {result[0]} (Score: {result[1]:.8f})")
    
print('-'*106)
    
results = rag_tools.recommend_similar_books_by_description("a man of God who is busy caring for his quadriplegic wife, severely injured in a serious car accident. In an innocent effort to reach out to a lonely member of his church, Heath finds himself as the man and not the minister when Heath and Julia give their bodies to each other and face God's wrath. Julia is overtaken by a life-threatening illness, the loss of her home and rumors of his wicked affair.", 10) # Expected Whispers of the Wicked Saints
for idx, result in enumerate(results, start=1):
    print(f"{idx}. {result[0]} (Score: {result[1]:.8f})")
    
print('-'*106)

results = rag_tools.recommend_same_genre_as("The Human Zoo", 12) # Expected Animal Behavior genre: 	How Animals Talk: And Other Pleasant Studies of Birds and Beasts, 	Never Cry Wolf,	Cry Wolf, 	Can I Be Good?, Call of the Dolphins, ...
for idx, result in enumerate(results, start=1):
    print(f"{idx}. {result[0]} (Score: {result[1]:.8f})")
    
print('-'*106)
    
results = rag_tools.recommend_same_author_as("Fortinbras", 12) # Expected 'Lee Blessing''s books: A Walk in the Woods: a Play in Two Acts, Fortinbras
for idx, result in enumerate(results, start=1):
    print(f"{idx}. {result[0]} (Score: {result[1]:.8f})")

end_time = time.time()
elapsed_time = end_time - start_time
minutes, seconds = divmod(elapsed_time, 60)
print(f"Execution time: {int(minutes)} minutes and {seconds:.2f} seconds")
"""
import random
from langchain_core.tools import tool
@tool
def weather_forecast(location: str):
    """Weather forecast tool."""
    tmp = random.randint(-10, 35)
    print(f"Weather for {location}: {tmp}")
    return f"{tmp}ºC, sunny"

@tool
def recommend_similar_books_by_title(title: str, top_k: int=5):
    """
    Recommends similar books based on the title of a given book.
    This method uses the Neo4j graph database to find books that have similar embeddings
    to the specified book title.
    Args:
        book_title (str): The title of the book for which to find similar books.
        top_k (int, optional): The number of similar books to return. Defaults to 5.
    Returns:
        list: A list of tuples, where each tuple contains the title of a similar book and
              the similarity score.
    """
    val = str(rag_tools.recommend_similar_books_by_title(title, top_k))
    print(f"Recommend similar books by title: {val}")
    return val

agent = Agent(
    model_name=env.agent_llm_model,
    prompt_path="agents/prompts/REACT_agent.txt",
    tools=[
        weather_forecast,
        recommend_similar_books_by_title,
        #rag_tools.recommend_similar_books_by_description,
        #rag_tools.recommend_same_genre_as,
        #rag_tools.recommend_same_author_as,
    ],
)

while True:
    message = input("You: ")
    if message.lower() == "exit":
        break
    response = agent.send_msg(message)
    print(f"Bot: {response}")
