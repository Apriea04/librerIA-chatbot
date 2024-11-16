import time
from utils.db_manager import DBManager

start_time = time.time()

# Se debe cargar el dataset manualmente.
dbManager = DBManager()
dbManager.project_graph("full_db_projection")

# No se ejecuta node2vec_write dado que requiere aproximadamente 107 GB de memoria.

dbManager.drop_projection("full_db_projection")

end_time = time.time()
elapsed_time = end_time - start_time
minutes, seconds = divmod(elapsed_time, 60)
print(f"Execution time: {int(minutes)} minutes and {seconds:.2f} seconds")
