import os
from dotenv import load_dotenv

class EnvLoader:
    def __init__(self):
        self.load_env_vars()
        self.books_path = self.get_env_var('ALL_BOOKS_PATH')
        self.ratings_path = self.get_env_var('ALL_RATINGS_PATH')
        self.neo4j_uri = self.get_env_var("NEO4J_URI")
        self.neo4j_user = self.get_env_var("NEO4J_USERNAME")
        self.neo4j_password = self.get_env_var("NEO4J_PASSWORD")
        self.batch_size = int(self.get_env_var("BATCH_SIZE", "100"))

    def load_env_vars(self):
        load_dotenv(override=True)

    def get_env_var(self, var_name, default=None):
        value = os.getenv(var_name, default)
        if value is None:
            raise ValueError(f"Environment variable {var_name} is not set")
        return value