import os
from dotenv import load_dotenv

class EnvLoader:
    _instance = None
    books_path = ""
    ratings_path = ""
    neo4j_uri = ""
    neo4j_user = ""
    neo4j_password = ""
    batch_size = ""
    embeddings_model = ""

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(EnvLoader, cls).__new__(cls)
            cls._instance.load_env_vars()
            cls.books_path = cls._instance.get_env_var('ALL_BOOKS_PATH')
            cls.ratings_path = cls._instance.get_env_var('ALL_RATINGS_PATH')
            cls.neo4j_uri = cls._instance.get_env_var("NEO4J_URI")
            cls.neo4j_user = cls._instance.get_env_var("NEO4J_USERNAME")
            cls.neo4j_password = cls._instance.get_env_var("NEO4J_PASSWORD")
            cls.batch_size = int(cls._instance.get_env_var("BATCH_SIZE", "100"))
            cls.embeddings_model = cls._instance.get_env_var("EMBEDDINGS_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
        return cls._instance

    @staticmethod
    def load_env_vars():
        load_dotenv(override=True)

    @staticmethod
    def get_env_var(var_name, default=None):
        value = os.getenv(var_name, default)
        if value is None:
            raise ValueError(f"Environment variable {var_name} is not set")
        return value