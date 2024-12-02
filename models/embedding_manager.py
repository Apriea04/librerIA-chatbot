from transformers import AutoModel, AutoTokenizer
import torch

class EmbeddingManager:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(EmbeddingManager, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            self.graph = None
            self.tokenizer = None
            self.model = None
            self.initialized = True

    def load_tokenizer(self, model_name: str):
        if self.tokenizer is not None:
            del self.tokenizer
        if self.model is not None:
            del self.model
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name).to(self.device)

    def generate_text_embedding(self, texts: list):
        inputs = self.tokenizer(
            texts, return_tensors="pt", padding=True, truncation=True
        ) # type: ignore
        inputs = {key: val.to(self.device) for key, val in inputs.items()}
        with torch.no_grad():
            outputs = self.model(**inputs) # type: ignore
            embeddings = outputs.last_hidden_state.mean(dim=1)
        return embeddings.cpu().numpy().tolist()