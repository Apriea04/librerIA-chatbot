from transformers import AutoModel, AutoTokenizer
import torch

class EmbeddingManager:
    def __init__(self):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.graph = None
        self.tokenizer = None
        self.model = None

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