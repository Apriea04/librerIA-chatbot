from transformers import AutoTokenizer, AutoModel
import torch
import pickle
from tqdm import tqdm

# Configuración del modelo
model_name = "dunzhang/stella_en_1.5B_v5"
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModel.from_pretrained(model_name).to(device)

# Cargar datos desde el archivo pickle
input_file = "Review_summary_texts.pkl"  # Cambia al nombre del archivo generado
with open(input_file, "rb") as file:
    data = pickle.load(file)

texts = [row["text"] for row in data]
node_ids = [row["nodeId"] for row in data]

# Generar embeddings
def generate_text_embedding(texts_batch):
    inputs = tokenizer(texts_batch, return_tensors="pt", padding=True, truncation=True)
    inputs = {key: val.to(device) for key, val in inputs.items()}
    with torch.no_grad():
        outputs = model(**inputs)
        embeddings = outputs.last_hidden_state.mean(dim=1)
    return embeddings.cpu().numpy().tolist()

embeddings = []
batch_size = 32
for i in tqdm(range(0, len(texts), batch_size), desc="Generating embeddings"):
    batch_texts = texts[i:i + batch_size]
    batch_embeddings = generate_text_embedding(batch_texts)
    embeddings.extend(batch_embeddings)

# Guardar embeddings en un archivo pickle
output_file = "Review_summary_texts_embeddings.pkl"  # Nombre del archivo de salida
with open(output_file, "wb") as file:
    pickle.dump(list(zip(node_ids, embeddings)), file)

print(f"Embeddings saved to {output_file}")
