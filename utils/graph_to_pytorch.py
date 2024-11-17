import torch
from torch_geometric.data import Data

def graph_to_pytorch(graph_data):
    # Crear listas para almacenar los índices de los bordes y los atributos de los nodos
    edge_index = []
    node_features = {}
    
    for record in graph_data:
        source = int(record['source'].split(':')[-1])  # Convertir a entero
        target = int(record['target'].split(':')[-1])  # Convertir a entero
        
        # Agregar los índices de los bordes
        edge_index.append([source, target])
        
        # Inicializar los atributos de los nodos si no existen
        if source not in node_features:
            node_features[source] = [0]  # Reemplaza [0] con los atributos reales del nodo
        if target not in node_features:
            node_features[target] = [0]  # Reemplaza [0] con los atributos reales del nodo
    
    # Convertir edge_index a un tensor de PyTorch
    edge_index = torch.tensor(edge_index, dtype=torch.long).t().contiguous()
    
    # Convertir node_features a un tensor de PyTorch
    node_features = torch.tensor([node_features[node] for node in sorted(node_features)], dtype=torch.float)
    
    # Crear el objeto Data de PyTorch Geometric
    graph = Data(x=node_features, edge_index=edge_index)
    
    return graph, node_features
