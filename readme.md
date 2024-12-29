# librerIA Chatbot

librerIA Chatbot es un asistente de IA diseñado para proporcionar recomendaciones de libros y responder preguntas relacionadas con libros utilizando un modelo de lenguaje avanzado y una base de datos Neo4j.
La base de datos está cargada con el dataset de Kaggle [Amazon Book Reviews](https://www.kaggle.com/datasets/mohamedbakhet/amazon-books-reviews).

## Estructura del Proyecto

```
- .env: Archivo de configuración de variables de entorno.
- .gitignore: Lista de archivos y directorios que Git debe ignorar.
- agents/: Contiene la lógica del agente y herramientas relacionadas.
- data/: Archivos de datos y scripts de procesamiento.
- main.py: Punto de entrada principal de la aplicación.
- models/: Gestión de embeddings y modelos.
- requirements.txt: Lista de dependencias del proyecto.
- testing/: Scripts de prueba.
- utils/: Utilidades y scripts auxiliares.
- view/: Interfaz de usuario y lógica de presentación.
```

## Instalación

1. Clona el repositorio:

   ```sh
   git clone https://github.com/Apriea04/SIBI
   cd SIBI
   ```

2. Crea y activa un entorno virtual:

   ```sh
   python -m venv .venv
   source .venv/bin/activate  # En Windows: .venv\Scripts\activate
   ```

3. Instala las dependencias:

   ```sh
   pip install -r requirements.txt
   ```

4. Configura las variables de entorno en el archivo [.env](http://_vscodecontentref_/2):
   ```env
   NEO4J_URI=bolt://<TU_URI_NEO4J>
   NEO4J_USERNAME=<TU_USUARIO_NEO4J>
   NEO4J_PASSWORD=<TU_CONTRASEÑA_NEO4J>
   ALL_BOOKS_PATH=data/books_data.csv
   ALL_RATINGS_PATH=data/books_rating.csv
   BOOKS_PATH=data/books_data.csv
   RATINGS_PATH=data/books_rating.csv
   BATCH_SIZE=10000
   EMBEDDINGS_MODEL=dunzhang/stella_en_1.5B_v5
   AGENT_LLM_MODEL=llama3.2
   ```

## Uso

1. Inicia la aplicación:

   ```sh
   streamlit run main.py
   ```

2. Abre tu navegador web y ve a `http://localhost:8501` para interactuar con el chatbot.

## Estructura de Archivos

- [main.py](http://_vscodecontentref_/3): Punto de entrada principal de la aplicación.
- [agents](http://_vscodecontentref_/4): Contiene la lógica del agente y herramientas relacionadas.
- [data](http://_vscodecontentref_/5): Archivos de datos y scripts de procesamiento.
- [models](http://_vscodecontentref_/6): Gestión de embeddings y modelos.
- [testing](http://_vscodecontentref_/7): Scripts de prueba.
- [utils](http://_vscodecontentref_/8): Utilidades y scripts auxiliares.
- [view](http://_vscodecontentref_/9): Interfaz de usuario y lógica de presentación.
