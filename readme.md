# librerIA Chatbot

librerIA Chatbot es un chatbot de IA 100% local diseñado para proporcionar recomendaciones y responder preguntas sobre libros utilizando modelos de NLP y una base de datos Neo4j.
Este es el proyecto final de la asignatura de Sistemas de Información de Gestión y Business Intelligence del Grado en Ingeniería Informática de la Universidad de León.

## Estructura del Proyecto

- `agents/`: Contiene la lógica del agente y herramientas relacionadas.
- `data/`: Archivos de datos y scripts de procesamiento.
- `doc/`: Documentación del proyecto.
- `models/`: Gestión de embeddings y modelos.
- `testing/`: Scripts de prueba.
- `utils/`: Utilidades y scripts auxiliares.
- `view/`: Interfaz de usuario y lógica de presentación.
- `.env`: Archivo de configuración de variables de entorno.
- `.gitignore`: Lista de archivos y directorios que Git debe ignorar.
- `main.py`: Punto de entrada principal de la aplicación.
- `requirements.txt`: Lista de dependencias del proyecto.
- `README.md`: Breve descripción del proyecto.

## Instalación

1. Clona el repositorio:

   ```sh
   git clone https://github.com/Apriea04/librerIA-chatbot
   cd libreIA-chatbot
   ```

2. Se recomienda crear y activar un entorno virtual con `Python 3.11.9`.

3. Instala las dependencias:

   ```sh
   pip install -r requirements.txt
   ```

4. Descarga e instala Ollama y los modelos necesarios, por ejemplo, `llama3.3`.

5. Descarga, instala y crea una nueva BBDD en Neo4j.

6. Configura las variables de entorno en el archivo `.env`:

   ```bash
   NEO4J_URI=bolt://<TU_URI_NEO4J>
   NEO4J_USERNAME=<TU_USUARIO_NEO4J>
   NEO4J_PASSWORD=<TU_CONTRASEÑA_NEO4J>
   ALL_BOOKS_PATH=data/books_data.csv
   ALL_RATINGS_PATH=data/books_rating.csv
   BOOKS_PATH=data/books_data.csv
   RATINGS_PATH=data/books_rating.csv
   BATCH_SIZE=10000
   EMBEDDINGS_MODEL=dunzhang/stella_en_1.5B_v5
   AGENT_LLM_MODEL=llama3.3
   ```

7. Descarga el dataset `Amazon Book Reviews` de [Kaggle](https://www.kaggle.com/datasets/mohamedbakhet/amazon-books-reviews).

8. Ejecuta el fichero `data/dataset_corrections.py` para limpiar y procesar el dataset.

9. Abre una conexion con la BBDD y ejecuta la consulta Cypher del fichero `utils/load.cypher`.

10. Ejecuta el método `generate_embeddings_for` de la clase `DBManager` para generar los embeddings de los campos necesarios y guardarlos en la BBDD, pasando como parámetros los siguientes valores:
    - `"Book"`, `"title"`, `"title"`; para los títulos de los libros.
    - `"Book"`, `"description"`, `"title"`; para las descripciones de los libros.
    - `"Review"`, `"summary"`, `""`; para los resúmenes de las reseñas.
    - `"Review"`, `"text"`, `""`; para los textos de las reseñas.

## Ejecución

1. Poner en marcha la BBDD de Neo4j.
2. Ejecutar Ollama para tener un servidor de LLM.
3. Ejecutar la aplicación:

   ```sh
   streamlit run main.py
   ```

4. Abre tu navegador web y ve a `http://localhost:8501` para interactuar con el chatbot.

Nótese que al ser la ejecución 100% local, es posible que el sistema sea lento.

## Autor

Este proyecto ha sido desarrollado por [Álvaro Prieto Álvarez](https://github.com/Apriea04).
