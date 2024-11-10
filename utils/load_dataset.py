import utils.db as db


def load_dataset():
    with db.connect() as driver:
        with driver.session() as session:
            # Crear índices en consultas separadas
            session.run("CREATE INDEX FOR (b:Book) ON (b.title);")
            session.run("CREATE INDEX FOR (u:User) ON (u.user_id);")

            # Crear nodos de libros con propiedades opcionales, ignorando filas sin título
            session.run("""
                LOAD CSV WITH HEADERS FROM 'file:///books_data.csv' AS row
                WITH row
                WHERE row.Title IS NOT NULL AND row.Title <> ""
                
                MERGE (b:Book {title: row.Title})
                ON CREATE SET 
                    b.description = CASE WHEN row.description IS NOT NULL AND row.description <> "" THEN row.description ELSE NULL END,
                    b.image = CASE WHEN row.image IS NOT NULL AND row.image <> "" THEN row.image ELSE NULL END,
                    b.previewLink = CASE WHEN row.previewLink IS NOT NULL AND row.previewLink <> "" THEN row.previewLink ELSE NULL END,
                    b.publishedDate = CASE WHEN row.publishedDate IS NOT NULL AND row.publishedDate <> "" THEN row.publishedDate ELSE NULL END,
                    b.infoLink = CASE WHEN row.infoLink IS NOT NULL AND row.infoLink <> "" THEN row.infoLink ELSE NULL END,
                    b.categories = CASE WHEN row.categories IS NOT NULL AND row.categories <> "" THEN row.categories ELSE NULL END,
                    b.ratingsCount = CASE WHEN row.ratingsCount IS NOT NULL AND row.ratingsCount <> "" THEN toInteger(row.ratingsCount) ELSE NULL END;
            """)

            # Crear nodos de autores y relaciones WRITTEN_BY
            session.run("""
                LOAD CSV WITH HEADERS FROM 'file:///books_data.csv' AS row
                WITH row
                WHERE row.Title IS NOT NULL AND row.Title <> ""
                
                MATCH (b:Book {title: row.Title})
                FOREACH (authorName IN CASE WHEN row.authors IS NOT NULL THEN split(row.authors, ",") ELSE [] END |
                    MERGE (a:Author {name: trim(authorName)})
                    MERGE (b)-[:WRITTEN_BY]->(a)
                );
            """)

            # Crear nodos de editoriales y relaciones PUBLISHED_BY
            session.run("""
                LOAD CSV WITH HEADERS FROM 'file:///books_data.csv' AS row
                WITH row
                WHERE row.Title IS NOT NULL AND row.Title <> ""
                
                MATCH (b:Book {title: row.Title})
                WHERE row.publisher IS NOT NULL
                MERGE (p:Publisher {name: row.publisher})
                MERGE (b)-[:PUBLISHED_BY]->(p);
            """)

            # Crear nodos de usuarios y reseñas con relaciones
            session.run("""
                LOAD CSV WITH HEADERS FROM 'file:///books_rating.csv' AS row
                WITH row
                
                MERGE (u:User {user_id: row.User_id})
                ON CREATE SET u.profileName = row.profileName;

                CREATE (r:Review {
                    helpfulness: row.`review/helpfulness`,
                    score: toFloat(row.`review/score`),
                    time: datetime({epochSeconds: toInteger(row.`review/time`)}),
                    summary: row.`review/summary`,
                    text: row.`review/text`
                });

                MERGE (u)-[:WROTE_REVIEW]->(r);
            """)

            # Crear relaciones entre libros y reseñas
            session.run("""
                LOAD CSV WITH HEADERS FROM 'file:///books_rating.csv' AS row
                WITH row
                WHERE row.Title IS NOT NULL AND row.Title <> ""
                
                MATCH (b:Book {title: row.Title})
                MATCH (r:Review {summary: row.`review/summary`})
                MERGE (r)-[:REVIEWS]->(b);
            """)
