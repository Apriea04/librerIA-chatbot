import utils.db as db


def load_dataset():
    with db.connect() as driver:
        with driver.session() as session:
            session.run("""
                        CREATE INDEX FOR (b:Book) ON (b.title);
                        CREATE INDEX FOR (u:User) ON (u.user_id);
                        """)
            session.run("""
                        LOAD CSV WITH HEADERS FROM 'file:///books_data.csv' AS row
                        CREATE (b:Book {
                        title: row.Title,
                        description: row.description,
                        authors: row.authors,
                        image: row.image,
                        previewLink: row.previewLink,
                        publisher: row.publisher,
                        publishedDate: row.publishedDate,
                        infoLink: row.infoLink,
                        categories: row.categories,
                        ratingsCount: toInteger(row.ratingsCount)
                        });
                        """)
            session.run("""
                        LOAD CSV WITH HEADERS FROM 'file:///books_rating.csv' AS row
                        // Crear nodos de usuario
                        MERGE (u:User {user_id: row.User_id})
                        ON CREATE SET u.profileName = row.profileName;

                        // Crear nodos de reseña
                        CREATE (r:Review {
                        helpfulness: row.`review/helpfulness`,
                        score: toFloat(row.`review/score`),
                        time: datetime({epochSeconds: toInteger(row.`review/time`)}),
                        summary: row.`review/summary`,
                        text: row.`review/text`
                        });

                        // Crear relaciones entre usuarios y reseñas
                        MERGE (u)-[:WROTE_REVIEW]->(r);

                        // Crear relaciones entre libros y reseñas
                        MATCH (b:Book {title: row.Title})
                        MERGE (r)-[:REVIEWS]->(b);
                        """)
