import utils.db as db

def load_dataset():
    with db.connect() as driver:
        with driver.session() as session:
            print("Creating indexes...")
            session.run("CREATE INDEX IF NOT EXISTS FOR (b:Book) ON (b.title);")
            session.run("CREATE INDEX IF NOT EXISTS FOR (u:User) ON (u.user_id);")
            session.run("CREATE INDEX IF NOT EXISTS FOR (a:Author) ON (a.name);")
            session.run("CREATE INDEX IF NOT EXISTS FOR (p:Publisher) ON (p.name);")
            print("Indexes created.")

            print("Loading books, authors, and publishers data...")
            session.run("""
                CALL apoc.periodic.iterate(
                    'LOAD CSV WITH HEADERS FROM "file:///books_data.csv" AS row RETURN row',
                    'WITH row WHERE row.Title IS NOT NULL AND row.Title <> ""
                    CREATE (b:Book {
                        title: row.Title,
                        description: CASE WHEN row.description IS NOT NULL AND row.description <> "" THEN row.description ELSE NULL END,
                        image: CASE WHEN row.image IS NOT NULL AND row.image <> "" THEN row.image ELSE NULL END,
                        previewLink: CASE WHEN row.previewLink IS NOT NULL AND row.previewLink <> "" THEN row.previewLink ELSE NULL END,
                        publishedDate: CASE WHEN row.publishedDate IS NOT NULL AND row.publishedDate <> "" THEN row.publishedDate ELSE NULL END,
                        infoLink: CASE WHEN row.infoLink IS NOT NULL AND row.infoLink <> "" THEN row.infoLink ELSE NULL END,
                        categories: CASE WHEN row.categories IS NOT NULL AND row.categories <> "" THEN row.categories ELSE NULL END,
                        ratingsCount: CASE WHEN row.ratingsCount IS NOT NULL AND row.ratingsCount <> "" THEN toInteger(row.ratingsCount) ELSE NULL END
                    })
                    WITH b, row
                    FOREACH (authorName IN CASE
                        WHEN row.authors IS NOT NULL AND size(row.authors) > 2 THEN
                            split(substring(row.authors, 1, size(row.authors) - 2), ",")
                        ELSE []
                        END |
                        MERGE (a:Author {name: trim(authorName)})
                        CREATE (b)-[:WRITTEN_BY]->(a)
                    )
                    WITH b, row
                    WHERE row.publisher IS NOT NULL
                    MERGE (p:Publisher {name: row.publisher})
                    CREATE (b)-[:PUBLISHED_BY]->(p)',
                    {batchSize: 10000, parallel: true}
                )
            """)
            print("Books, authors, and publishers data loaded.")

            print("Loading users, reviews, and relationships...")
            session.run("""
                CALL apoc.periodic.iterate(
                    'LOAD CSV WITH HEADERS FROM "file:///books_rating.csv" AS row RETURN row',
                    'MERGE (u:User {user_id: row.User_id})
                    SET u.profileName = row.profileName
                    CREATE (r:Review {
                        helpfulness: row.`review/helpfulness`,
                        score: toFloat(row.`review/score`),
                        time: datetime({epochSeconds: toInteger(row.`review/time`)}),
                        summary: row.`review/summary`,
                        text: row.`review/text`
                    })
                    CREATE (u)-[:WROTE_REVIEW]->(r)
                    WITH r, row
                    WHERE row.Title IS NOT NULL AND row.Title <> ""
                    MATCH (b:Book {title: row.Title})
                    CREATE (r)-[:REVIEWS]->(b)',
                    {batchSize: 10000, parallel: true}
                )
            """)
            print("Users, reviews, and relationships loaded.")
