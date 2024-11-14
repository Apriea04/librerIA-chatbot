CREATE INDEX IF NOT EXISTS FOR (b:Book) ON (b.title);
CREATE INDEX IF NOT EXISTS FOR (u:User) ON (u.user_id);
CREATE INDEX IF NOT EXISTS FOR (a:Author) ON (a.name);
CREATE INDEX IF NOT EXISTS FOR (p:Publisher) ON (p.name);
CREATE INDEX IF NOT EXISTS FOR (g:Genre) ON (g.name);
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
                    CREATE (b)-[:PUBLISHED_BY]->(p)
                    WITH b, row
                    WHERE row.categories IS NOT NULL
                    WITH b, split(replace(replace(row.categories, "[", ""), "]", ""), ",") AS genres
                    FOREACH (genreName IN genres |
                        MERGE (g:Genre {name: trim(genreName)})
                        MERGE (b)-[:BELONGS_TO]->(g)
                    )',
                    {batchSize: 10000, parallel: true}
                );
CALL apoc.periodic.iterate(
                    'LOAD CSV WITH HEADERS FROM "file:///books_rating_processed.csv" AS row RETURN row',
                    'FOREACH(ignoreMe IN CASE WHEN row.User_id IS NOT NULL THEN [1] ELSE [] END |
                        MERGE (u:User {userId: row.User_id})
                        SET u.profileName = row.profileName
                    )
                    // Crear el nodo de revisión independientemente de la existencia del usuario
                    CREATE (r:Review {
                        helpfulness: row.`review/helpfulness`,
                        score: row.`review/score`,
                        time: row.`review/time`,
                        summary: row.`review/summary`,
                        text: row.`review/text`
                    })
                    
                    // Establecer la relación entre User y Review si el usuario existe
                    FOREACH(ignoreMe IN CASE WHEN row.User_id IS NOT NULL THEN [1] ELSE [] END |
                        MERGE (u)-[:WROTE_REVIEW]->(r)
                    )

                    // Crear la relación entre Review y Book si el título existe
                    WITH r, row
                    WHERE row.Title IS NOT NULL AND row.Title <> ""
                    MATCH (b:Book {title: row.Title})
                    SET b.bookId = row.Id
                    CREATE (r)-[:REVIEWS]->(b)',
                    {batchSize: 10000, parallel: false}
                );