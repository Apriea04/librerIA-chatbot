//TODO: se recomienda primero crear las contrains y uan vez cargados los datos, crear los indices
// Hay que crear los vector index
CREATE INDEX IF NOT EXISTS FOR (b:Book) ON (b.title);
CREATE INDEX IF NOT EXISTS FOR (u:User) ON (u.userId);
CREATE INDEX IF NOT EXISTS FOR (a:Author) ON (a.name);
CREATE INDEX IF NOT EXISTS FOR (p:Publisher) ON (p.name);
CREATE INDEX IF NOT EXISTS FOR (g:Genre) ON (g.name);

CALL apoc.periodic.iterate(
    'LOAD CSV WITH HEADERS FROM "file:///books_data.csv" AS row RETURN row',
    '
    WITH row WHERE row.Title IS NOT NULL AND row.Title <> ""
    MERGE (b:Book {title: row.Title}) // Usa MERGE para evitar duplicados
    SET b.description = CASE WHEN row.description IS NOT NULL AND row.description <> "" THEN row.description ELSE b.description END,
        b.image = CASE WHEN row.image IS NOT NULL AND row.image <> "" THEN row.image ELSE b.image END,
        b.previewLink = CASE WHEN row.previewLink IS NOT NULL AND row.previewLink <> "" THEN row.previewLink ELSE b.previewLink END,
        b.publishedDate = CASE WHEN row.publishedDate IS NOT NULL AND row.publishedDate <> "" THEN row.publishedDate ELSE b.publishedDate END,
        b.infoLink = CASE WHEN row.infoLink IS NOT NULL AND row.infoLink <> "" THEN row.infoLink ELSE b.infoLink END,
        b.ratingsCount = CASE WHEN row.ratingsCount IS NOT NULL AND row.ratingsCount <> "" THEN toInteger(row.ratingsCount) ELSE b.ratingsCount END

    WITH b, row
    FOREACH (authorName IN CASE
        WHEN row.authors IS NOT NULL AND size(row.authors) > 2 THEN
            split(substring(row.authors, 1, size(row.authors) - 2), ",")
        ELSE []
        END |
        MERGE (a:Author {name: trim(authorName)}) // Usa MERGE para los autores
        MERGE (b)-[:WRITTEN_BY]->(a)
    )

    WITH b, row
    WHERE row.publisher IS NOT NULL
    MERGE (p:Publisher {name: row.publisher}) // Usa MERGE para editoriales
    MERGE (b)-[:PUBLISHED_BY]->(p)

    WITH b, row
    WHERE row.categories IS NOT NULL
    WITH b, split(replace(replace(row.categories, "[", ""), "]", ""), ",") AS genres
    FOREACH (genreName IN genres |
        MERGE (g:Genre {name: trim(genreName)}) // Usa MERGE para géneros
        MERGE (b)-[:BELONGS_TO]->(g)
    )',
    {batchSize: 100000, parallel: false}
);

CALL apoc.periodic.iterate(
    'LOAD CSV WITH HEADERS FROM "file:///books_rating_processed_reduced.csv" AS row RETURN row',
    '
    // Crear o actualizar el nodo Usuario
    WITH row
    WHERE row.User_id IS NOT NULL AND row.User_id <> ""
    MERGE (u:User {userId: trim(row.User_id)}) // Usa trim para evitar espacios
    SET u.profileName = row.profileName

    // Crear la reseña
    CREATE (r:Review {
        helpfulness: row.`review/helpfulness`,
        score: toFloat(row.`review/score`), // Convierte a float si es necesario
        time: row.`review/time`,
        summary: row.`review/summary`,
        text: row.`review/text`
    })

    // Relacionar usuario con la reseña
    MERGE (u)-[:WROTE_REVIEW]->(r)

    // Relacionar reseña con el libro
    WITH r, row
    WHERE row.Title IS NOT NULL AND row.Title <> ""
    MATCH (b:Book {title: trim(row.Title)}) // Usa trim para evitar espacios
    SET b.bookId = row.Id
    MERGE (r)-[:REVIEWS]->(b)
    ',
    {batchSize: 100000, parallel: false}
);
