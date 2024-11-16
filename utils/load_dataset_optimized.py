import utils.db as db


def load_dataset():
    with db.connect() as driver:
        with driver.session() as session:
            with open("utils/p.cypher", "r") as file:
                query = file.read()
            session.run(query)  # type: ignore
