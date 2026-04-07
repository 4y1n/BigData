from src.db.mongo_client import get_database


def insert_document(collection_name: str, document: dict):
    db = get_database()
    result = db[collection_name].insert_one(document)
    return result.inserted_id


def insert_many_documents(collection_name: str, documents: list[dict]):
    if not documents:
        return []

    db = get_database()
    result = db[collection_name].insert_many(documents)
    return result.inserted_ids