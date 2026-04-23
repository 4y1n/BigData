from src.db.mongo_client import get_database


def insert_document(collection_name: str, document: dict):
    db = get_database()
    result = db[collection_name].insert_one(document)
    return result.inserted_id


def insert_document_if_changed(collection_name: str, document: dict):
    db = get_database()
    latest_document = db[collection_name].find_one(sort=[("_id", -1)])

    if latest_document is not None:
        latest_document.pop("_id", None)
        if latest_document == document:
            return None, False

    result = db[collection_name].insert_one(document)
    return result.inserted_id, True


def insert_many_documents(collection_name: str, documents: list[dict]):
    if not documents:
        return []

    db = get_database()
    result = db[collection_name].insert_many(documents)
    return result.inserted_ids
