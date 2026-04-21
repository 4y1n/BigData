import os
from functools import lru_cache

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "big_data_weather_airpollution")


@lru_cache(maxsize=1)
def get_mongo_client():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)


def get_database():
    client = get_mongo_client()
    try:
        client.admin.command("ping")
    except ServerSelectionTimeoutError as exc:
        raise RuntimeError(
            f"MongoDB unter {MONGO_URI} ist nicht erreichbar. "
            "Starte zuerst den Container mit 'docker compose up -d mongodb'."
        ) from exc
    return client[MONGO_DB]
