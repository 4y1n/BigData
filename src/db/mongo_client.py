import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "big_data_weather_airpollution")


def get_mongo_client():
    return MongoClient(MONGO_URI)


def get_database():
    client = get_mongo_client()
    return client[MONGO_DB]