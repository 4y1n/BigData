import os
from dotenv import load_dotenv

load_dotenv()


def fetch_air_quality_data():
    api_key = os.getenv("AIR_QUALITY_API_KEY")

    # Platzhalter: später echter API-Request
    sample_data = {
        "source": "air_quality_api",
        "city": "Vienna",
        "pm25": 12.4,
        "pm10": 20.1,
        "aqi": 42
    }

    return sample_data