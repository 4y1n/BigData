import os
from dotenv import load_dotenv

load_dotenv()


def fetch_weather_data():
    api_key = os.getenv("WEATHER_API_KEY")

    # Platzhalter: später echter API-Request
    sample_data = {
        "source": "weather_api",
        "city": "Vienna",
        "temperature": 18.5,
        "unit": "celsius"
    }

    return sample_data