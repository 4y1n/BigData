import os

import requests
from dotenv import load_dotenv

load_dotenv()

OPENAQ_BASE_URL = "https://api.openaq.org/v3"
DEFAULT_LOCATION_ID = "8118"


def fetch_air_quality_data():
    api_key = os.getenv("AIR_QUALITY_API_KEY")
    location_id = os.getenv("OPENAQ_LOCATION_ID", DEFAULT_LOCATION_ID)

    if not api_key:
        raise RuntimeError(
            "AIR_QUALITY_API_KEY fehlt. Trage deinen OpenAQ-API-Key in die .env ein."
        )

    response = requests.get(
        f"{OPENAQ_BASE_URL}/locations/{location_id}",
        headers={"X-API-Key": api_key},
        timeout=30,
    )
    response.raise_for_status()

    return {
        "source": "openaq",
        "location_id": location_id,
        "data": response.json(),
    }
