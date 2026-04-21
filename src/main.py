from src.api.weather_api import fetch_weather_data
from src.api.air_quality_api import fetch_air_quality_data
from src.storage.save_raw_json import save_raw_json
from src.storage.insert_mongo import insert_document


def main():
    print("Starte Datenabholung...")

    weather_data = fetch_weather_data()
    air_quality_data = fetch_air_quality_data()

    weather_file = save_raw_json(
        data=weather_data,
        base_folder="data/raw/weather",
        prefix="weather"
    )

    air_quality_file = save_raw_json(
        data=air_quality_data,
        base_folder="data/raw/air_quality",
        prefix="air_quality"
    )

    weather_id = insert_document("weather_raw", weather_data)
    air_quality_id = insert_document("air_quality_raw", air_quality_data)

    print(f"Wetterdaten gespeichert in Datei: {weather_file}")
    print(f"Luftqualitätsdaten gespeichert in Datei: {air_quality_file}")
    print(f"Wetterdaten in Mongo gespeichert mit ID: {weather_id}")
    print(f"Luftqualitätsdaten in Mongo gespeichert mit ID: {air_quality_id}")

