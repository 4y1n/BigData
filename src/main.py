import argparse

from src.api.air_quality_api import fetch_air_quality_data
from src.api.weather_api import fetch_weather_data
from src.storage.insert_mongo import insert_document_if_changed
from src.storage.raw_cache import load_latest_raw_snapshot
from src.storage.save_raw_json import save_raw_json


def _sync_raw_cache_to_mongo(weather_data: dict, air_quality_data: dict) -> None:
    weather_id, weather_inserted = insert_document_if_changed("weather_raw", weather_data)
    air_quality_id, air_quality_inserted = insert_document_if_changed("air_quality_raw", air_quality_data)

    if weather_inserted:
        print(f"Wetterdaten aus RAW-Cache in MongoDB uebertragen ({weather_id}).")
    else:
        print("Wetterdaten in MongoDB bereits auf dem Stand des RAW-Caches.")

    if air_quality_inserted:
        print(f"Luftqualitaetsdaten aus RAW-Cache in MongoDB uebertragen ({air_quality_id}).")
    else:
        print("Luftqualitaetsdaten in MongoDB bereits auf dem Stand des RAW-Caches.")


def main(refresh: bool = False):
    if not refresh:
        raw_snapshot = load_latest_raw_snapshot()

        if raw_snapshot is not None:
            weather_data, air_quality_data, snapshot_info = raw_snapshot
            print("Kein Datenrefresh angefordert. Verwende vorhandene RAW-JSON-Dateien.")
            print(f"Wetter-Cache: {snapshot_info['weather_file']}")
            print(f"Luftqualitaets-Cache: {snapshot_info['air_quality_file']}")
            _sync_raw_cache_to_mongo(weather_data, air_quality_data)
            print("Prozess abgeschlossen.")
            return

        print("Kein RAW-Cache gefunden. Es wird einmalig ein API-Abruf ausgefuehrt.")

    print("Starte Datenrefresh ueber die APIs...")

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

    weather_id, weather_inserted = insert_document_if_changed("weather_raw", weather_data)
    air_quality_id, air_quality_inserted = insert_document_if_changed("air_quality_raw", air_quality_data)

    print(f"Wetterdaten von OpenWeather gespeichert: {weather_file}")
    print(f"Luftqualitaetsdaten von OpenAQ gespeichert: {air_quality_file}")
    if weather_inserted:
        print(f"Wetterdaten in Datenbank uebertragen ({weather_id}).")
    else:
        print("Wetterdaten in MongoDB bereits vorhanden.")
    if air_quality_inserted:
        print(f"Luftqualitaetsdaten in Datenbank uebertragen ({air_quality_id}).")
    else:
        print("Luftqualitaetsdaten in MongoDB bereits vorhanden.")
    print("Prozess abgeschlossen.")


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Synchronisiert vorhandene RAW-JSON-Dateien nach MongoDB oder fuehrt bei Bedarf einen API-Refresh aus."
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Ruft Wetter- und Luftqualitaetsdaten erneut ueber die APIs ab und speichert einen neuen RAW-Snapshot.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    main(refresh=args.refresh)
