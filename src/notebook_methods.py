from html import escape
import importlib
import os
from pathlib import Path
import shutil
import subprocess
import sys

from dotenv import dotenv_values


NOTEBOOK_MODULES = [
    ("requests", "HTTP-Anfragen"),
    ("pymongo", "MongoDB-Anbindung"),
    ("dotenv", "Umgebungsvariablen"),
    ("matplotlib", "Matplotlib"),
    ("pandas", "Pandas"),
]

NOTEBOOK_ENV_KEYS = [
    "MONGO_URI",
    "MONGO_DB",
    "WEATHER_API_KEY",
    "WEATHER_USE_MOCK",
    "WEATHER_LAT",
    "WEATHER_LON",
    "WEATHER_UNITS",
    "WEATHER_LANG",
    "WEATHER_EXCLUDE",
    "AIR_QUALITY_API_KEY",
    "OPENAQ_LOCATION_ID",
]

NOTEBOOK_COMPARISON_CITIES = [
    ("vienna", "Wien"),
    ("new-york", "New York"),
    ("new-delhi", "Neu Delhi"),
    ("phoenix", "Phoenix"),
    ("reykjavik", "Reykjavik"),
]


def ensure_notebook_dependencies(requirements_file: str = "requirements.txt") -> None:
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "--quiet", "-r", requirements_file],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            result.stderr.strip() or "Installation der Abhaengigkeiten fehlgeschlagen."
        )

    for module_name, label in NOTEBOOK_MODULES:
        importlib.import_module(module_name)
        print(f"{label}: verfügbar und aktiv")


def _run_command(command: list[str], success_message: str, cwd: Path) -> None:
    result = subprocess.run(command, cwd=cwd, text=True, capture_output=True)
    if result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip() or "Unbekannter Fehler"
        raise RuntimeError(f"Fehler bei {' '.join(command)}:\n{message}")
    print(success_message)


def prepare_notebook_environment() -> Path:
    project_dir = Path.cwd()
    os.chdir(project_dir)
    print(f"Arbeitsverzeichnis: {project_dir}")

    env_example = project_dir / ".env.example"
    env_file = project_dir / ".env"

    if not env_file.exists():
        shutil.copyfile(env_example, env_file)
        print(".env aus .env.example erstellt")
    else:
        print(".env bereits vorhanden")

    for key, value in dotenv_values(env_file).items():
        if value is not None:
            os.environ[key] = value

    print(f"MONGO_URI={os.environ['MONGO_URI']}")
    print(f"MONGO_DB={os.environ['MONGO_DB']}")

    _run_command(["docker", "compose", "up", "-d", "mongodb"], "MongoDB-Container laeuft.", project_dir)
    return project_dir


def show_notebook_env_settings(env_path: str = ".env") -> None:
    env_file = Path(env_path)

    if env_file.exists():
        print(".env gefunden")
        values = dotenv_values(env_file)
        for key in NOTEBOOK_ENV_KEYS:
            value = values.get(key)
            if key.endswith("API_KEY"):
                state = "gesetzt" if value else "leer"
                print(f"{key}={state}")
            elif value is not None:
                print(f"{key}={value}")
        return

    print("Keine .env gefunden. Lege bitte eine Datei mit folgendem Inhalt an:")
    print("MONGO_URI=mongodb://localhost:27017/")
    print("MONGO_DB=big_data_weather_airpollution")
    print("WEATHER_API_KEY=")
    print("WEATHER_USE_MOCK=true")
    print("WEATHER_LAT=48.2082")
    print("WEATHER_LON=16.3738")
    print("WEATHER_UNITS=metric")
    print("WEATHER_LANG=de")
    print("WEATHER_EXCLUDE=minutely,alerts")
    print("AIR_QUALITY_API_KEY=")
    print("OPENAQ_LOCATION_ID=8118")


def _format_weather(point: dict) -> str:
    if point.get("status") != "available" or not point.get("data"):
        return "null"

    data = point["data"]
    weather = (data.get("weather") or [{}])[0]
    temp = data.get("temp")
    description = weather.get("description") or "ohne Beschreibung"
    if temp is None:
        return description
    return f"{temp} °C - {description}"


def _format_air_quality(point: dict, default_unit: str | None) -> str:
    value = point.get("value")
    if value is None:
        return "null"

    unit = point.get("unit") or default_unit or ""
    return f"{value} {unit}".strip()


def render_latest_comparison_tables() -> None:
    from IPython.display import HTML, display

    from src.db.mongo_client import get_database

    db = get_database()
    weather_data = db["weather_raw"].find_one(sort=[("_id", -1)])
    air_quality_data = db["air_quality_raw"].find_one(sort=[("_id", -1)])

    if not weather_data or not air_quality_data:
        raise RuntimeError(
            "Keine passenden Daten in MongoDB gefunden. Fuehre zuerst die Datenabholung aus."
        )

    weather_by_slug = {city["slug"]: city for city in weather_data["cities"]}
    air_quality_by_slug = {city["slug"]: city for city in air_quality_data["cities"]}
    sample_dates = weather_data.get("sample_dates") or air_quality_data.get("sample_dates") or []

    for slug, title in NOTEBOOK_COMPARISON_CITIES:
        weather_city = weather_by_slug[slug]
        air_quality_city = air_quality_by_slug[slug]
        weather_series = {point["date"]: point for point in weather_city["series"]}
        air_quality_series = {point["date"]: point for point in air_quality_city["series"]}

        rows = []
        for sample_date in sample_dates:
            weather_point = weather_series.get(sample_date, {})
            air_quality_point = air_quality_series.get(sample_date, {})
            timestamp = (
                weather_point.get("target_timestamp_local")
                or air_quality_point.get("target_timestamp_local")
                or sample_date
            )
            rows.append(
                "<tr>"
                f"<td>{escape(timestamp)}</td>"
                f"<td>{escape(_format_weather(weather_point))}</td>"
                f"<td>{escape(_format_air_quality(air_quality_point, air_quality_city.get('unit')))}</td>"
                "</tr>"
            )

        table_html = (
            f"<h3>{escape(title)}</h3>"
            "<table style='border-collapse: collapse; width: 100%; margin-bottom: 1.5rem;'>"
            "<thead><tr>"
            "<th style='border: 1px solid #ccc; padding: 0.5rem; text-align: left;'>Zeitpunkt</th>"
            "<th style='border: 1px solid #ccc; padding: 0.5rem; text-align: left;'>Wetter</th>"
            "<th style='border: 1px solid #ccc; padding: 0.5rem; text-align: left;'>Luftqualitaet</th>"
            "</tr></thead>"
            f"<tbody>{''.join(rows)}</tbody>"
            "</table>"
        )
        display(HTML(table_html))


def _records_to_dataframe(records: list[dict], columns: list[str]):
    import pandas as pd

    frame = pd.DataFrame(records)

    if frame.empty:
        return pd.DataFrame(columns=columns)

    for column in columns:
        if column not in frame.columns:
            frame[column] = None

    frame = frame[columns]
    return frame.where(frame.notna(), "null")


def _display_dataframe_table(title: str, records: list[dict], columns: list[str]) -> None:
    from IPython.display import HTML, display

    frame = _records_to_dataframe(records, columns)
    table_html = (
        f"<h3>{escape(title)}</h3>"
        + frame.to_html(index=False, border=0, classes="dataframe")
    )
    display(HTML(table_html))


def _format_wind_value(speed: float | None, direction_deg: float | int | None) -> str:
    if speed is None:
        return "null"

    if direction_deg is None:
        return f"{speed} m/s"

    return f"{speed} m/s ({direction_deg}°)"


def render_MapReduce_raw_table() -> None:
    from src.MapReduce import load_latest_raw_records

    raw_records, input_source = load_latest_raw_records()
    display_records = [
        {
            "Stadt": record["city"],
            "Datum": record["date"],
            "Zeitpunkt": record["timestamp_local"],
            "Temperatur (°C)": record["temperature_c"],
            "Wind": _format_wind_value(record["wind_speed"], record["wind_direction_deg"]),
            "Wetter": record["weather_description"] or "null",
            "Luftqualitaet": record["air_quality_value"],
            "Einheit": record["air_quality_unit"] or "null",
            "Fehlendes Wetter": "ja" if record["missing_weather"] else "nein",
            "Fehlende Windgeschwindigkeit": "ja" if record["missing_wind_speed"] else "nein",
            "Fehlende Luftqualitaet": "ja" if record["missing_air_quality"] else "nein",
        }
        for record in raw_records
    ]
    _display_dataframe_table(
        title=f"MapReduce Rohdaten ({input_source['type']})",
        records=display_records,
        columns=[
            "Stadt",
            "Datum",
            "Zeitpunkt",
            "Temperatur (°C)",
            "Wind",
            "Wetter",
            "Luftqualitaet",
            "Einheit",
            "Fehlendes Wetter",
            "Fehlende Windgeschwindigkeit",
            "Fehlende Luftqualitaet",
        ],
    )


def render_MapReduce_processed_table(mapreduce_result: dict | None = None) -> None:
    from src.MapReduce import load_latest_processed_result

    if mapreduce_result is None:
        mapreduce_result = load_latest_processed_result()

    display_records = [
        {
            "Stadt": record["city"],
            "Datum": record["date"],
            "Zeitpunkt": record["timestamp_local"],
            "Temperatur (°C)": record["processed_temperature_c"],
            "Wind": _format_wind_value(record["processed_wind_speed"], record["wind_direction_deg"]),
            "Wetter": record["weather_description"] or "null",
            "Luftqualitaet": record["processed_air_quality_value"],
            "Einheit": record["air_quality_unit"] or "null",
            "Temperatur imputiert": "ja" if record["imputed_temperature"] else "nein",
            "Wind imputiert": "ja" if record["imputed_wind_speed"] else "nein",
            "Luftqualitaet imputiert": "ja" if record["imputed_air_quality"] else "nein",
        }
        for record in mapreduce_result["processed_records"]
    ]
    _display_dataframe_table(
        title="MapReduce Processed Data",
        records=display_records,
        columns=[
            "Stadt",
            "Datum",
            "Zeitpunkt",
            "Temperatur (°C)",
            "Wind",
            "Wetter",
            "Luftqualitaet",
            "Einheit",
            "Temperatur imputiert",
            "Wind imputiert",
            "Luftqualitaet imputiert",
        ],
    )
