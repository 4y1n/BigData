import json
from pathlib import Path
from typing import Optional, Tuple


def load_json_file(file_path: Path | str) -> dict:
    """
    Lädt eine JSON-Datei und gibt das Ergebnis als Dict zurück.
    """
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_latest_raw_snapshot() -> Optional[Tuple[dict, dict, dict]]:
    """
    Lädt die neuesten RAW-JSON-Snapshots aus data/raw/weather/ und data/raw/air_quality/.

    Returns:
        Tuple von (weather_data, air_quality_data, snapshot_info) oder None wenn keine Dateien vorhanden
    """
    weather_dir = Path("data/raw/weather")
    air_quality_dir = Path("data/raw/air_quality")

    # Finde die neueste Wetterdatei
    weather_files = sorted(weather_dir.glob("weather_*.json"), reverse=True)
    if not weather_files:
        return None

    # Finde die neueste Luftqualitätsdatei
    air_quality_files = sorted(air_quality_dir.glob("air_quality_*.json"), reverse=True)
    if not air_quality_files:
        return None

    # Lade die neuesten Dateien
    with open(weather_files[0], "r", encoding="utf-8") as f:
        weather_data = json.load(f)

    with open(air_quality_files[0], "r", encoding="utf-8") as f:
        air_quality_data = json.load(f)

    snapshot_info = {
        "weather_file": str(weather_files[0]),
        "air_quality_file": str(air_quality_files[0]),
    }

    return weather_data, air_quality_data, snapshot_info


