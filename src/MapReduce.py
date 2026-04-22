import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from src.db.mongo_client import get_database


RAW_ROOT_CANDIDATES = [Path("data/raw")]
LEGACY_RAW_ROOT_CANDIDATES = [Path("src/data/raw")]
PROCESSED_OUTPUT_DIR = Path("data/processed")


def _find_latest_file(folder: Path, prefix: str) -> Path | None:
    if not folder.exists():
        return None

    matching_files = sorted(folder.glob(f"{prefix}_*.json"))
    return matching_files[-1] if matching_files else None


def _load_json_file(file_path: Path) -> dict:
    with file_path.open("r", encoding="utf-8") as file_handle:
        return json.load(file_handle)


def _load_latest_raw_inputs() -> tuple[dict, dict, dict]:
    for raw_root in RAW_ROOT_CANDIDATES:
        weather_file = _find_latest_file(raw_root / "weather", "weather")
        air_quality_file = _find_latest_file(raw_root / "air_quality", "air_quality")

        if weather_file and air_quality_file:
            return (
                _load_json_file(weather_file),
                _load_json_file(air_quality_file),
                {
                    "type": "json_files",
                    "weather_file": str(weather_file),
                    "air_quality_file": str(air_quality_file),
                },
            )

    try:
        db = get_database()
        weather_data = db["weather_raw"].find_one(sort=[("_id", -1)])
        air_quality_data = db["air_quality_raw"].find_one(sort=[("_id", -1)])
    except RuntimeError:
        weather_data = None
        air_quality_data = None

    if weather_data and air_quality_data:
        weather_data.pop("_id", None)
        air_quality_data.pop("_id", None)

        return (
            weather_data,
            air_quality_data,
            {
                "type": "mongodb_latest_documents",
                "weather_collection": "weather_raw",
                "air_quality_collection": "air_quality_raw",
            },
        )

    for raw_root in LEGACY_RAW_ROOT_CANDIDATES:
        weather_file = _find_latest_file(raw_root / "weather", "weather")
        air_quality_file = _find_latest_file(raw_root / "air_quality", "air_quality")

        if weather_file and air_quality_file:
            return (
                _load_json_file(weather_file),
                _load_json_file(air_quality_file),
                {
                    "type": "legacy_json_files",
                    "weather_file": str(weather_file),
                    "air_quality_file": str(air_quality_file),
                },
            )

    raise RuntimeError(
        "Keine Rohdaten gefunden. Fuehre zuerst die Datenabholung aus oder lege JSON-Dateien unter data/raw ab."
    )


def load_latest_raw_records() -> tuple[list[dict], dict]:
    weather_data, air_quality_data, input_source = _load_latest_raw_inputs()
    weather_by_slug = {city["slug"]: city for city in weather_data["cities"]}
    air_quality_by_slug = {city["slug"]: city for city in air_quality_data["cities"]}
    sample_dates = weather_data.get("sample_dates") or air_quality_data.get("sample_dates") or []

    raw_records = []

    for slug, weather_city in weather_by_slug.items():
        air_quality_city = air_quality_by_slug.get(slug)

        if not air_quality_city:
            continue

        weather_series = {point["date"]: point for point in weather_city["series"]}
        air_quality_series = {point["date"]: point for point in air_quality_city["series"]}

        for sample_date in sample_dates:
            weather_point = weather_series.get(sample_date, {})
            air_quality_point = air_quality_series.get(sample_date, {})
            weather_data_point = weather_point.get("data") or {}
            weather_meta = (weather_data_point.get("weather") or [{}])[0]

            raw_records.append(
                {
                    "city": weather_city["city"],
                    "city_slug": slug,
                    "date": sample_date,
                    "timestamp_local": (
                        weather_point.get("target_timestamp_local")
                        or air_quality_point.get("target_timestamp_local")
                        or sample_date
                    ),
                    "weather_status": weather_point.get("status", "missing"),
                    "air_quality_status": air_quality_point.get("status", "missing"),
                    "temperature_c": weather_data_point.get("temp"),
                    "weather_description": weather_meta.get("description"),
                    "air_quality_value": air_quality_point.get("value"),
                    "air_quality_unit": air_quality_point.get("unit") or air_quality_city.get("unit"),
                    "missing_weather": weather_point.get("status") != "available",
                    "missing_air_quality": air_quality_point.get("value") is None,
                }
            )

    return raw_records, input_source


def impute_missing_neighbor_averages(raw_records: list[dict]) -> list[dict]:
    processed_records = [dict(record) for record in raw_records]

    for record in processed_records:
        record["processed_temperature_c"] = record["temperature_c"]
        record["processed_air_quality_value"] = record["air_quality_value"]
        record["imputed_temperature"] = False
        record["imputed_air_quality"] = False

    grouped_indices = defaultdict(list)

    for index, record in enumerate(processed_records):
        grouped_indices[record["city_slug"]].append(index)

    for city_indices in grouped_indices.values():
        for position, current_index in enumerate(city_indices):
            if 0 < position < len(city_indices) - 1:
                previous_record = processed_records[city_indices[position - 1]]
                current_record = processed_records[current_index]
                next_record = processed_records[city_indices[position + 1]]

                if current_record["processed_temperature_c"] is None:
                    previous_temperature = previous_record["temperature_c"]
                    next_temperature = next_record["temperature_c"]
                    if previous_temperature is not None and next_temperature is not None:
                        current_record["processed_temperature_c"] = round(
                            (previous_temperature + next_temperature) / 2,
                            2,
                        )
                        current_record["imputed_temperature"] = True

                if current_record["processed_air_quality_value"] is None:
                    previous_air_quality = previous_record["air_quality_value"]
                    next_air_quality = next_record["air_quality_value"]
                    if previous_air_quality is not None and next_air_quality is not None:
                        current_record["processed_air_quality_value"] = round(
                            (previous_air_quality + next_air_quality) / 2,
                            2,
                        )
                        current_record["imputed_air_quality"] = True

    return processed_records


def map_raw_records(processed_records: list[dict]) -> list[tuple[str, dict]]:
    mapped_records = []

    for record in processed_records:
        mapped_records.append(
            (
                record["city_slug"],
                {
                    "city": record["city"],
                    "temperature_sum": record["processed_temperature_c"] or 0,
                    "temperature_count": 0 if record["processed_temperature_c"] is None else 1,
                    "air_quality_sum": record["processed_air_quality_value"] or 0,
                    "air_quality_count": 0 if record["processed_air_quality_value"] is None else 1,
                    "missing_weather_count": 1 if record["missing_weather"] else 0,
                    "missing_air_quality_count": 1 if record["missing_air_quality"] else 0,
                    "imputed_temperature_count": 1 if record["imputed_temperature"] else 0,
                    "imputed_air_quality_count": 1 if record["imputed_air_quality"] else 0,
                    "complete_pair_count": 1
                    if (
                        record["processed_temperature_c"] is not None
                        and record["processed_air_quality_value"] is not None
                    )
                    else 0,
                    "record_count": 1,
                    "air_quality_unit": record["air_quality_unit"],
                },
            )
        )

    return mapped_records


def reduce_mapped_records(mapped_records: list[tuple[str, dict]]) -> list[dict]:
    grouped_records = defaultdict(list)

    for key, value in mapped_records:
        grouped_records[key].append(value)

    reduced_records = []

    for city_slug, values in grouped_records.items():
        city_name = values[0]["city"]
        temperature_sum = sum(value["temperature_sum"] for value in values)
        temperature_count = sum(value["temperature_count"] for value in values)
        air_quality_sum = sum(value["air_quality_sum"] for value in values)
        air_quality_count = sum(value["air_quality_count"] for value in values)
        missing_weather_count = sum(value["missing_weather_count"] for value in values)
        missing_air_quality_count = sum(value["missing_air_quality_count"] for value in values)
        imputed_temperature_count = sum(value["imputed_temperature_count"] for value in values)
        imputed_air_quality_count = sum(value["imputed_air_quality_count"] for value in values)
        complete_pair_count = sum(value["complete_pair_count"] for value in values)
        record_count = sum(value["record_count"] for value in values)

        reduced_records.append(
            {
                "city": city_name,
                "city_slug": city_slug,
                "record_count": record_count,
                "complete_pair_count": complete_pair_count,
                "missing_weather_count": missing_weather_count,
                "missing_air_quality_count": missing_air_quality_count,
                "imputed_temperature_count": imputed_temperature_count,
                "imputed_air_quality_count": imputed_air_quality_count,
                "average_temperature_c": (
                    round(temperature_sum / temperature_count, 2) if temperature_count else None
                ),
                "average_air_quality": (
                    round(air_quality_sum / air_quality_count, 2) if air_quality_count else None
                ),
                "air_quality_unit": values[0]["air_quality_unit"],
            }
        )

    reduced_records.sort(key=lambda record: record["city"])
    return reduced_records


def _save_processed_result(result: dict, output_dir: Path = PROCESSED_OUTPUT_DIR) -> str:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"MapReduce_{timestamp}.json"

    with output_file.open("w", encoding="utf-8") as file_handle:
        json.dump(result, file_handle, ensure_ascii=False, indent=2)

    return str(output_file)


def load_latest_processed_result(output_dir: Path = PROCESSED_OUTPUT_DIR) -> dict:
    latest_file = _find_latest_file(output_dir, "MapReduce")

    if latest_file is None:
        raise RuntimeError(
            "Keine verarbeiteten MapReduce-Daten gefunden. Fuehre zuerst run_MapReduce_pipeline() aus."
        )

    result = _load_json_file(latest_file)
    result["processed_file"] = str(latest_file)
    return result


def run_MapReduce_pipeline() -> dict:
    raw_records, input_source = load_latest_raw_records()
    processed_records = impute_missing_neighbor_averages(raw_records)
    mapped_records = map_raw_records(processed_records)
    summary_records = reduce_mapped_records(mapped_records)

    result = {
        "pipeline": "MapReduce",
        "imputation_rule": "fehlender_wert = durchschnitt(vorheriger_zeitpunkt, naechster_zeitpunkt)",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "input_source": input_source,
        "raw_record_count": len(raw_records),
        "processed_record_count": len(processed_records),
        "summary_record_count": len(summary_records),
        "processed_records": processed_records,
        "summary_records": summary_records,
    }
    processed_file = _save_processed_result(result)
    result["processed_file"] = processed_file

    print(f"MapReduce verarbeitet: {len(raw_records)} Rohdatensaetze.")
    print(f"MapReduce gespeichert: {processed_file}")
    return result
