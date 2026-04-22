import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

from src.comparison_config import (
    COMPARISON_CITIES,
    COMPARISON_NOON_WINDOW_HOURS,
    COMPARISON_SAMPLE_YEAR,
    COMPARISON_TIMEPOINT_COUNT,
    get_city_target_schedule,
    get_sample_dates,
)

load_dotenv()

OPENAQ_BASE_URL = "https://api.openaq.org/v3"


def _iso_to_datetime(timestamp_value: str) -> datetime:
    return datetime.fromisoformat(timestamp_value.replace("Z", "+00:00"))


def _datetime_to_iso(timestamp: datetime) -> str:
    return timestamp.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _get_sensor(sensor_id: int, headers: dict) -> dict:
    response = requests.get(f"{OPENAQ_BASE_URL}/sensors/{sensor_id}", headers=headers, timeout=30)
    response.raise_for_status()

    results = response.json().get("results") or []

    if not results:
        raise RuntimeError(f"OpenAQ-Sensor {sensor_id} wurde nicht gefunden.")

    return results[0]


def _get_sensor_hours(sensor_id: int, headers: dict, datetime_from: str, datetime_to: str) -> list[dict]:
    response = requests.get(
        f"{OPENAQ_BASE_URL}/sensors/{sensor_id}/hours",
        headers=headers,
        params={
            "limit": 12,
            "datetime_from": datetime_from,
            "datetime_to": datetime_to,
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json().get("results") or []


def _select_closest_hour(hours: list[dict], target_local: datetime, timezone_name: str):
    timezone_info = ZoneInfo(timezone_name)
    ranked_hours = []

    for hour in hours:
        period = hour.get("period") or {}
        datetime_to = (period.get("datetimeTo") or {}).get("utc")

        if not datetime_to:
            continue

        hour_utc = _iso_to_datetime(datetime_to)
        hour_local = hour_utc.astimezone(timezone_info)
        difference = abs((hour_local - target_local).total_seconds())
        ranked_hours.append((difference, hour_utc, hour_local, hour))

    if not ranked_hours:
        return None

    ranked_hours.sort(key=lambda item: item[0])
    return ranked_hours[0]


def fetch_air_quality_data() -> dict:
    api_key = os.getenv("AIR_QUALITY_API_KEY")

    if not api_key:
        raise RuntimeError(
            "AIR_QUALITY_API_KEY fehlt. Trage deinen OpenAQ-API-Key in die .env ein."
        )

    headers = {"X-API-Key": api_key}
    cities = []

    for city in COMPARISON_CITIES:
        city_schedule = get_city_target_schedule(city["timezone"])
        air_quality_config = city["air_quality"]
        sensor_id = air_quality_config["sensor_id"]
        location_id = air_quality_config["location_id"]
        sensor = _get_sensor(sensor_id, headers)
        parameter = sensor["parameter"]
        series = []
        available_points = 0

        for schedule_entry in city_schedule:
            target_utc = _iso_to_datetime(schedule_entry["target_timestamp_utc"])
            target_local = _iso_to_datetime(schedule_entry["target_timestamp_local"])
            window_start = _datetime_to_iso(target_utc - timedelta(hours=COMPARISON_NOON_WINDOW_HOURS))
            window_end = _datetime_to_iso(target_utc + timedelta(hours=COMPARISON_NOON_WINDOW_HOURS))
            hours = _get_sensor_hours(sensor_id, headers, window_start, window_end)
            selected_hour = _select_closest_hour(hours, target_local, city["timezone"])

            point = {
                "date": schedule_entry["date"],
                "target_timestamp_utc": schedule_entry["target_timestamp_utc"],
                "target_timestamp_local": schedule_entry["target_timestamp_local"],
                "window_start_utc": window_start,
                "window_end_utc": window_end,
                "status": "missing",
                "matched_timestamp_utc": None,
                "matched_timestamp_local": None,
                "value": None,
                "unit": parameter["units"],
                "coverage_percent": None,
            }

            if selected_hour is not None:
                _, matched_utc, matched_local, hour = selected_hour
                point.update(
                    {
                        "status": "available",
                        "matched_timestamp_utc": _datetime_to_iso(matched_utc),
                        "matched_timestamp_local": matched_local.isoformat(),
                        "value": hour["value"],
                        "coverage_percent": hour["coverage"]["percentCoverage"],
                    }
                )
                available_points += 1

            series.append(point)

        status = "available"

        if available_points == 0:
            status = "unavailable"
        elif available_points < COMPARISON_TIMEPOINT_COUNT:
            status = "partial"

        cities.append(
            {
                "city": city["name"],
                "slug": city["slug"],
                "timezone": city["timezone"],
                "status": status,
                "location_id": location_id,
                "location_name": air_quality_config["location_name"],
                "sensor_id": sensor_id,
                "parameter": parameter["name"],
                "unit": parameter["units"],
                "available_timepoint_count": available_points,
                "missing_timepoint_count": COMPARISON_TIMEPOINT_COUNT - available_points,
                "series": series,
            }
        )

    return {
        "source": "openaq_comparison",
        "comparison_mode": "fixed_yearly_local_noon_window",
        "sample_year": COMPARISON_SAMPLE_YEAR,
        "timepoint_count": COMPARISON_TIMEPOINT_COUNT,
        "sample_dates": [sample_date.isoformat() for sample_date in get_sample_dates()],
        "cities": cities,
    }
