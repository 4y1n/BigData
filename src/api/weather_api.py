import os
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

from src.comparison_config import (
    COMPARISON_CITIES,
    COMPARISON_SAMPLE_YEAR,
    COMPARISON_TIMEPOINT_COUNT,
    get_city_target_schedule,
    get_sample_dates,
)

load_dotenv()

OPENWEATHER_TIMEMACHINE_BASE_URL = "https://api.openweathermap.org/data/3.0/onecall/timemachine"
DEFAULT_UNITS = "metric"
DEFAULT_LANG = "de"


def _use_mock_weather() -> bool:
    explicit_value = os.getenv("WEATHER_USE_MOCK")

    if explicit_value is not None:
        return explicit_value.lower() == "true"

    return not bool(os.getenv("WEATHER_API_KEY"))


def _iso_to_datetime(timestamp_value: str) -> datetime:
    return datetime.fromisoformat(timestamp_value.replace("Z", "+00:00"))


def _build_mock_weather_data(units: str, lang: str) -> dict:
    cities = []

    for city_index, city in enumerate(COMPARISON_CITIES):
        city_schedule = get_city_target_schedule(city["timezone"])
        series = []
        base_temp = 8.0 + (city_index * 6.0)

        for point_index, schedule_entry in enumerate(city_schedule):
            target_utc = _iso_to_datetime(schedule_entry["target_timestamp_utc"])
            series.append(
                {
                    "date": schedule_entry["date"],
                    "target_timestamp_utc": schedule_entry["target_timestamp_utc"],
                    "target_timestamp_local": schedule_entry["target_timestamp_local"],
                    "status": "available",
                    "data": {
                        "dt": int(target_utc.timestamp()),
                        "temp": round(base_temp + (point_index * 0.4), 1),
                        "feels_like": round(base_temp - 0.8 + (point_index * 0.4), 1),
                        "humidity": 40 + (point_index % 6) * 5,
                        "wind_speed": round(2.0 + city_index + (point_index * 0.1), 1),
                        "weather": [
                            {
                                "id": 801,
                                "main": "Clouds",
                                "description": f"Mock-Wetter fuer {city['name']}",
                                "icon": "02d",
                            }
                        ],
                    },
                }
            )

        cities.append(
            {
                "city": city["name"],
                "slug": city["slug"],
                "timezone": city["timezone"],
                "lat": city["weather"]["lat"],
                "lon": city["weather"]["lon"],
                "status": "mock",
                "available_timepoint_count": COMPARISON_TIMEPOINT_COUNT,
                "missing_timepoint_count": 0,
                "series": series,
            }
        )

    return {
        "source": "openweathermap_comparison_mock",
        "comparison_mode": "fixed_yearly_local_noon",
        "request_type": "timemachine",
        "sample_year": COMPARISON_SAMPLE_YEAR,
        "timepoint_count": COMPARISON_TIMEPOINT_COUNT,
        "sample_dates": [sample_date.isoformat() for sample_date in get_sample_dates()],
        "units": units,
        "lang": lang,
        "cities": cities,
    }


def _fetch_weather_snapshot(
    api_key: str,
    lat: float,
    lon: float,
    units: str,
    lang: str,
    timestamp_utc: str,
) -> dict | None:
    params = {
        "lat": lat,
        "lon": lon,
        "dt": int(_iso_to_datetime(timestamp_utc).timestamp()),
        "appid": api_key,
        "units": units,
        "lang": lang,
    }

    try:
        response = requests.get(OPENWEATHER_TIMEMACHINE_BASE_URL, params=params, timeout=30)
    except requests.RequestException as exc:
        raise RuntimeError(f"OpenWeather-Anfrage fehlgeschlagen: {exc}") from None

    if response.status_code >= 400:
        raise RuntimeError(
            f"OpenWeather-Abfrage fehlgeschlagen (HTTP {response.status_code}). "
            "Pruefe WEATHER_API_KEY und ob One Call 3.0 fuer deinen Account verfuegbar ist."
        )

    data_points = response.json().get("data") or []
    return data_points[0] if data_points else None


def fetch_weather_data() -> dict:
    units = os.getenv("WEATHER_UNITS", DEFAULT_UNITS)
    lang = os.getenv("WEATHER_LANG", DEFAULT_LANG)

    if _use_mock_weather():
        return _build_mock_weather_data(units, lang)

    api_key = os.getenv("WEATHER_API_KEY")

    if not api_key:
        raise RuntimeError(
            "WEATHER_API_KEY fehlt. Trage deinen OpenWeather-API-Key in die .env ein "
            "oder setze WEATHER_USE_MOCK=true fuer Testdaten."
        )

    cities = []

    for city in COMPARISON_CITIES:
        city_schedule = get_city_target_schedule(city["timezone"])
        series = []
        available_points = 0

        for schedule_entry in city_schedule:
            data = _fetch_weather_snapshot(
                api_key=api_key,
                lat=city["weather"]["lat"],
                lon=city["weather"]["lon"],
                units=units,
                lang=lang,
                timestamp_utc=schedule_entry["target_timestamp_utc"],
            )
            point = {
                "date": schedule_entry["date"],
                "target_timestamp_utc": schedule_entry["target_timestamp_utc"],
                "target_timestamp_local": schedule_entry["target_timestamp_local"],
                "status": "missing",
                "data": None,
            }

            if data is not None:
                point["status"] = "available"
                point["data"] = data
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
                "lat": city["weather"]["lat"],
                "lon": city["weather"]["lon"],
                "status": status,
                "available_timepoint_count": available_points,
                "missing_timepoint_count": COMPARISON_TIMEPOINT_COUNT - available_points,
                "series": series,
            }
        )

    return {
        "source": "openweathermap_comparison",
        "comparison_mode": "fixed_yearly_local_noon",
        "request_type": "timemachine",
        "sample_year": COMPARISON_SAMPLE_YEAR,
        "timepoint_count": COMPARISON_TIMEPOINT_COUNT,
        "sample_dates": [sample_date.isoformat() for sample_date in get_sample_dates()],
        "units": units,
        "lang": lang,
        "cities": cities,
    }
