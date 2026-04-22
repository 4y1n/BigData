from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo


COMPARISON_SAMPLE_YEAR = 2025
COMPARISON_TIMEPOINT_COUNT = 15
COMPARISON_NOON_WINDOW_HOURS = 1

COMPARISON_CITIES = [
    {
        "name": "Vienna",
        "slug": "vienna",
        "timezone": "Europe/Vienna",
        "weather": {"lat": 48.2082, "lon": 16.3738},
        "air_quality": {
            "location_id": 3420,
            "location_name": "Taborstrasse",
            "sensor_id": 30217,
            "parameter": "pm25",
        },
    },
    {
        "name": "New York",
        "slug": "new-york",
        "timezone": "America/New_York",
        "weather": {"lat": 40.7128, "lon": -74.0060},
        "air_quality": {
            "location_id": 928,
            "location_name": "Jersey City FH",
            "sensor_id": 5077566,
            "parameter": "pm25",
        },
    },
    {
        "name": "New Delhi",
        "slug": "new-delhi",
        "timezone": "Asia/Kolkata",
        "weather": {"lat": 28.6139, "lon": 77.2090},
        "air_quality": {
            "location_id": 8118,
            "location_name": "New Delhi",
            "sensor_id": 23534,
            "parameter": "pm25",
        },
    },
]


def _datetime_to_iso(timestamp: datetime) -> str:
    return timestamp.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def get_sample_dates() -> list[date]:
    start = date(COMPARISON_SAMPLE_YEAR, 1, 1)
    interval_days = 364 / (COMPARISON_TIMEPOINT_COUNT - 1)

    return [
        start + timedelta(days=round(index * interval_days))
        for index in range(COMPARISON_TIMEPOINT_COUNT)
    ]


def get_city_target_schedule(city_timezone: str) -> list[dict]:
    timezone_info = ZoneInfo(city_timezone)
    schedule = []

    for sample_date in get_sample_dates():
        target_local = datetime.combine(sample_date, time(12, 0), tzinfo=timezone_info)
        schedule.append(
            {
                "date": sample_date.isoformat(),
                "target_timestamp_local": target_local.isoformat(),
                "target_timestamp_utc": _datetime_to_iso(target_local),
            }
        )

    return schedule
