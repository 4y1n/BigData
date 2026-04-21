import os

import requests
from dotenv import load_dotenv

load_dotenv()

OPENWEATHER_BASE_URL = "https://api.openweathermap.org/data/3.0/onecall"
DEFAULT_USE_MOCK = "true"
DEFAULT_LAT = "48.2082"
DEFAULT_LON = "16.3738"
DEFAULT_UNITS = "metric"
DEFAULT_LANG = "de"
DEFAULT_EXCLUDE = "minutely,alerts"


def _get_coordinate(env_name: str, default_value: str) -> float:
    raw_value = os.getenv(env_name, default_value)

    try:
        return float(raw_value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"{env_name} muss eine gueltige Koordinate sein. Aktueller Wert: {raw_value!r}"
        ) from exc


def _use_mock_weather() -> bool:
    return os.getenv("WEATHER_USE_MOCK", DEFAULT_USE_MOCK).lower() == "true"


def _build_mock_weather_data(lat: float, lon: float, units: str, lang: str, exclude: str) -> dict:
    return {
        "source": "openweathermap_mock",
        "lat": lat,
        "lon": lon,
        "units": units,
        "lang": lang,
        "exclude": [part for part in exclude.split(",") if part] if exclude else [],
        "data": {
            "lat": lat,
            "lon": lon,
            "timezone": "Europe/Vienna",
            "timezone_offset": 7200,
            "current": {
                "temp": 17.8,
                "feels_like": 17.2,
                "humidity": 58,
                "wind_speed": 3.4,
                "weather": [
                    {
                        "id": 803,
                        "main": "Clouds",
                        "description": "aufgelockerte Bewoelkung",
                        "icon": "04d",
                    }
                ],
            },
            "hourly": [
                {"dt": 1760000000, "temp": 17.8, "pop": 0.1},
                {"dt": 1760003600, "temp": 16.9, "pop": 0.2},
                {"dt": 1760007200, "temp": 15.7, "pop": 0.15},
            ],
        },
    }


def fetch_weather_data():
    lat = _get_coordinate("WEATHER_LAT", DEFAULT_LAT)
    lon = _get_coordinate("WEATHER_LON", DEFAULT_LON)
    units = os.getenv("WEATHER_UNITS", DEFAULT_UNITS)
    lang = os.getenv("WEATHER_LANG", DEFAULT_LANG)
    exclude = os.getenv("WEATHER_EXCLUDE", DEFAULT_EXCLUDE)

    if _use_mock_weather():
        return _build_mock_weather_data(lat, lon, units, lang, exclude)

    api_key = os.getenv("WEATHER_API_KEY")

    if not api_key:
        raise RuntimeError(
            "WEATHER_API_KEY fehlt. Trage deinen OpenWeather-API-Key in die .env ein."
        )

    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        "units": units,
        "lang": lang,
    }

    if exclude:
        params["exclude"] = exclude

    # Falls ihr wieder auf den echten Zugang zurueckgehen wollt:
    # response = requests.get(
    #     "https://api.openweathermap.org/data/3.0/onecall",
    #     params={"lat": lat, "lon": lon, "appid": api_key},
    #     timeout=30,
    # )
    try:
        response = requests.get(OPENWEATHER_BASE_URL, params=params, timeout=30)
    except requests.RequestException as exc:
        raise RuntimeError(f"OpenWeather-Anfrage fehlgeschlagen: {exc}") from None

    if response.status_code >= 400:
        raise RuntimeError(
            f"OpenWeather-Abfrage fehlgeschlagen (HTTP {response.status_code}). "
            "Pruefe WEATHER_API_KEY und ob One Call 3.0 fuer deinen Account verfuegbar ist."
        )

    return {
        "source": "openweathermap",
        "lat": lat,
        "lon": lon,
        "units": units,
        "lang": lang,
        "exclude": [part for part in exclude.split(",") if part] if exclude else [],
        "data": response.json(),
    }
