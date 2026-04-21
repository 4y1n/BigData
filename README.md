# big-data-weather-airpollution

Projekt zum Sammeln, Speichern und späteren Auswerten von Wetter- und Luftqualitätsdaten.

## Struktur

- `data/raw/`: rohe JSON-Daten aus APIs
- `data/processed/`: später bereinigte/verarbeitete Daten
- `src/api/`: API-Zugriffe
- `src/db/`: MongoDB-Verbindung
- `src/storage/`: Speichern von JSON und MongoDB-Inserts
- `src/main.py`: Einstiegspunkt

## Setup

### Voraussetzungen

- Python 3.10 oder neuer
- Docker bzw. Docker Desktop
- Git
- optional: DataGrip / DataSpell

### Python vorbereiten

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Unter Windows:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Environment-Datei anlegen

Die Projektdatei `.env` ist absichtlich nicht versioniert. Lege sie aus der Vorlage an:

```bash
cp .env.example .env
```

Inhalt der Vorlage:

```env
MONGO_URI=mongodb://localhost:27017/
MONGO_DB=big_data_weather_airpollution

WEATHER_API_KEY=
WEATHER_USE_MOCK=true
WEATHER_LAT=48.2082
WEATHER_LON=16.3738
WEATHER_UNITS=metric
WEATHER_LANG=de
WEATHER_EXCLUDE=minutely,alerts
AIR_QUALITY_API_KEY=
OPENAQ_LOCATION_ID=8118
```

Für die Wetterdaten wird OpenWeather One Call 3.0 verwendet. Solange die Freischaltung fehlt, ist standardmaessig `WEATHER_USE_MOCK=true` aktiv und es werden kleine Testdaten fuer Wien geliefert. Sobald der echte Zugang verfuegbar ist, setze `WEATHER_USE_MOCK=false` und trage deinen OpenWeather-Key in `WEATHER_API_KEY` ein. Koordinaten und Ausgabe kannst du ueber `WEATHER_LAT`, `WEATHER_LON`, `WEATHER_UNITS`, `WEATHER_LANG` und `WEATHER_EXCLUDE` anpassen.

Für die Luftqualitätsdaten wird OpenAQ v3 verwendet. Trage dafür deinen OpenAQ-API-Key in `AIR_QUALITY_API_KEY` ein. Standardmäßig wird die Location `8118` abgefragt; bei Bedarf kannst du sie über `OPENAQ_LOCATION_ID` ändern.

### MongoDB mit Docker starten

Im Repository liegt jetzt eine `docker-compose.yml`, die MongoDB mit persistentem Volume startet:

```bash
docker compose up -d mongodb
```

Status prüfen:

```bash
docker compose ps
```

MongoDB lauscht danach standardmäßig auf `localhost:27017`.

### Projekt starten

```bash
python -m src.main
```

## Erwartetes Ergebnis

Nach dem Start:

- lokale JSON-Dateien in `data/raw/weather/` und `data/raw/air_quality/`
- MongoDB-Datenbank `big_data_weather_airpollution`
- Collections `weather_raw` und `air_quality_raw`

Wenn MongoDB nicht läuft, bricht die Anwendung jetzt mit einer klaren Fehlermeldung ab und verweist auf:

```bash
docker compose up -d mongodb
```

## MongoDB mit DataGrip verbinden

Connection String:

```text
mongodb://localhost:27017
```

Danach die Datenbank `big_data_weather_airpollution` und die Collections `weather_raw` sowie `air_quality_raw` öffnen.
