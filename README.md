# big-data-weather-airpollution

Projekt zum Sammeln, Zwischenspeichern, Speichern und Auswerten historischer Wetter- und Luftqualitätsdaten.

## Projektziel

Das Projekt vergleicht Wetter und Luftqualität für eine feste Jahresstichprobe:

- **5 Städte**: Wien, New York, Neu Delhi, Phoenix, Reykjavik
- **15 Zeitpunkte** pro Stadt
- **Jahr 2025**
- Zielzeitpunkt: **12:00 Uhr Ortszeit**

Wetterdaten kommen von **OpenWeather Time Machine**, Luftqualitätsdaten von **OpenAQ v3** mit **PM2.5**.

## Aktuelle Struktur

| Pfad | Zweck |
| --- | --- |
| `data/raw/` | gemeinsame RAW-JSON-Snapshots als Cache |
| `data/processed/` | erzeugte MapReduce-Ergebnisse |
| `src/api/` | API-Zugriffe für Wetter und Luftqualität |
| `src/db/` | MongoDB-Verbindung |
| `src/storage/` | RAW-Cache- und Mongo-Helfer |
| `src/MapReduce.py` | explizite MapReduce-Pipeline |
| `src/comparison_config.py` | Städte, Sensoren, Zeitplan |
| `src/main.py` | Einstiegspunkt für Cache-Sync oder Refresh |
| `Jypiter Notebook - BigData Project.ipynb` | Dokumentation, Tabellen, Visualisierung |

## Setup

### Voraussetzungen

- Python 3.10 oder neuer
- Docker bzw. Docker Desktop
- Git
- optional: DataSpell / DataGrip / Jupyter

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

```bash
cp .env.example .env
```

Aktuelle Vorlage:

```env
MONGO_URI=mongodb://localhost:27017/
MONGO_DB=big_data_weather_airpollution

WEATHER_API_KEY=
WEATHER_USE_MOCK=
WEATHER_UNITS=metric
WEATHER_LANG=de
AIR_QUALITY_API_KEY=
```

Hinweise:

- `WEATHER_API_KEY`: OpenWeather API-Key
- `AIR_QUALITY_API_KEY`: OpenAQ API-Key
- `WEATHER_USE_MOCK=true`: erzeugt Mock-Wetterdaten für alle konfigurierten Städte
- `WEATHER_UNITS=metric`: Temperaturen in Celsius, Wind in m/s
- `WEATHER_LANG=de`: deutsche Wetterbeschreibungen

Die Städte, Koordinaten und OpenAQ-Sensoren werden **nicht** über `.env`, sondern zentral in `src/comparison_config.py` gepflegt.

### MongoDB starten

```bash
docker compose up -d mongodb
```

Status prüfen:

```bash
docker compose ps
```

MongoDB läuft danach standardmäßig unter:

```text
mongodb://localhost:27017
```

## Workflow: cache-first statt unnötiger API-Abfragen

Das Projekt ist jetzt bewusst auf einen **Cache-First-Workflow** ausgelegt, damit historische Daten nicht immer wieder neu abgefragt werden.

### Standardfall

```bash
python -m src.main
```

Dieses Kommando:

- verwendet vorhandene RAW-Dateien aus `data/raw/weather/` und `data/raw/air_quality/`
- schreibt diese bei Bedarf nach MongoDB
- erzeugt **keine neuen API-Abfragen**, solange ein RAW-Snapshot vorhanden ist

### Nur wenn wirklich neue Daten gebraucht werden

```bash
python -m src.main --refresh
```

Dieses Kommando:

- ruft Wetter- und Luftqualitätsdaten erneut über die APIs ab
- speichert einen neuen Snapshot in `data/raw/...`
- schreibt den neuen Stand nach MongoDB

**Wichtig:** Wegen des API-Limits sollte `--refresh` nur bewusst und nur bei echtem Bedarf verwendet werden.

## Gemeinsame Arbeit im Team

Die Ordner unter `data/raw/` sind der gemeinsame Arbeitscache.

Empfohlener Ablauf:

1. Eine Person führt bei Bedarf einen bewussten Refresh aus.
2. Das neue zusammengehörige Snapshot-Paar wird versioniert:
   - `data/raw/weather/...`
   - `data/raw/air_quality/...`
3. Alle anderen arbeiten mit genau diesen RAW-Dateien weiter.
4. `python -m src.main` synchronisiert den RAW-Stand lokal nach MongoDB, ohne neue API-Abfragen zu erzeugen.

Für Git sollte in der Regel **nur das neueste passende Wetter-/Luftqualitäts-Paar** committed werden, nicht alle historischen Dumps.

## Datenspeicherung

Nach einem Lauf stehen die Daten an zwei Stellen:

1. **RAW-JSON-Dateien**
   - `data/raw/weather/`
   - `data/raw/air_quality/`

2. **MongoDB**
   - Datenbank: `big_data_weather_airpollution`
   - Collections:
     - `weather_raw`
     - `air_quality_raw`

Wenn bereits identische Daten in MongoDB vorhanden sind, wird kein doppeltes Dokument geschrieben.

## MapReduce

Die sichtbare MapReduce-Implementierung liegt in:

```text
src/MapReduce.py
```

Die Pipeline:

1. lädt RAW-Daten bevorzugt aus `data/raw/...`
2. fällt bei Bedarf auf MongoDB zurück
3. flacht Wetter- und Luftqualitätsdaten in gemeinsame Records ab
4. ergänzt fehlende Werte
5. reduziert auf Stadt-Zusammenfassungen
6. speichert das Ergebnis in `data/processed/`

### Aktuelle Vervollständigungsregeln

- **Temperatur**: Durchschnitt aus vorherigem und nächstem Zeitpunkt
- **Luftqualität (PM2.5)**: Durchschnitt aus vorherigem und nächstem Zeitpunkt
- **Windgeschwindigkeit**: Jahresdurchschnitt der jeweiligen Stadt
- **Windrichtung**: wird nicht imputiert

### MapReduce-Ergebnis

Die Pipeline erzeugt JSON-Dateien in:

```text
data/processed/
```

Zusätzlich werden Statistiken gespeichert, z. B.:

- Anzahl der Rohdatensätze
- Anzahl der Städte
- fehlende Werte vor der Verarbeitung
- Anzahl imputierter Werte
- vollständige Temperatur-/Luftqualitäts-Paare vor und nach der Verarbeitung

## Notebook

Das Jupyter-Notebook dokumentiert den Projektablauf:

- Setup und Environment
- MongoDB-Verbindung
- sicherer Start über `src.main`
- RAW-Tabellen
- sichtbare MapReduce-Ausführung
- verarbeitete Tabellen
- Visualisierung von Temperatur, PM2.5 und Wind

Im Notebook ist beim `main`-Schritt jetzt ausdrücklich vermerkt:

> **Achtung: Datenrefresh nur wenn dringend erforderlich!**

## Projektstart in Kurzform

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
docker compose up -d mongodb
python -m src.main
```

Danach kann das Notebook geöffnet werden, ohne zusätzliche API-Abfragen zu produzieren, solange mit dem vorhandenen RAW-Cache gearbeitet wird.

## MongoDB mit DataGrip verbinden

Connection String:

```text
mongodb://localhost:27017
```

Danach die Datenbank `big_data_weather_airpollution` und die Collections `weather_raw` sowie `air_quality_raw` öffnen.
