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

```bash
pip install -r requirements.txt
__________________________________________________
Genauere Anweisungen zur Einrichtung:

big-data-weather-airpollution

Projekt zum Sammeln, Speichern und späteren Auswerten von Wetter- und Luftqualitätsdaten.

--------------------------------------------------

PROJEKTÜBERBLICK

Dieses Projekt:

1. Holt Daten aus APIs (Wetter + Luftqualität)
2. Speichert rohe Daten:
   - lokal als JSON
   - in MongoDB
3. Bereitet Daten später für Analyse auf

--------------------------------------------------

PROJEKTSTRUKTUR

big-data-weather-airpollution/

├── data/
│   ├── raw/
│   │   ├── weather/
│   │   └── air_quality/
│   └── processed/
│
├── src/
│   ├── api/
│   ├── db/
│   ├── storage/
│   └── main.py
│
├── .env
├── .gitignore
├── requirements.txt
└── README.md

--------------------------------------------------

VORAUSSETZUNGEN

Bitte vorher installieren:

- Python 3.10 oder neuer
- Docker Desktop
- Git
- optional: DataGrip / DataSpell

--------------------------------------------------

PYTHON SETUP

1. Projekt klonen

git clone <REPO_URL>
cd big-data-weather-airpollution

--------------------------------------------------

2. Virtuelle Umgebung erstellen

Linux / Mac:

python3 -m venv .venv
source .venv/bin/activate

Windows (PowerShell):

python -m venv .venv
.venv\Scripts\Activate.ps1

--------------------------------------------------

3. Abhängigkeiten installieren

pip install -r requirements.txt

requirements.txt:

requests
pymongo
python-dotenv

--------------------------------------------------

MONGODB MIT DOCKER STARTEN

1. Container starten

Windows PowerShell:

docker run -d ^
  --name mongodb ^
  -p 27017:27017 ^
  -v mongodb_data:/data/db ^
  mongo:7

Linux / Mac:

docker run -d \
  --name mongodb \
  -p 27017:27017 \
  -v mongodb_data:/data/db \
  mongo:7

--------------------------------------------------

2. Prüfen ob Mongo läuft

docker ps

Es sollte ein Container namens "mongodb" laufen.

--------------------------------------------------

ENVIRONMENT-DATEI

Datei ".env" im Projekt-Root erstellen:

MONGO_URI=mongodb://localhost:27017/
MONGO_DB=big_data_weather_airpollution

WEATHER_API_KEY=
AIR_QUALITY_API_KEY=

--------------------------------------------------

PROJEKT STARTEN

python -m src.main

--------------------------------------------------

ERWARTETES ERGEBNIS

Nach dem Start:

Lokal:
- Dateien entstehen in:
  data/raw/weather/
  data/raw/air_quality/

MongoDB:
- Datenbank: big_data_weather_airpollution
- Collections:
  weather_raw
  air_quality_raw

--------------------------------------------------

MONGODB MIT DATAGRIP VERBINDEN

1. Neue Datenquelle erstellen:
   + -> MongoDB

2. Connection String eintragen:

mongodb://localhost:27017

3. Falls notwendig:
   "Download missing driver files" klicken

4. Test Connection:
   sollte "Success" anzeigen

5. Daten ansehen:
   Datenbank: big_data_weather_airpollution
   Collections: weather_raw, air_quality_raw

--------------------------------------------------

ARCHITEKTUR

api/       -> Holt Daten von APIs
storage/   -> Speichert Daten (JSON + Mongo)
db/        -> MongoDB Verbindung
main.py    -> Startpunkt

--------------------------------------------------

WORKFLOW

1. API wird aufgerufen
2. Daten werden geholt
3. Daten werden gespeichert:
   - JSON (raw)
   - MongoDB
4. später: Analyse

--------------------------------------------------

TYPISCHE PROBLEME

Mongo läuft nicht:
docker ps

Verbindung verweigert:
Mongo-Container läuft nicht

DataGrip zeigt nichts:
Rechtsklick auf Verbindung -> Refresh

Python Import Fehler:

Falls nötig folgende Dateien anlegen:

src/__init__.py
src/api/__init__.py
src/db/__init__.py
src/storage/__init__.py

--------------------------------------------------

HINWEISE FÜR WINDOWS

- PowerShell verwenden
- Docker Desktop muss laufen
- ggf. Script-Ausführung erlauben:

Set-ExecutionPolicy RemoteSigned -Scope CurrentUser

--------------------------------------------------

STATUS

[x] Projektstruktur
[x] MongoDB Setup
[x] JSON Speicherung
[x] Mongo Speicherung
[ ] APIs integrieren
[ ] Datenanalyse