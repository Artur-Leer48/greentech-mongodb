"""
GreenTech GmbH – IoT Sensor Data Import & Queries
===================================================
Lernfeld 8 – LS 8.5 | Fachinformatiker Anwendungsentwicklung

Aufgabe: Dieses Skript ist vorbereitet, aber noch nicht vollständig.
Ihr müsst:
  1. Die richtige Verbindung aktivieren (Atlas ODER Docker – nur eine!)
  2. Den Pfad zur JSON-Datei prüfen
  3. Das Skript ausführen und die Ausgabe dokumentieren
  4. Mindestens zwei Aggregationen ausführen (Abschnitt 3)
  5. Die sensors-Collection befüllen (Abschnitt 4)
"""

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, BulkWriteError
from datetime import datetime
from dotenv import load_dotenv
import json
import os

load_dotenv()

CONNECTION_STRING = os.getenv("MONGODB_URI")
if not CONNECTION_STRING:
    raise RuntimeError("MONGODB_URI fehlt in der .env")






# ===========================================================================
# ABSCHNITT 0 – Verbindung konfigurieren
# ===========================================================================
# Aktiviert GENAU EINE der beiden Optionen (die andere auskommentiert lassen).

# --- Option A: MongoDB Atlas (Free Tier) ------------------------------------
# Euren Connection String aus Atlas kopieren:
#   Atlas UI -> Cluster -> Connect -> Drivers -> Python
# Format: mongodb+srv://<user>:<passwort>@<cluster>.mongodb.net/
#
# ACHTUNG: Passwörter mit Sonderzeichen müssen URL-kodiert werden,
#          z.B. @ -> %40, # -> %23


# --- Option B: Lokales Docker -----------------------------------------------
# Voraussetzung: Docker läuft und der Container wurde gestartet mit:
#   docker run -d -p 27017:27017 --name greentech-mongo mongo:latest
#
# Dann diese Zeile aktivieren und Option A auskommentieren:

# CONNECTION_STRING = "mongodb://localhost:27017/"

# ===========================================================================
# ABSCHNITT 1 – Verbindung herstellen und Datenbank wählen
# ===========================================================================

DB_NAME         = "greentech_poc"
COLLECTION_NAME = "sensordaten"

def verbinden():
    """Stellt die Verbindung her und gibt client, db, collection zurück."""
    try:
        client = MongoClient(CONNECTION_STRING, serverSelectionTimeoutMS=5000)
        # Verbindung aktiv testen
        client.admin.command("ping")
        print(f"Verbindung erfolgreich: {CONNECTION_STRING[:40]}...")
        db         = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        return client, db, collection
    except ConnectionFailure as e:
        print(f"Verbindung fehlgeschlagen: {e}")
        raise


# ===========================================================================
# ABSCHNITT 2 – Daten importieren
# ===========================================================================
# Die JSON-Dateien liegen im selben Ordner wie dieses Skript.
# Passe den Pfad an, falls ihr sie woanders gespeichert habt.

JSON_DATEIEN = [
    "sensordaten_minified.json",    # T-100, CO2-200, L-300  (120 Dokumente)
    "sensordaten_erweitert.json",   # TH-500, EV-900          (53 Dokumente)
]

def importieren(collection):
    """Liest alle JSON-Dateien und importiert die Dokumente."""
    gesamt = 0
    for dateiname in JSON_DATEIEN:
        pfad = os.path.join(os.path.dirname(__file__), dateiname)
        if not os.path.exists(pfad):
            print(f"  FEHLER: Datei nicht gefunden: {pfad}")
            continue
        with open(pfad, encoding="utf-8") as f:
            dokumente = json.load(f)
        try:
            ergebnis = collection.insert_many(dokumente, ordered=False)
            anzahl   = len(ergebnis.inserted_ids)
            gesamt  += anzahl
            print(f"  {dateiname}: {anzahl} Dokumente importiert")
        except BulkWriteError as e:
            # Tritt auf wenn Dokumente mit gleicher _id schon vorhanden sind
            ok = e.details.get("nInserted", 0)
            gesamt += ok
            print(f"  {dateiname}: {ok} importiert ({len(e.details['writeErrors'])} Duplikate übersprungen)")
    print(f"\nGesamt importiert: {gesamt} Dokumente in '{DB_NAME}.{COLLECTION_NAME}'")
    return gesamt


# ===========================================================================
# ABSCHNITT 3 – Abfragen und Aggregationen
# ===========================================================================
# Führt mindestens ZWEI der folgenden Abfragen aus.
# Dokumentiert die Ausgabe mit einem Screenshot.

def abfragen(collection):

    print("\n--- Abfrage 1: Alle Messwerte von Sensor T-100 ---")
    ergebnisse = list(collection.find({"sensor_id": "T-100"}, {"_id": 0}))
    print(f"  Anzahl Dokumente: {len(ergebnisse)}")
    if ergebnisse:
        print(f"  Beispiel: {ergebnisse[0]}")

    print("\n--- Abfrage 2: Zeitfenster 08:00 bis 09:00 Uhr ---")
    ergebnisse = list(collection.find(
        {"timestamp": {"$gte": "2025-05-07 08:00", "$lte": "2025-05-07 09:00"}},
        {"_id": 0, "sensor_id": 1, "timestamp": 1}
    ))
    print(f"  Anzahl Dokumente: {len(ergebnisse)}")

    print("\n--- Abfrage 3: Nur Dokumente mit Feld 'humidity' (Sensor TH-500) ---")
    ergebnisse = list(collection.find(
        {"readings.humidity": {"$exists": True}},
        {"_id": 0, "sensor_id": 1, "timestamp": 1, "readings": 1}
    ))
    print(f"  Anzahl Dokumente: {len(ergebnisse)}")
    if ergebnisse:
        print(f"  Beispiel: {ergebnisse[0]}")

    print("\n--- Abfrage 4: Alle irrigation_start-Ereignisse (EV-900) ---")
    ergebnisse = list(collection.find(
        {"sensor_id": "EV-900", "event_type": "irrigation_start"},
        {"_id": 0}
    ))
    print(f"  Anzahl Dokumente: {len(ergebnisse)}")
    for e in ergebnisse:
        print(f"    {e}")


def aggregationen(collection):

    # -----------------------------------------------------------------------
    # Aggregation 1: Stundenmittelwert Temperatur (T-100)
    # Hinweis: timestamp liegt als String vor, daher $substr zum Extrahieren
    # der Stunde aus "2025-05-07 08:35"
    # -----------------------------------------------------------------------
    print("\n--- Aggregation 1: Stundenmittelwert T-100 ---")
    pipeline_mittelwert = [
        {"$match": {"sensor_id": "T-100"}},
        {"$addFields": {
            "stunde": {"$substr": ["$timestamp", 11, 2]}   # Zeichen 11-12 = Stunde
        }},
        {"$group": {
            "_id":       "$stunde",
            "avg_temp":  {"$avg": "$value"},
            "min_temp":  {"$min": "$value"},
            "max_temp":  {"$max": "$value"},
            "anzahl":    {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]
    for row in collection.aggregate(pipeline_mittelwert):
        print(f"  {row['_id']}:00 Uhr  |  "
              f"Ø {row['avg_temp']:.2f} °C  |  "
              f"min {row['min_temp']:.2f}  |  "
              f"max {row['max_temp']:.2f}  |  "
              f"n={row['anzahl']}")

    # -----------------------------------------------------------------------
    # Aggregation 2: Schwellenwert-Alarm (Temperatur > 22.5 °C oder Feuchte > 70 %)
    # -----------------------------------------------------------------------
    print("\n--- Aggregation 2: Schwellenwert-Alarm ---")
    pipeline_alarm = [
        {"$match": {
            "$or": [
                {"value":                    {"$gt": 22.5}, "sensor_id": "T-100"},
                {"readings.temperature.value": {"$gt": 22.5}},
                {"readings.humidity.value":    {"$gt": 70.0}},
            ]
        }},
        {"$project": {
            "_id": 0,
            "timestamp": 1,
            "sensor_id": 1,
            "value": 1,
            "readings": 1,
        }},
        {"$sort": {"timestamp": 1}}
    ]
    alarme = list(collection.aggregate(pipeline_alarm))
    print(f"  Alarm-Ereignisse: {len(alarme)}")
    for a in alarme[:5]:   # max. 5 Zeilen ausgeben
        print(f"    {a}")
    if len(alarme) > 5:
        print(f"    ... ({len(alarme) - 5} weitere)")

    # -----------------------------------------------------------------------
    # Aggregation 3: Ereignis-Zählung EV-900
    # -----------------------------------------------------------------------
    print("\n--- Aggregation 3: Ereignis-Zählung EV-900 ---")
    pipeline_events = [
        {"$match": {"sensor_id": "EV-900"}},
        {"$group": {
            "_id":   "$event_type",
            "count": {"$sum": 1}
        }},
        {"$sort": {"count": -1}}
    ]
    for row in collection.aggregate(pipeline_events):
        print(f"  {row['_id']:<25} {row['count']}x")

    # -----------------------------------------------------------------------
    # Aggregation 4: Min/Max je Sensor
    # -----------------------------------------------------------------------
    print("\n--- Aggregation 4: Wertebereich je Sensor ---")
    pipeline_range = [
        {"$match": {"value": {"$exists": True}}},
        {"$group": {
            "_id":   "$sensor_id",
            "min":   {"$min": "$value"},
            "max":   {"$max": "$value"},
            "avg":   {"$avg": "$value"},
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]
    for row in collection.aggregate(pipeline_range):
        print(f"  {row['_id']:<10}  min={row['min']:.2f}  "
              f"max={row['max']:.2f}  avg={row['avg']:.2f}  n={row['count']}")


# ===========================================================================
# ABSCHNITT 4 – Zweite Collection: sensors (Stammdaten)
# ===========================================================================
# A5: Befüllt die Collection 'sensors' mit Stammdaten für alle 5 Sensortypen.
# Ergänzt die Felder location und thresholds sinnvoll für euer Szenario.

SENSOR_STAMMDATEN = [
    {
        "sensor_id":  "T-100",
        "type":       "temperature",
        "location":   "Sektion A",
        "unit":       "°C",
        "thresholds": {"min": 15.0, "max": 30.0},
        "active":     True,
    },
    {
        "sensor_id":  "CO2-200",
        "type":       "co2",
        "location":   "Sektion A",
        "unit":       "ppm",
        "thresholds": {"min": 300.0, "max": 1000.0},
        "active":     True,
    },
    {
        "sensor_id":  "L-300",
        "type":       "light",
        "location":   "Dach",
        "unit":       "lux",
        "thresholds": {"min": 50.0, "max": 200.0},
        "active":     True,
    },
    {
        "sensor_id":  "TH-500",
        "type":       "temperature_humidity",
        "location":   "Sektion B",
        "thresholds": {
            "temperature": {"min": 15.0, "max": 30.0},
            "humidity":    {"min": 40.0, "max": 75.0},
        },
        "active":     True,
    },
    {
        "sensor_id":  "EV-900",
        "type":       "event",
        "location":   "Steuereinheit",
        "thresholds": None,   # Ereignissensor hat keine Schwellenwerte
        "active":     True,
    },
]

def sensors_collection_befuellen(db):
    """Legt die Collection 'sensors' an und befüllt sie mit Stammdaten."""
    sensors = db["sensors"]
    sensors.drop()   # Vorher leeren um Duplikate zu vermeiden
    ergebnis = sensors.insert_many(SENSOR_STAMMDATEN)
    print(f"\n--- Abschnitt 4: sensors-Collection ---")
    print(f"  {len(ergebnis.inserted_ids)} Stammdatensätze eingefügt")

    # $lookup: Messdaten mit Stammdaten zusammenführen
    # Zeigt die ersten 3 T-100-Messwerte mit den Sensordaten verknüpft
    print("\n--- $lookup: T-100 Messwerte + Stammdaten ---")
    pipeline_lookup = [
        {"$match":  {"sensor_id": "T-100"}},
        {"$limit":  3},
        {"$lookup": {
            "from":         "sensors",
            "localField":   "sensor_id",
            "foreignField": "sensor_id",
            "as":           "stammdaten"
        }},
        {"$project": {
            "_id": 0,
            "timestamp": 1,
            "value": 1,
            "unit": 1,
            "sensor_info": {"$arrayElemAt": ["$stammdaten", 0]}
        }}
    ]
    for doc in db["sensordaten"].aggregate(pipeline_lookup):
        print(f"  {doc['timestamp']}  {doc['value']} {doc.get('unit','')}  "
              f"-> Standort: {doc['sensor_info']['location']}  "
              f"| Schwellenwert max: {doc['sensor_info']['thresholds']['max']}")


# ===========================================================================
# HAUPTPROGRAMM
# ===========================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("GreenTech GmbH – Sensordaten Import & Auswertung")
    print("=" * 60)

    client, db, collection = verbinden()

    # --- Schritt 1: Importieren ---
    # Kommentiert diese Zeile aus, wenn ihr die Daten bereits importiert habt
    # und nur die Abfragen testen wollt:
    importieren(collection)

    # --- Schritt 2: Einfache Abfragen ---
    abfragen(collection)

    # --- Schritt 3: Aggregationen (mindestens 2 ausführen) ---
    aggregationen(collection)

    # --- Schritt 4: sensors-Collection + $lookup ---
    sensors_collection_befuellen(db)

    client.close()
    print("\nVerbindung geschlossen. Fertig.")
