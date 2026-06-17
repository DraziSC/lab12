import os
import json
import requests
from datetime import datetime, timezone

API_URL = "https://opensky-network.org/api/states/all"
OUT_DIR = "data/opensky_raw"

def fetch_and_save():
    os.makedirs(OUT_DIR, exist_ok=True)
    r = requests.get(API_URL, timeout=30)
    r.raise_for_status()
    payload = r.json()
    record = {
        "ingest_ts": datetime.now(timezone.utc).isoformat(),
        "api_ts": payload.get("time"),
        "states": payload.get("states")
    }
    filename = f"opensky_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}.json"
    path = os.path.join(OUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(record, f)
if __name__ == "__main__":
    fetch_and_save()