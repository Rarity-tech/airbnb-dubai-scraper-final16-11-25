import os
import csv
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any

import pyairbnb

# ========================
# Configuration utilisateur
# ========================
LISTINGS_PER_RUN = int(os.getenv("LISTINGS_PER_RUN", "50"))
CHECK_IN_DAYS_AHEAD = int(os.getenv("CHECK_IN_DAYS_AHEAD", "14"))
STAY_NIGHTS = int(os.getenv("STAY_NIGHTS", "5"))

CSV_PATH = "dubai_hosts.csv"
DUBAI_BBOX = {
    "lat_min": 24.85,
    "lat_max": 25.35,
    "lon_min": 54.95,
    "lon_max": 55.45,
}
GRID_ROWS = 3
GRID_COLS = 4

CSV_COLUMNS = [
    "room_id",
    "listing_url",
    "listing_title",
    "license_code",
    "host_id",
    "host_name",
    "host_profile_url",
    "host_rating",
    "host_reviews_count",
    "host_joined_year",
    "host_years_active",
    "host_total_listings_in_dubai",
]

# ========================
# Outils internes
# ========================

def safe_get(obj: Any, path: str, default: Any = "") -> Any:
    try:
        for key in path.split("."):
            if isinstance(obj, dict):
                obj = obj.get(key, default)
            else:
                return default
        return obj
    except Exception:
        return default

def log(msg: str):
    print(msg, flush=True)

def ensure_csv_header():
    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()

def read_existing_ids() -> set:
    existing = set()
    if not os.path.exists(CSV_PATH):
        return existing
    with open(CSV_PATH, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("room_id"):
                existing.add(row["room_id"])
    return existing

def build_zones() -> List[Dict[str, float]]:
    zones = []
    lat_step = (DUBAI_BBOX["lat_max"] - DUBAI_BBOX["lat_min"]) / GRID_ROWS
    lon_step = (DUBAI_BBOX["lon_max"] - DUBAI_BBOX["lon_min"]) / GRID_COLS
    for i in range(GRID_ROWS):
        for j in range(GRID_COLS):
            zones.append({
                "sw": {
                    "latitude": DUBAI_BBOX["lat_min"] + i * lat_step,
                    "longitude": DUBAI_BBOX["lon_min"] + j * lon_step,
                },
                "ne": {
                    "latitude": DUBAI_BBOX["lat_min"] + (i + 1) * lat_step,
                    "longitude": DUBAI_BBOX["lon_min"] + (j + 1) * lon_step,
                }
            })
    return zones

# ========================
# Phase unique: Recherche + Détails limités
# ========================

def scrape_dubai_listings():
    today = datetime.utcnow().date()
    checkin = (today + timedelta(days=CHECK_IN_DAYS_AHEAD)).isoformat()
    checkout = (today + timedelta(days=CHECK_IN_DAYS_AHEAD + STAY_NIGHTS)).isoformat()
    zones = build_zones()
    collected = []
    existing_ids = read_existing_ids()

    log("📦 Démarrage du scraping de Dubaï...")
    ensure_csv_header()

    for zone in zones:
        try:
            results = pyairbnb.search_all(
                check_in=checkin,
                check_out=checkout,
                map_bounds={"ne": zone["ne"], "sw": zone["sw"]},
                refinement_paths=["/homes"],
                selected_tab_id="home_tab",
                search_type="PAGINATION",
                proxy_url=""
            )
        except Exception as e:
            log(f"⚠️ Erreur de recherche zone: {e}")
            continue

        for item in results:
            room_id = str(item.get("id"))
            if room_id in existing_ids:
                continue

            try:
                details = pyairbnb.get_details(room_id=room_id)
                listing = safe_get(details, "listing", {})
                host = safe_get(listing, "primaryHost", {})

                row = {
                    "room_id": room_id,
                    "listing_url": f"https://www.airbnb.com/rooms/{room_id}",
                    "listing_title": safe_get(listing, "name"),
                    "license_code": safe_get(listing, "license"),
                    "host_id": safe_get(host, "id"),
                    "host_name": safe_get(host, "hostName") or safe_get(host, "name"),
                    "host_profile_url": f"https://www.airbnb.com/users/show/{safe_get(host, 'id')}",
                    "host_rating": safe_get(host, "profileRating"),
                    "host_reviews_count": safe_get(host, "reviewCount"),
                    "host_joined_year": safe_get(host, "createdAt", "")[:4],
                    "host_years_active": str(datetime.utcnow().year - int(safe_get(host, "createdAt", "2000")[:4])),
                    "host_total_listings_in_dubai": safe_get(host, "listingCount"),
                }
                collected.append(row)
                if len(collected) >= LISTINGS_PER_RUN:
                    break
            except Exception as e:
                log(f"   ⚠️ Erreur sur room_id={room_id}: {e}")
        if len(collected) >= LISTINGS_PER_RUN:
            break

    if not collected:
        log("❌ Aucun listing nouveau collecté.")
        return

    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        for row in collected:
            writer.writerow(row)

    log(f"✅ {len(collected)} nouveaux listings ajoutés dans {CSV_PATH}.")

if __name__ == "__main__":
    scrape_dubai_listings()
