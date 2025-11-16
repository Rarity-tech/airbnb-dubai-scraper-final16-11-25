import os
import csv
from pyairbnb import pyairbnb
from datetime import datetime
from time import sleep

# -----------------------------
# Paramètres du scraping
# -----------------------------
NE_LAT = 25.2853
NE_LONG = 55.3657
SW_LAT = 24.8509
SW_LONG = 54.9674
CHECKIN = "2025-12-01"
CHECKOUT = "2025-12-05"
LISTINGS_LIMIT = 50  # Nombre d'annonces à scraper pour test
CSV_PATH = "dubai_listings.csv"
PAUSE_BETWEEN_REQUESTS = 1  # en secondes

# Champs à extraire
CSV_FIELDS = [
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
    "host_total_listings_in_dubai"
]

# -----------------------------
# Chargement des IDs déjà scrappés
# -----------------------------
def load_existing_ids(csv_path):
    if not os.path.exists(csv_path):
        return set()
    with open(csv_path, newline='', encoding='utf-8') as f:
        return {row['room_id'] for row in csv.DictReader(f)}

# -----------------------------
# Scraping principal
# -----------------------------
def scrape_dubai_airbnb():
    client = pyairbnb.PyAirbnb()
    existing_ids = load_existing_ids(CSV_PATH)
    all_results = []

    print("🔍 Recherche d’annonces sur Airbnb Dubaï…")

    try:
        listings = client.search_all(
            check_in=CHECKIN,
            check_out=CHECKOUT,
            ne_lat=NE_LAT,
            ne_long=NE_LONG,
            sw_lat=SW_LAT,
            sw_long=SW_LONG
        )
    except Exception as e:
        print(f"❌ Erreur lors du search_all(): {e}")
        return

    count = 0
    for listing in listings:
        room_id = str(listing.get("id", ""))
        if room_id in existing_ids:
            continue

        try:
            detail = client.get_details(listing_id=room_id)
            host = detail.get("host", {})
            metadata = {
                "room_id": room_id,
                "listing_url": f"https://www.airbnb.com/rooms/{room_id}",
                "listing_title": detail.get("name", ""),
                "license_code": detail.get("license", "") or detail.get("listing_license", ""),
                "host_id": str(host.get("id", "")),
                "host_name": host.get("name", ""),
                "host_profile_url": host.get("host_url", ""),
                "host_rating": host.get("host_rating", ""),
                "host_reviews_count": host.get("host_review_count", ""),
                "host_joined_year": host.get("host_since", "")[:4] if host.get("host_since") else "",
                "host_years_active": str(datetime.now().year - int(host.get("host_since", "")[:4])) if host.get("host_since") else "",
                "host_total_listings_in_dubai": host.get("total_listings_count", "")
            }

            all_results.append(metadata)
            count += 1
            print(f"✅ {count}. {metadata['listing_title']} (ID: {room_id})")

            if count >= LISTINGS_LIMIT:
                break

            sleep(PAUSE_BETWEEN_REQUESTS)
        except Exception as e:
            print(f"⚠️ Erreur sur l'annonce {room_id}: {e}")
            continue

    if all_results:
        file_exists = os.path.exists(CSV_PATH)
        with open(CSV_PATH, "a", newline='', encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=CSV_FIELDS)
            if not file_exists:
                writer.writeheader()
            writer.writerows(all_results)
        print(f"\n📁 Données sauvegardées dans {CSV_PATH}")
    else:
        print("❗ Aucune nouvelle donnée à ajouter.")

# -----------------------------
# Lancement
# -----------------------------
if __name__ == "__main__":
    scrape_dubai_airbnb()
