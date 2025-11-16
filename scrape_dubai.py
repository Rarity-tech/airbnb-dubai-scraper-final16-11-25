from pyairbnb import Client
import csv
from datetime import datetime
import time

# Paramètres pour la recherche sur Dubaï
LOCATION = "Dubai"
MAX_RESULTS = 50
OUTPUT_CSV = "dubai_listings.csv"

# Initialisation du client
client = Client()

# Fonction utilitaire pour extraire l'année d'une date
def extract_year(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").year
    except:
        return ""

# Fonction principale

def scrape_dubai_listings():
    print("\nDémarrage du scraping Airbnb...")

    # Recherche initiale sur la ville
    listings = client.search_all(location=LOCATION)
    listings = listings[:MAX_RESULTS]  # Limiter à 50 pour test

    rows = []

    for listing in listings:
        room_id = listing.get("id")
        listing_url = f"https://www.airbnb.com/rooms/{room_id}"

        # Récupération des détails complets de l'annonce
        detail = client.get_details(room_id)

        listing_title = detail.get("name", "")
        license_code = detail.get("license") or detail.get("listing_license") or ""

        host = detail.get("host", {})
        host_id = host.get("id", "")
        host_name = host.get("name", "")
        host_profile_url = f"https://www.airbnb.com/users/show/{host_id}"
        host_rating = host.get("host_rating", "")
        host_reviews_count = host.get("host_review_count", "")
        host_joined_year = extract_year(host.get("host_since", ""))
        host_years_active = datetime.now().year - int(host_joined_year) if host_joined_year else ""
        host_total_listings_in_dubai = host.get("total_listings_count", "")

        row = {
            "room_id": room_id,
            "listing_url": listing_url,
            "listing_title": listing_title,
            "license_code": license_code,
            "host_id": host_id,
            "host_name": host_name,
            "host_profile_url": host_profile_url,
            "host_rating": host_rating,
            "host_reviews_count": host_reviews_count,
            "host_joined_year": host_joined_year,
            "host_years_active": host_years_active,
            "host_total_listings_in_dubai": host_total_listings_in_dubai
        }

        print(f"Scrappé : {room_id} - {listing_title[:40]}...")
        rows.append(row)
        time.sleep(1)  # Pause pour éviter les détections

    # Sauvegarde en CSV
    print(f"\nSauvegarde dans {OUTPUT_CSV}...")
    with open(OUTPUT_CSV, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print("\nScraping terminé avec succès.")


if __name__ == "__main__":
    scrape_dubai_listings()
