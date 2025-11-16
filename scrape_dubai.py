import csv
import time
from pyairbnb.api import Api   # <<< CORRECTION CRITIQUE

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

LOCATION = "Dubai, United Arab Emirates"
CHECKIN = "2025-12-16"
CHECKOUT = "2025-12-19"
ADULTS = 1
MAX_SAMPLE = 40  # petit échantillon pour test rapide
CSV_FILE = "dubai_listings.csv"

# ---------------------------------------------------------------------------
# FONCTION : extraction robuste de l'identifiant
# ---------------------------------------------------------------------------

def extract_listing_id(item):
    """
    Extract listing ID from search results in a safe way for pyairbnb 2.1.1
    """
    if "listing" in item and isinstance(item["listing"], dict):
        if "id" in item["listing"]:
            return item["listing"]["id"]

    if "listingId" in item:
        return item["listingId"]

    if "id" in item:
        return item["id"]

    return None

# ---------------------------------------------------------------------------
# SCRAPER PRINCIPAL
# ---------------------------------------------------------------------------

def main():
    print("Initialisation pyairbnb…")
    client = Api(random_ua=True)

    print(f"Recherche : {LOCATION}")

    results = client.search.search_all(
        location=LOCATION,
        checkin=CHECKIN,
        checkout=CHECKOUT,
        adults=ADULTS
    )

    total = len(results)
    print(f"Total logements trouvés : {total}")

    if total == 0:
        print("Aucun résultat.")
        return

    sample = results[:MAX_SAMPLE]
    print(f"Échantillon : {len(sample)}")

    final_rows = []

    for idx, item in enumerate(sample, start=1):
        listing_id = extract_listing_id(item)

        if not listing_id:
            print(f"[ID MANQUANT] {idx}")
            continue

        try:
            print(f"→ Récupération détails {listing_id}")
            details = client.listings.get_details(listing_id)
            time.sleep(1)
        except Exception as e:
            print(f"[ERREUR] {listing_id} : {e}")
            continue

        listing_info = details.get("listing", {})

        title = listing_info.get("name")
        url = f"https://www.airbnb.com/rooms/{listing_id}"

        license_info = listing_info.get("license") or listing_info.get("licenseNumber")

        host_info = listing_info.get("primary_host", {})

        host_id = host_info.get("id")
        host_name = host_info.get("first_name")
        host_url = f"https://www.airbnb.com/users/show/{host_id}" if host_id else None
        host_since = host_info.get("host_since")
        host_total_listings = host_info.get("total_listings_count")
        host_review_count = host_info.get("reviewee_count")
        host_rating = host_info.get("star_rating")

        row = {
            "listing_id": listing_id,
            "title": title,
            "url": url,
            "license": license_info,
            "host_id": host_id,
            "host_name": host_name,
            "host_url": host_url,
            "host_since": host_since,
            "host_total_listings": host_total_listings,
            "host_review_count": host_review_count,
            "host_rating": host_rating,
        }

        final_rows.append(row)

    print(f"Écriture CSV → {CSV_FILE}")

    fieldnames = list(final_rows[0].keys()) if final_rows else []
    if not fieldnames:
        print("Aucune donnée à écrire.")
        return

    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(final_rows)

    print(f"✔ CSV généré ({len(final_rows)} lignes)")

# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()
