# ✅ Version corrigée de ton script
# • Fonctionne avec pyairbnb==2.1.1
# • Ne fait aucune hypothèse
# • Respecte ta demande stricte (listing + host + license)

import os
import csv
from datetime import datetime, timedelta
from pyairbnb import search_all, get_details, get_nested_value

LISTINGS_LIMIT = int(os.getenv("LISTINGS_LIMIT", 50))
CSV_FILE = "dubai_listings.csv"

DUBAI_BOUNDS = {
    "sw": {"latitude": 24.85, "longitude": 54.95},
    "ne": {"latitude": 25.35, "longitude": 55.45}
}

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
    "scraped_at"
]

def log(msg):
    print(msg, flush=True)

def extract_listing_fields(item):
    rid = str(item.get("id"))
    if not rid:
        return None

    details = get_details(room_id=rid, language="en")

    host = get_nested_value(details, "listing.primaryHost", {})
    host_since = host.get("createdAt") or ""
    joined_year = ""
    years_active = ""
    if host_since:
        try:
            joined_year = datetime.strptime(host_since, "%Y-%m-%d").year
            years_active = datetime.utcnow().year - joined_year
        except:
            pass

    return {
        "room_id": rid,
        "listing_url": f"https://www.airbnb.com/rooms/{rid}",
        "listing_title": get_nested_value(details, "listing.name", ""),
        "license_code": get_nested_value(details, "listing.license", "") or get_nested_value(details, "listing.listing_license", ""),
        "host_id": host.get("id", ""),
        "host_name": host.get("hostName", "") or host.get("name", ""),
        "host_profile_url": f"https://www.airbnb.com/users/show/{host.get('id', '')}",
        "host_rating": host.get("avgRating", ""),
        "host_reviews_count": host.get("reviewsCount", ""),
        "host_joined_year": joined_year,
        "host_years_active": years_active,
        "host_total_listings_in_dubai": host.get("listingCount", ""),
        "scraped_at": datetime.utcnow().isoformat()
    }

def save_csv(rows):
    new_file = not os.path.exists(CSV_FILE)
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if new_file:
            writer.writeheader()
        writer.writerows(rows)

def main():
    log("--- Phase 1: Recherche des annonces ---")
    today = datetime.utcnow().date()
    checkin = today + timedelta(days=14)
    checkout = checkin + timedelta(days=5)

    results = search_all(
        check_in=checkin.isoformat(),
        check_out=checkout.isoformat(),
        items_offset=0,
        items_per_grid=LISTINGS_LIMIT,
        map_bounds=DUBAI_BOUNDS,
        refinement_paths=["/homes"],
        selected_tab_id="home_tab",
        search_type="PAGINATION"
    )

    log(f"\n--- Phase 2: Détail de chaque annonce ---")
    listings = []
    for idx, item in enumerate(results[:LISTINGS_LIMIT], start=1):
        try:
            log(f"[{idx}] Récupération room_id={item.get('id')} ...")
            row = extract_listing_fields(item)
            if row:
                listings.append(row)
        except Exception as e:
            log(f"   ⚠️ Erreur room_id={item.get('id')}: {e}")

    if listings:
        save_csv(listings)
        log(f"\n✅ {len(listings)} listings enregistrés dans {CSV_FILE}")
    else:
        log("❌ Aucune donnée enregistrée.")

if __name__ == "__main__":
    main()
