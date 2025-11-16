import os
import csv
import datetime as dt
from pathlib import Path

from pyairbnb import Airbnb

# -----------------------------------------------------------------------------
# Configuration générale
# -----------------------------------------------------------------------------

# Nombre max de listings à traiter par run (configurable via l'environnement)
LISTINGS_PER_RUN = int(os.getenv("LISTINGS_PER_RUN", "50"))

# Fichier de sortie
CSV_PATH = Path("dubai_listings.csv")

# Bounding box approximative de Dubaï
DUBAI_SW = (24.85, 54.95)  # (lat_min, lon_min)
DUBAI_NE = (25.35, 55.45)  # (lat_max, lon_max)
GRID_ROWS = 3
GRID_COLS = 4

# Fenêtre de dates pour le pricing (à adapter si besoin)
CHECKIN_OFFSET_DAYS = 14
STAY_NIGHTS = 5


# -----------------------------------------------------------------------------
# Utilitaires
# -----------------------------------------------------------------------------

def compute_dates():
    today = dt.date.today()
    check_in = today + dt.timedelta(days=CHECKIN_OFFSET_DAYS)
    check_out = check_in + dt.timedelta(days=STAY_NIGHTS)
    return check_in, check_out


def generate_zones():
    lat_min, lon_min = DUBAI_SW
    lat_max, lon_max = DUBAI_NE
    d_lat = (lat_max - lat_min) / GRID_ROWS
    d_lon = (lon_max - lon_min) / GRID_COLS

    zones = []
    for r in range(GRID_ROWS):
        for c in range(GRID_COLS):
            sw_lat = lat_min + r * d_lat
            sw_lon = lon_min + c * d_lon
            ne_lat = sw_lat + d_lat
            ne_lon = sw_lon + d_lon
            zones.append((f"zone_{r+1}_{c+1}", sw_lat, sw_lon, ne_lat, ne_lon))
    return zones


def load_existing_ids():
    if not CSV_PATH.exists():
        return set()
    ids = set()
    with CSV_PATH.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rid = row.get("room_id")
            if rid:
                ids.add(rid)
    return ids


CSV_COLUMNS = [
    "room_id",
    "name",
    "url",
    "city",
    "neighbourhood",
    "latitude",
    "longitude",
    "property_type",
    "room_type",
    "accommodates",
    "bathrooms",
    "bedrooms",
    "beds",
    "amenities",
    "price_total",
    "price_currency",
    "is_superhost",
    "rating",
    "reviews_count",
    "license",
    "instant_bookable",
    "min_nights",
    "max_nights",
    "host_id",
    "host_name",
    "host_since",
    "host_is_superhost",
    "host_total_listings",
]


def ensure_csv_header():
    if not CSV_PATH.exists():
        with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()


def safe_get(obj, *keys, default=None):
    cur = obj
    for k in keys:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur


# -----------------------------------------------------------------------------
# Extraction des infos détaillées d'un listing
# -----------------------------------------------------------------------------

def extract_listing_info(room_id, details, price_total=None, currency=None):
    d = details or {}

    # pyairbnb renvoie typiquement un dict avec une clé "listing" ou
    # "pdp_listing_detail". On combine les deux pour être robuste.
    listing = d.get("listing") or d.get("pdp_listing_detail") or {}
    pdp = d.get("pdp_listing_detail") or {}
    primary_host = (
        listing.get("primary_host")
        or pdp.get("primary_host")
        or {}
    )

    room_type = (
        listing.get("room_type")
        or safe_get(pdp, "room_and_property_type", "display_type")
    )

    neighbourhood = (
        listing.get("neighborhood")
        or listing.get("neighborhood_overview")
        or listing.get("localized_neighborhood")
        or safe_get(pdp, "neighborhood", "name")
    )

    # Gestion robuste de la liste d'amenities
    amenities_raw = listing.get("amenities") or safe_get(pdp, "listing_amenities")
    if isinstance(amenities_raw, list):
        amenities = ", ".join(
            sorted(
                {
                    a["name"]
                    if isinstance(a, dict) and "name" in a
                    else str(a)
                    for a in amenities_raw
                }
            )
        )
    else:
        amenities = None

    return {
        "room_id": str(room_id),
        "name": listing.get("name") or pdp.get("name"),
        "url": f"https://www.airbnb.com/rooms/{room_id}",
        "city": listing.get("city")
        or listing.get("localized_city")
        or pdp.get("city"),
        "neighbourhood": neighbourhood,
        "latitude": listing.get("lat")
        or listing.get("latitude")
        or pdp.get("lat"),
        "longitude": listing.get("lng")
        or listing.get("longitude")
        or pdp.get("lng"),
        "property_type": listing.get("property_type")
        or safe_get(pdp, "room_and_property_type", "property_type"),
        "room_type": room_type,
        "accommodates": listing.get("person_capacity")
        or listing.get("accommodates")
        or pdp.get("person_capacity"),
        "bathrooms": listing.get("bathrooms")
        or listing.get("bathrooms_number")
        or pdp.get("bathrooms"),
        "bedrooms": listing.get("bedrooms") or pdp.get("bedrooms"),
        "beds": listing.get("beds") or pdp.get("beds"),
        "amenities": amenities,
        "price_total": price_total,
        "price_currency": currency,
        "is_superhost": primary_host.get("is_superhost"),
        "rating": listing.get("star_rating")
        or listing.get("avg_rating")
        or pdp.get("star_rating"),
        "reviews_count": listing.get("reviews_count")
        or pdp.get("reviews_count"),
        "license": listing.get("license") or pdp.get("license"),
        "instant_bookable": listing.get("instant_bookable")
        or pdp.get("is_instant_bookable"),
        "min_nights": listing.get("min_nights")
        or listing.get("minimum_nights")
        or pdp.get("min_nights"),
        "max_nights": listing.get("max_nights")
        or listing.get("maximum_nights")
        or pdp.get("max_nights"),
        "host_id": primary_host.get("id"),
        "host_name": primary_host.get("host_name")
        or primary_host.get("name"),
        "host_since": primary_host.get("since")
        or primary_host.get("host_since"),
        "host_is_superhost": primary_host.get("is_superhost"),
        "host_total_listings": primary_host.get("total_listings_count"),
    }


# -----------------------------------------------------------------------------
# Script principal
# -----------------------------------------------------------------------------

def main():
    check_in, check_out = compute_dates()

    print("=" * 78)
    print(f"🚀 SCRAPING DUBAI - {dt.datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 78)
    print(f"📊 Configuration: {LISTINGS_PER_RUN} listings ce run\n")

    print("🔍 Phase 1: Recherche des room_ids")
    zones = generate_zones()
    print(f"   Zones : {len(zones)}")
    print(f"   Dates : {check_in} → {check_out}\n")

    client = Airbnb()
    all_ids = set()

    for idx, (zone_name, sw_lat, sw_lon, ne_lat, ne_lon) in enumerate(zones, start=1):
        print(
            f"[{idx}/{len(zones)}] 📍 {zone_name} "
            f"({sw_lat:.4f},{sw_lon:.4f}) → ({ne_lat:.4f},{ne_lon:.4f})..."
        )
        try:
            # Signature basée sur la doc pyairbnb: search_all(
            #   check_in, check_out,
            #   ne_lat, ne_long,
            #   sw_lat, sw_long,
            #   zoom_value,
            #   currency,
            #   place_type,
            #   price_min, price_max,
            #   amenities,
            #   language,
            # )
            results = client.search_all(
                check_in.isoformat(),
                check_out.isoformat(),
                ne_lat,
                ne_lon,
                sw_lat,
                sw_lon,
                15,  # zoom_value
                "USD",
                None,  # place_type
                None,  # price_min
                None,  # price_max
                None,  # amenities
                "en",
            )
            ids_zone = {str(item.get("id")) for item in results if item.get("id") is not None}
            print(f"   ✓ {len(ids_zone)} résultats")
            all_ids.update(ids_zone)
        except Exception as e:
            print(f"   ⚠️ Erreur pendant la recherche de la zone {zone_name}: {e}")
            print("   ✓ 0 résultats")

    print()
    print(f"✅ Phase 1 terminée: {len(all_ids)} room_ids uniques trouvés\n")

    existing_ids = load_existing_ids()
    remaining_ids = [rid for rid in all_ids if rid not in existing_ids]

    print("📊 Statut:")
    print(f"   • Total Dubai (IDs trouvés): {len(all_ids)}")
    print(f"   • Déjà traités: {len(existing_ids)}")
    print(f"   • Restants: {len(remaining_ids)}")

    if not remaining_ids:
        print("ℹ️ Aucun listing à traiter en Phase 2.")
        return

    to_process = remaining_ids[:LISTINGS_PER_RUN]
    print(f"   • Ce run (max): {len(to_process)}\n")

    print("🏗️ Phase 2: Récupération des détails listings")
    ensure_csv_header()

    rows_to_append = []
    processed = 0

    for idx, room_id in enumerate(to_process, start=1):
        print(f"[{idx}/{len(to_process)}] 🏠 room_id={room_id} ...")
        try:
            details = client.get_details(
                room_id=room_id,
                currency="USD",
                adults=2,
                language="en",
            )

            # Prix total pour les dates choisies (si la fonction échoue, on ignore le prix)
            price_total = None
            currency = "USD"
            try:
                price_info = client.get_price(
                    check_in.isoformat(),
                    check_out.isoformat(),
                    room_id=room_id,
                    currency="USD",
                )
                if isinstance(price_info, dict):
                    price_total = (
                        price_info.get("price_total")
                        or price_info.get("total_price")
                        or price_info.get("price")
                    )
                    currency = price_info.get("currency") or "USD"
            except Exception:
                pass

            row = extract_listing_info(
                room_id,
                details,
                price_total=price_total,
                currency=currency,
            )
            rows_to_append.append(row)
            processed += 1
        except Exception as e:
            print(f"   ⚠️ Erreur pour room_id {room_id}: {e}")

    if rows_to_append:
        with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            for row in rows_to_append:
                writer.writerow(row)

    print(f"\n✅ Phase 2 terminée: {processed} listings ajoutés dans ce run")
    print("=" * 78)
    print("🎉 RUN TERMINÉ")
    print("=" * 78)
    print(f"📊 Ce run: +{processed} listings")

    total = len(existing_ids) + processed
    approx_remaining = max(len(all_ids) - total, 0)
    print(f"📊 Total dans CSV: {total} listings")
    print(f"📊 Restants (approx): {approx_remaining}")
    print("\n💡 Pour continuer: relance le workflow")
    print("   (ou augmente LISTINGS_PER_RUN si tu veux aller plus vite)")
    print("=" * 78)


if __name__ == "__main__":
    main()
