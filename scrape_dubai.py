import os
import csv
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple

import pyairbnb

# ========================
# Configuration
# ========================

LISTINGS_PER_RUN = int(os.getenv("LISTINGS_PER_RUN", "50"))
CHECK_IN_DAYS_AHEAD = int(os.getenv("CHECK_IN_DAYS_AHEAD", "14"))
STAY_NIGHTS = int(os.getenv("STAY_NIGHTS", "5"))

CSV_PATH = "dubai_listings.csv"

DUBAI_BBOX = {
    "lat_min": 24.85,
    "lat_max": 25.35,
    "lon_min": 54.95,
    "lon_max": 55.45,
}
GRID_ROWS = 3
GRID_COLS = 4

CSV_COLUMNS = [
    "scrape_datetime",
    "room_id",
    "name",
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
    "amenities_count",
    "review_count",
    "rating",
    "license",
    # Host variables
    "host_id",
    "host_name",
    "host_is_superhost",
    "host_total_listings_count",
    "host_identity_verified",
    # Booking/availability
    "min_nights",
    "max_nights",
    "instant_bookable",
    # Price (if disponible dans les résultats de search)
    "price_nightly",
    "currency",
]


# ========================
# Helpers
# ========================


def log(msg: str) -> None:
    print(msg, flush=True)


def build_zones() -> List[Tuple[str, float, float, float, float]]:
    zones = []
    lat_step = (DUBAI_BBOX["lat_max"] - DUBAI_BBOX["lat_min"]) / GRID_ROWS
    lon_step = (DUBAI_BBOX["lon_max"] - DUBAI_BBOX["lon_min"]) / GRID_COLS

    for i in range(GRID_ROWS):
        for j in range(GRID_COLS):
            lat_min = DUBAI_BBOX["lat_min"] + i * lat_step
            lat_max = DUBAI_BBOX["lat_min"] + (i + 1) * lat_step
            lon_min = DUBAI_BBOX["lon_min"] + j * lon_step
            lon_max = DUBAI_BBOX["lon_min"] + (j + 1) * lon_step
            zone_name = f"zone_{i+1}_{j+1}"
            zones.append((zone_name, lat_min, lon_min, lat_max, lon_max))
    return zones


def safe_get_nested(obj: Any, path: str, default: Any = "") -> Any:
    """Wrapper autour de pyairbnb.get_nested_value, toujours safe."""
    try:
        return pyairbnb.get_nested_value(obj, path, default)
    except Exception:
        return default


def read_existing_ids(path: str) -> Dict[str, Dict[str, Any]]:
    existing: Dict[str, Dict[str, Any]] = {}
    if not os.path.exists(path):
        return existing

    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rid = row.get("room_id")
            if rid:
                existing[rid] = row
    return existing


def ensure_csv_header(path: str) -> None:
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()


# ========================
# Phase 1: search_all → room_ids + prix
# ========================


def search_room_ids(checkin: str, checkout: str) -> Tuple[List[str], Dict[str, Dict[str, Any]]]:
    """Retourne la liste de room_ids et un cache de métadonnées par room_id."""
    zones = build_zones()
    all_room_ids: List[str] = []
    meta_by_room: Dict[str, Dict[str, Any]] = {}

    log("===============================================================================" )
    log(f"🚀 SCRAPING DUBAI - {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} (UTC)")
    log("===============================================================================" )
    log(f"📊 Configuration: {LISTINGS_PER_RUN} listings ce run")
    log("")
    log("🔍 Phase 1: Recherche des room_ids")
    log(f"   Zones : {len(zones)}")
    log(f"   Dates : {checkin} → {checkout}")
    log("")

    for idx, (zone_name, lat_min, lon_min, lat_max, lon_max) in enumerate(zones, start=1):
        log(f"[{idx}/{len(zones)}] 📍 {zone_name} ({lat_min:.4f},{lon_min:.4f}) → ({lat_max:.4f},{lon_max:.4f})...")
        try:
            results = pyairbnb.search_all(
                check_in=checkin,
                check_out=checkout,
                items_offset=0,
                items_per_grid=50,
                map_bounds={
                    "ne": {"latitude": lat_max, "longitude": lon_max},
                    "sw": {"latitude": lat_min, "longitude": lon_min},
                },
                refinement_paths=["/homes"],
                selected_tab_id="home_tab",
                search_type="PAGINATION",
                place_id=None,
                proxy_url="",
            )
        except Exception as e:
            log(f"   ⚠️ Erreur pendant la recherche de la zone {zone_name}: {e}")
            log("   ✓ 0 résultats")
            continue

        if not isinstance(results, list):
            log("   ⚠️ Résultat inattendu (non-list)")
            log("   ✓ 0 résultats")
            continue

        count = 0
        for item in results:
            # room_id: on prend d'abord id direct, sinon listing.id
            rid = None
            if isinstance(item, dict):
                rid = str(item.get("id") or safe_get_nested(item, "listing.id", "")).strip()

            if not rid:
                continue

            if rid not in meta_by_room:
                meta: Dict[str, Any] = {}

                # On essaie de capturer prix & devise depuis le résultat de recherche
                meta["price_nightly"] = (
                    safe_get_nested(item, "pricingQuote.structuredStayDisplayPrice.primaryLine.price", "")
                    or safe_get_nested(item, "pricingQuote.priceString", "")
                )
                meta["currency"] = safe_get_nested(item, "pricingQuote.price.currency", "")

                meta_by_room[rid] = meta
                all_room_ids.append(rid)
                count += 1

        log(f"   ✓ {count} résultats (uniques)")

    log("")
    log(f"✅ Phase 1 terminée: {len(all_room_ids)} room_ids uniques trouvés")
    log("")

    return all_room_ids, meta_by_room


# ========================
# Phase 2: get_details → colonnes détaillées
# ========================


def build_row(room_id: str, details: Dict[str, Any], search_meta: Dict[str, Any]) -> Dict[str, Any]:
    listing = details  # get_nested_value attend l'objet brut

    row: Dict[str, Any] = {c: "" for c in CSV_COLUMNS}
    row["scrape_datetime"] = datetime.utcnow().isoformat()
    row["room_id"] = room_id

    # Infos listing
    row["name"] = safe_get_nested(listing, "listing.name", "")
    row["city"] = safe_get_nested(listing, "listing.city", "")
    row["neighbourhood"] = safe_get_nested(listing, "listing.neighborhood", "")
    row["latitude"] = safe_get_nested(listing, "listing.lat", "")
    row["longitude"] = safe_get_nested(listing, "listing.lng", "")
    row["property_type"] = safe_get_nested(listing, "listing.propertyType", "")
    row["room_type"] = safe_get_nested(listing, "listing.roomTypeCategory", "")

    row["accommodates"] = safe_get_nested(listing, "listing.personCapacity", "")
    row["bathrooms"] = safe_get_nested(listing, "listing.bathrooms", "")
    row["bedrooms"] = safe_get_nested(listing, "listing.bedrooms", "")
    row["beds"] = safe_get_nested(listing, "listing.beds", "")

    # Amenities count (si liste disponible)
    amenities = safe_get_nested(listing, "listing.amenities", [])
    if isinstance(amenities, list):
        row["amenities_count"] = str(len(amenities))
    else:
        row["amenities_count"] = ""

    row["review_count"] = safe_get_nested(listing, "listing.reviewsCount", "")
    row["rating"] = safe_get_nested(listing, "listing.avgRating", "")

    # Licence
    row["license"] = safe_get_nested(listing, "listing.license", "")

    # Host infos (schéma typique d'Airbnb : primaryHost)
    row["host_id"] = safe_get_nested(listing, "listing.primaryHost.id", "")
    row["host_name"] = (
        safe_get_nested(listing, "listing.primaryHost.hostName", "")
        or safe_get_nested(listing, "listing.primaryHost.name", "")
    )
    row["host_is_superhost"] = safe_get_nested(listing, "listing.primaryHost.isSuperhost", "")
    row["host_total_listings_count"] = safe_get_nested(listing, "listing.primaryHost.listingCount", "")
    row["host_identity_verified"] = safe_get_nested(listing, "listing.primaryHost.isIdentityVerified", "")

    # Booking constraints
    row["min_nights"] = safe_get_nested(listing, "listing.minNights", "")
    row["max_nights"] = safe_get_nested(listing, "listing.maxNights", "")
    row["instant_bookable"] = safe_get_nested(listing, "listing.isInstantBookable", "")

    # Prix depuis la phase 1 (si dispo)
    row["price_nightly"] = search_meta.get("price_nightly", "") if search_meta else ""
    row["currency"] = search_meta.get("currency", "") if search_meta else ""

    return row


def fetch_and_append_details(room_ids: List[str], meta_by_room: Dict[str, Dict[str, Any]], existing_ids: Dict[str, Dict[str, Any]]) -> int:
    ensure_csv_header(CSV_PATH)

    to_process = [rid for rid in room_ids if rid not in existing_ids]
    if not to_process:
        log("ℹ️ Aucun nouveau listing à traiter en Phase 2.")
        return 0

    to_process = to_process[:LISTINGS_PER_RUN]

    log("🏗️ Phase 2: Récupération des détails listings")
    log(f"   • Déjà dans CSV : {len(existing_ids)}")
    log(f"   • Nouveaux trouvés (Phase 1) : {len(room_ids)}")
    log(f"   • À traiter dans ce run : {len(to_process)} (max {LISTINGS_PER_RUN})")
    log("")

    rows: List[Dict[str, Any]] = []

    for idx, rid in enumerate(to_process, start=1):
        log(f"[{idx}/{len(to_process)}] 🏠 room_id={rid} ...")
        try:
            details = pyairbnb.get_details(
                room_id=rid,
                currency="USD",
                adults=2,
                language="en",
                proxy_url="",
            )
            meta = meta_by_room.get(rid, {})
            row = build_row(rid, details, meta)
            rows.append(row)
        except Exception as e:
            log(f"   ⚠️ Erreur pour room_id {rid}: {e}")

    if not rows:
        log("ℹ️ Aucun détail à ajouter dans le CSV.")
        return 0

    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        for row in rows:
            writer.writerow(row)

    log("")
    log(f"✅ Phase 2 terminée: {len(rows)} listings ajoutés dans ce run")
    return len(rows)


# ========================
# Main
# ========================


def main() -> None:
    today = datetime.utcnow().date()
    checkin_date = today + timedelta(days=CHECK_IN_DAYS_AHEAD)
    checkout_date = checkin_date + timedelta(days=STAY_NIGHTS)

    checkin_str = checkin_date.isoformat()
    checkout_str = checkout_date.isoformat()

    # Phase 1: recherche des IDs
    room_ids, meta_by_room = search_room_ids(checkin_str, checkout_str)

    existing_ids = read_existing_ids(CSV_PATH)

    log("📊 Statut:")
    log(f"   • Total Dubai (IDs trouvés): {len(room_ids)}")
    log(f"   • Déjà traités: {len(existing_ids)}")
    restants = len([rid for rid in room_ids if rid not in existing_ids])
    log(f"   • Restants: {restants}")
    log("")

    # Phase 2: détails + CSV
    added = fetch_and_append_details(room_ids, meta_by_room, existing_ids)

    total_after = len(existing_ids) + added

    log("===============================================================================")
    log("🎉 RUN TERMINÉ")
    log("===============================================================================")
    log(f"📊 Ce run: +{added} listings")
    log(f"📊 Total dans CSV: {total_after} listings")
    log(f"📊 Restants (approx): {max(restants - added, 0)}")
    log("")
    log("💡 Pour continuer: relance le workflow")
    log(f"   (ou augmente LISTINGS_PER_RUN si tu veux aller plus vite)")
    log("===============================================================================")


if __name__ == "__main__":
    main()
