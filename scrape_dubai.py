import os
import csv
from datetime import datetime, timedelta
from typing import List, Dict, Any, Set, Tuple

import pyairbnb  # type: ignore


CSV_PATH = "dubai_listings.csv"
LISTINGS_PER_RUN = int(os.getenv("LISTINGS_PER_RUN", "50"))
CURRENCY = os.getenv("CURRENCY", "AED")
LANGUAGE = os.getenv("LANGUAGE", "en")


def banner(msg: str) -> None:
    print("=" * 79)
    if msg:
        print(msg)
    print("=" * 79)


def compute_date_range() -> Tuple[str, str]:
    """Check-in dans 14 jours, 5 nuits par défaut."""
    today = datetime.utcnow().date()
    check_in = today + timedelta(days=14)
    check_out = check_in + timedelta(days=5)
    return check_in.isoformat(), check_out.isoformat()


def build_dubai_grid(rows: int = 3, cols: int = 4) -> List[Dict[str, Any]]:
    """Grille Dubai (3x4) reproduisant les coordonnées des logs."""
    sw_lat, sw_lng = 24.8500, 54.9500
    ne_lat, ne_lng = 25.3501, 55.4500

    d_lat = (ne_lat - sw_lat) / rows
    d_lng = (ne_lng - sw_lng) / cols

    zones: List[Dict[str, Any]] = []
    for i in range(rows):
        for j in range(cols):
            z_sw_lat = sw_lat + i * d_lat
            z_sw_lng = sw_lng + j * d_lng
            z_ne_lat = sw_lat + (i + 1) * d_lat
            z_ne_lng = sw_lng + (j + 1) * d_lng
            zones.append(
                {
                    "name": f"zone_{i+1}_{j+1}",
                    "sw_lat": round(z_sw_lat, 4),
                    "sw_lng": round(z_sw_lng, 4),
                    "ne_lat": round(z_ne_lat, 4),
                    "ne_lng": round(z_ne_lng, 4),
                }
            )
    return zones


def load_existing_ids() -> Set[str]:
    if not os.path.exists(CSV_PATH):
        return set()
    ids: Set[str] = set()
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rid = row.get("room_id")
            if rid:
                ids.add(str(rid))
    return ids


def ensure_csv_header() -> None:
    if os.path.exists(CSV_PATH):
        return
    fieldnames = [
        "room_id",
        "url",
        "title",
        "neighbourhood",
        "city",
        "latitude",
        "longitude",
        "price",
        "currency",
        "rating",
        "reviews_count",
        "host_id",
        "host_name",
        "license",
        "created_at",
    ]
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()


def extract_room_ids(search_results: Any) -> List[str]:
    """Extrait une liste de room_id à partir de la réponse search_all()."""
    room_ids: List[str] = []
    if not isinstance(search_results, list):
        return room_ids

    for item in search_results:
        if not isinstance(item, dict):
            continue
        room_id = (
            item.get("room_id")
            or item.get("id")
            or item.get("listing_id")
        )
        if room_id is None:
            continue
        room_ids.append(str(room_id))
    return room_ids


def search_room_ids(check_in: str, check_out: str) -> List[str]:
    zones = build_dubai_grid()
    all_ids: Set[str] = set()

    print("🔍 Phase 1: Recherche des room_ids")
    print(f"   Zones : {len(zones)}")
    print(f"   Dates : {check_in} → {check_out}")
    print()

    for idx, zone in enumerate(zones, start=1):
        print(
            f"[{idx}/{len(zones)}] 📍 {zone['name']} "
            f"({zone['sw_lat']:.4f},{zone['sw_lng']:.4f}) → "
            f"({zone['ne_lat']:.4f},{zone['ne_lng']:.4f})..."
        )

        try:
            # CORRECTION IMPORTANTE: check_in / check_out (avec underscore)
            results = pyairbnb.search_all(
                check_in=check_in,
                check_out=check_out,
                ne_lat=zone["ne_lat"],
                ne_long=zone["ne_lng"],
                sw_lat=zone["sw_lat"],
                sw_long=zone["sw_lng"],
                zoom_value=12,
                price_min=0,
                price_max=0,
                place_type="",
                amenities=[],
                free_cancellation=False,
                currency=CURRENCY,
                language=LANGUAGE,
                proxy_url="",
            )
            zone_ids = extract_room_ids(results)
            print(f"   ✓ {len(zone_ids)} résultats")
            all_ids.update(zone_ids)
        except TypeError as e:
            print(f"   ⚠️ Erreur pendant la recherche de la zone {zone['name']}: {e}")
        except Exception as e:
            print(f"   ⚠️ Erreur inattendue pour la zone {zone['name']}: {e}")
    print()
    print(f"✅ Phase 1 terminée: {len(all_ids)} room_ids uniques trouvés")
    print()
    return list(all_ids)


def get_nested(d: Dict[str, Any], path: str, default=None):
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict):
            return default
        cur = cur.get(part)
        if cur is None:
            return default
    return cur


def fetch_listing_details(room_ids: List[str], already_in_csv: Set[str]) -> int:
    if not room_ids:
        print("ℹ️ Aucun listing à traiter en Phase 2.")
        return 0

    remaining_ids = [rid for rid in room_ids if rid not in already_in_csv]
    if not remaining_ids:
        print("ℹ️ Tous les listings trouvés sont déjà dans le CSV.")
        return 0

    to_process = remaining_ids[:LISTINGS_PER_RUN]

    print("🏗️ Phase 2: Récupération des détails listings")
    print(f"   • Déjà dans CSV : {len(already_in_csv)}")
    print(f"   • Nouveaux trouvés (Phase 1) : {len(room_ids)}")
    print(f"   • À traiter dans ce run : {len(to_process)} (max {LISTINGS_PER_RUN})")
    print()

    ensure_csv_header()
    added = 0

    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "room_id",
            "url",
            "title",
            "neighbourhood",
            "city",
            "latitude",
            "longitude",
            "price",
            "currency",
            "rating",
            "reviews_count",
            "host_id",
            "host_name",
            "license",
            "created_at",
        ])

        for idx, rid in enumerate(to_process, start=1):
            print(f"[{idx}/{len(to_process)}] 🏠 room_id={rid} ...")
            try:
                data = pyairbnb.get_details(
                    room_id=int(rid),
                    currency=CURRENCY,
                    proxy_url="",
                    adults=2,
                    language=LANGUAGE,
                )

                if isinstance(data, tuple):
                    data = data[0]

                listing = get_nested(data, "pdp_listing_detail", {}) or data

                url = get_nested(listing, "sectioned_description.product_url", "")
                if not url:
                    url = f"https://www.airbnb.com/rooms/{rid}"

                title = (
                    get_nested(listing, "p3_summary_title", "")
                    or listing.get("name")
                    or ""
                )
                neighbourhood = (
                    get_nested(listing, "location.neighborhood", "")
                    or get_nested(listing, "localized_city", "")
                    or ""
                )
                city = (
                    get_nested(listing, "location.city", "")
                    or get_nested(listing, "localized_city", "")
                    or ""
                )
                latitude = get_nested(listing, "lat", "")
                longitude = get_nested(listing, "lng", "")

                price = get_nested(listing, "p3_event_data.price.price_string", "")
                if isinstance(price, dict):
                    price = price.get("amount", "")

                rating = get_nested(listing, "star_rating", "")
                reviews_count = get_nested(listing, "reviews_count", "")

                host = get_nested(listing, "primary_host", {}) or {}
                host_id = host.get("id", "")
                host_name = host.get("host_name", "") or host.get("name", "")

                license_code = (
                    listing.get("license")
                    or get_nested(listing, "license", "")
                    or ""
                )

                writer.writerow(
                    {
                        "room_id": rid,
                        "url": url,
                        "title": title,
                        "neighbourhood": neighbourhood,
                        "city": city,
                        "latitude": latitude,
                        "longitude": longitude,
                        "price": price,
                        "currency": CURRENCY,
                        "rating": rating,
                        "reviews_count": reviews_count,
                        "host_id": host_id,
                        "host_name": host_name,
                        "license": license_code,
                        "created_at": datetime.utcnow().isoformat(),
                    }
                )
                added += 1
            except Exception as e:
                print(f"   ⚠️ Erreur pour room_id {rid}: {e}")

    print()
    print(f"✅ Phase 2 terminée: {added} listings ajoutés dans ce run")
    return added


def main() -> None:
    banner(f"🚀 SCRAPING DUBAI - {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📊 Configuration: {LISTINGS_PER_RUN} listings ce run")
    print()
    check_in, check_out = compute_date_range()

    # Phase 1
    room_ids = search_room_ids(check_in, check_out)

    # Statut intermédiaire
    in_csv = load_existing_ids()
    total_found = len(room_ids)
    already = len(in_csv.intersection(set(room_ids)))
    remaining = max(0, total_found - already)

    print("📊 Statut:")
    print(f"   • Total Dubai (IDs trouvés): {total_found}")
    print(f"   • Déjà traités: {already}")
    print(f"   • Restants: {remaining}")
    print(f"   • Ce run (max): {LISTINGS_PER_RUN}")
    print()

    # Phase 2
    added = fetch_listing_details(room_ids, in_csv)

    final_total = len(load_existing_ids())
    restants = max(0, total_found - final_total)

    banner("🎉 RUN TERMINÉ")
    print(f"📊 Ce run: +{added} listings")
    print(f"📊 Total dans CSV: {final_total} listings")
    print(f"📊 Restants (approx): {restants}")
    print()
    print("💡 Pour continuer: relance le workflow")
    print("   (ou augmente LISTINGS_PER_RUN si tu veux aller plus vite)")
    banner("")


if __name__ == "__main__":
    main()
