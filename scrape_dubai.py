import csv
import time
from datetime import datetime

import pyairbnb


# ==========================
# CONFIG GLOBALE
# ==========================

# Dates pour la recherche (nécessaires pour Airbnb, même si tu ne regardes pas les prix) :contentReference[oaicite:3]{index=3}
CHECK_IN = "2026-01-10"
CHECK_OUT = "2026-01-15"

CURRENCY = "AED"
LANGUAGE = "en"
PROXY_URL = ""  # laisse vide si tu n'as pas de proxy

# Zoom conseillé pour une grande ville (cf. exemples officiels) :contentReference[oaicite:4]{index=4}
ZOOM_VALUE = 10

# Limitation simple pour ne pas attaquer trop vite Airbnb (en secondes)
DELAY_BETWEEN_DETAIL_CALLS = 0.3  # 0.3s ~ raisonnable


# ==========================
# UTILITAIRES
# ==========================

def build_dubai_subzones(rows=3, cols=4):
    """
    Divise la bounding box de Dubaï en rows x cols sous-zones.

    Coordonnées de Dubaï (viewport Google): :contentReference[oaicite:5]{index=5}
    north = 25.3585607
    south = 24.7921359
    east  = 55.5650393
    west  = 54.8904543
    """
    north = 25.3585607
    south = 24.7921359
    east = 55.5650393
    west = 54.8904543

    lat_step = (north - south) / rows
    lng_step = (east - west) / cols

    zones = []
    for r in range(rows):
        for c in range(cols):
            z_sw_lat = south + r * lat_step
            z_sw_lng = west + c * lng_step
            z_ne_lat = z_sw_lat + lat_step
            z_ne_lng = z_sw_lng + lng_step
            zones.append(
                {
                    "name": f"zone_{r+1}_{c+1}",
                    "ne_lat": z_ne_lat,
                    "ne_long": z_ne_lng,
                    "sw_lat": z_sw_lat,
                    "sw_long": z_sw_lng,
                }
            )
    return zones


def try_paths(obj, paths, default=""):
    """
    Essaie plusieurs chemins possibles (dotted path) dans un gros JSON
    en utilisant pyairbnb.get_nested_value, qui est prévu pour ça. :contentReference[oaicite:6]{index=6}
    """
    for p in paths:
        try:
            val = pyairbnb.get_nested_value(obj, p, None)
        except Exception:
            val = None
        if val not in (None, "", []):
            return val
    return default


def safe_int(value, default=None):
    try:
        return int(value)
    except Exception:
        return default


# ==========================
# SCRAPING
# ==========================

def collect_listing_ids_for_dubai():
    """
    Utilise pyairbnb.search_all sur 12 sous-zones pour récupérer tous les listing IDs de Dubaï. :contentReference[oaicite:7]{index=7}
    """
    zones = build_dubai_subzones(rows=3, cols=4)
    listing_ids = set()

    print(f"Nombre de sous-zones à traiter : {len(zones)}", flush=True)

    for zone in zones:
        print(f"--- Recherche sur {zone['name']} ---", flush=True)

        search_results = pyairbnb.search_all(
            check_in=CHECK_IN,
            check_out=CHECK_OUT,
            ne_lat=zone["ne_lat"],
            ne_long=zone["ne_long"],
            sw_lat=zone["sw_lat"],
            sw_long=zone["sw_long"],
            zoom_value=ZOOM_VALUE,
            price_min=0,
            price_max=0,
            place_type="",        # toutes les catégories
            amenities=[],         # pas de filtre
            free_cancellation=False,
            currency=CURRENCY,
            language=LANGUAGE,
            proxy_url=PROXY_URL,
        )

        if not isinstance(search_results, list):
            # Selon les versions, search_all renvoie une liste ou une structure plus complexe.
            # On essaie de récupérer une liste de listings au mieux.
            possible_list = pyairbnb.get_nested_value(search_results, "results", [])
            if isinstance(possible_list, list):
                search_results = possible_list
            else:
                print(f"Format inattendu pour {zone['name']}, on continue.", flush=True)
                continue

        print(f"{zone['name']}: {len(search_results)} résultats bruts", flush=True)

        for item in search_results:
            # Beaucoup de scrapers Airbnb ont une structure type "listing.id"
            # On essaie cette structure d'abord, puis un id direct.
            lid = try_paths(item, ["listing.id", "id"])
            if not lid:
                continue
            # On force en string pour construire l'URL plus tard
            listing_ids.add(str(lid))

        print(f"Total d'IDs uniques cumulés: {len(listing_ids)}", flush=True)

    return sorted(listing_ids)


def get_listing_and_host_details(listing_id: str):
    """
    Récupère les détails complets pour une annonce + les infos host.
    Utilise pyairbnb.get_details comme dans la doc. :contentReference[oaicite:8]{index=8}
    """
    rid_int = safe_int(listing_id, listing_id)

    data = pyairbnb.get_details(
        room_id=rid_int,
        currency=CURRENCY,
        proxy_url=PROXY_URL,
        adults=2,
        language=LANGUAGE,
    )

    # Listing
    listing_title = try_paths(
        data,
        [
            "pdp_listing_detail.name",
            "listing.title",
            "listing.name",
        ],
        default="",
    )

    license_code = try_paths(
        data,
        [
            "pdp_listing_detail.license_number",
            "pdp_listing_detail.license",
            "listing.license_number",
            "listing.license",
        ],
        default="",
    )

    dtcm_link = ""
    if license_code:
        dtcm_link = (
            "https://hhpermits.det.gov.ae/holidayhomes/Customization/"
            "DTCM/CustomPages/HHQRCode.aspx?r=" + str(license_code)
        )

    # Host
    host_id = try_paths(
        data,
        [
            "pdp_listing_detail.primary_host.id",
            "primary_host.id",
            "listing.primary_host.id",
        ],
        default="",
    )
    host_name = try_paths(
        data,
        [
            "pdp_listing_detail.primary_host.full_name",
            "primary_host.full_name",
            "primary_host.name",
        ],
        default="",
    )

    host_rating = try_paths(
        data,
        [
            "pdp_listing_detail.primary_host.overall_rating",
            "primary_host.overall_rating",
            "primary_host.overallRatingLocalized",
        ],
        default="",
    )

    host_reviews_count = try_paths(
        data,
        [
            "pdp_listing_detail.primary_host.review_count",
            "primary_host.review_count",
        ],
        default="",
    )

    member_since = try_paths(
        data,
        [
            "pdp_listing_detail.primary_host.member_since",
            "primary_host.member_since",
        ],
        default="",
    )

    joined_year = ""
    years_active = ""

    if isinstance(member_since, str) and len(member_since) >= 4:
        try:
            joined_year_int = int(member_since[:4])
            joined_year = joined_year_int
            current_year = datetime.utcnow().year
            years_active = current_year - joined_year_int
        except Exception:
            joined_year = ""
            years_active = ""

    host_profile_url = ""
    if host_id:
        host_profile_url = f"https://www.airbnb.com/users/show/{host_id}"

    listing_url = f"https://www.airbnb.com/rooms/{listing_id}"

    return {
        "listing_id": listing_id,
        "listing_url": listing_url,
        "listing_title": listing_title,
        "license_code": license_code,
        "dtcm_link": dtcm_link,
        "host_id": host_id,
        "host_name": host_name,
        "host_profile_url": host_profile_url,
        "host_rating": host_rating,
        "host_reviews_count": host_reviews_count,
        "host_joined_year": joined_year,
        "host_years_active": years_active,
    }


def scrape_dubai_to_csv(output_csv_path: str):
    # 1) Récupérer tous les IDs d’annonces
    listing_ids = collect_listing_ids_for_dubai()
    print(f"Nombre total d’annonces uniques trouvées pour Dubaï: {len(listing_ids)}", flush=True)

    details_records = []
    host_listing_count = {}

    # 2) Pour chaque annonce, récupérer les détails + host
    for idx, listing_id in enumerate(listing_ids, start=1):
        print(f"[{idx}/{len(listing_ids)}] Détails pour listing {listing_id}", flush=True)

        try:
            record = get_listing_and_host_details(listing_id)
        except Exception as e:
            print(f"Erreur sur listing {listing_id}: {e}", flush=True)
            time.sleep(1.0)
            continue

        details_records.append(record)

        host_id = record.get("host_id")
        if host_id:
            host_listing_count[host_id] = host_listing_count.get(host_id, 0) + 1

        time.sleep(DELAY_BETWEEN_DETAIL_CALLS)

    # 3) Ajout du nombre de listings par host dans Dubaï
    for record in details_records:
        host_id = record.get("host_id")
        total_for_host = host_listing_count.get(host_id, 0) if host_id else 0
        record["host_total_listings_in_dubai"] = total_for_host

    # 4) Écriture du CSV final
    fieldnames = [
        "listing_id",
        "listing_url",
        "listing_title",
        "license_code",
        "dtcm_link",
        "host_id",
        "host_name",
        "host_profile_url",
        "host_rating",
        "host_reviews_count",
        "host_joined_year",
        "host_years_active",
        "host_total_listings_in_dubai",
    ]

    with open(output_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for record in details_records:
            writer.writerow(record)

    print(f"CSV final écrit dans: {output_csv_path}", flush=True)


if __name__ == "__main__":
    scrape_dubai_to_csv("dubai_listings.csv")
