import csv
import os
import re
import subprocess
import time
from datetime import datetime, timedelta
from functools import wraps

import pyairbnb


# ==========================
# ⚙️ CONTRÔLE DU RUN
# ==========================
LISTINGS_PER_RUN = 200  # ← tu peux mettre 500, 5000, etc.


# ==========================
# CONFIG GLOBALE
# ==========================
future_date = datetime.now() + timedelta(days=14)
CHECK_IN = future_date.strftime("%Y-%m-%d")
CHECK_OUT = (future_date + timedelta(days=5)).strftime("%Y-%m-%d")

CURRENCY = "AED"
LANGUAGE = "en"
PROXY_URL = ""

# IMPORTANT : zoom niveau "ville"
ZOOM_VALUE = 10

DELAY_BETWEEN_DETAILS = 0.5
DELAY_BETWEEN_ZONES = 2.0
COMMIT_EVERY = 50

CSV_FILE = "dubai_listings.csv"
PROCESSED_IDS_FILE = "processed_ids.txt"


# ==========================
# UTILITAIRES GÉNÉRIQUES
# ==========================

def retry_on_failure(max_retries=3, delay=2):
    """Decorator pour retry avec backoff exponentiel"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    wait_time = delay * (2 ** attempt)
                    print(
                        f"⚠️ Tentative {attempt + 1}/{max_retries} échouée: {e}. "
                        f"Retry dans {wait_time}s",
                        flush=True,
                    )
                    time.sleep(wait_time)
            return None
        return wrapper
    return decorator


def nested(obj, paths, default=""):
    """
    Essaie plusieurs chemins 'a.b.c' via pyairbnb.get_nested_value.
    Retourne le premier non vide trouvé.
    """
    for path in paths:
        try:
            val = pyairbnb.get_nested_value(obj, path, None)
        except Exception:
            val = None
        if val not in (None, "", [], {}):
            return val
    return default


def build_dubai_subzones(rows=3, cols=4):
    """
    Divise la ville de Dubai en sous-zones.

    Bounding box resserrée autour de Dubai (ville), pas tout l'émirat.
    Centre ≈ (25.07, 55.17)
    """
    north = 25.35
    south = 24.85
    east = 55.45
    west = 54.95

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


def extract_license_code(text):
    """
    Extrait le license code DTCM de la description
    Format attendu: BUS-PRI-UL7GO etc.
    """
    if not text:
        return ""
    pattern = r"\b[A-Z]{3}-[A-Z]{3}-[A-Z0-9]{5,6}\b"
    matches = re.findall(pattern, str(text))
    return matches[0] if matches else ""


def git_commit_and_push(message):
    """Commit et push vers GitHub"""
    try:
        subprocess.run(
            ["git", "config", "user.name", "GitHub Actions"],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "config", "user.email", "actions@github.com"],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "add", CSV_FILE, PROCESSED_IDS_FILE],
            check=True,
            capture_output=True,
        )
        # Ne pas committer s'il n'y a aucun changement
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            check=True,
            capture_output=True,
            text=True,
        )
        if not status.stdout.strip():
            print("ℹ️ Aucun changement à committer.", flush=True)
            return False

        subprocess.run(
            ["git", "commit", "-m", message],
            check=True,
            capture_output=True,
        )
        subprocess.run(["git", "push"], check=True, capture_output=True)
        print(f"✅ Git commit: {message}", flush=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit/push échoué: {e}", flush=True)
        return False


def load_processed_ids():
    """Charge les IDs déjà traités"""
    if os.path.exists(PROCESSED_IDS_FILE):
        with open(PROCESSED_IDS_FILE, "r") as f:
            ids = {line.strip() for line in f if line.strip()}
        print(
            f"📂 {len(ids)} listings déjà traités "
            f"(chargés depuis {PROCESSED_IDS_FILE})",
            flush=True,
        )
        return ids
    return set()


def save_processed_id(room_id):
    """Sauvegarde un ID comme traité"""
    with open(PROCESSED_IDS_FILE, "a") as f:
        f.write(f"{room_id}\n")


def load_existing_csv():
    """Charge le CSV existant pour append"""
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            existing = list(reader)
        print(f"📂 {len(existing)} lignes déjà dans {CSV_FILE}", flush=True)
        return existing
    return []


def get_room_id_from_result(result):
    """
    Extraire le room_id depuis un élément de search_all.
    On teste plusieurs clés possibles.
    """
    candidates = [
        result.get("room_id"),
        result.get("id"),
        nested(result, ["listing.id", "listing.room_id"], default=""),
    ]
    for c in candidates:
        if c not in (None, "", 0):
            return str(c)
    return None


# ==========================
# SCRAPING
# ==========================

@retry_on_failure(max_retries=3, delay=2)
def search_zone_with_retry(zone):
    """Recherche dans une zone avec retry"""
    return pyairbnb.search_all(
        check_in=CHECK_IN,
        check_out=CHECK_OUT,
        ne_lat=zone["ne_lat"],
        ne_long=zone["ne_long"],
        sw_lat=zone["sw_lat"],
        sw_long=zone["sw_long"],
        zoom_value=ZOOM_VALUE,
        price_min=0,
        price_max=0,
        place_type="",      # toutes les catégories
        amenities=[],       # pas de filtre
        free_cancellation=False,
        currency=CURRENCY,
        language=LANGUAGE,
        proxy_url=PROXY_URL,
    )


def collect_all_room_ids():
    """Phase 1: Récupère tous les room_ids de Dubai"""
    zones = build_dubai_subzones(rows=3, cols=4)
    all_room_ids = []

    print("\n🔍 Phase 1: Recherche des room_ids", flush=True)
    print(f"   Zones : {len(zones)}", flush=True)
    print(f"   Dates : {CHECK_IN} → {CHECK_OUT}\n", flush=True)

    for idx, zone in enumerate(zones, start=1):
        print(
            f"[{idx}/{len(zones)}] 📍 {zone['name']} "
            f"({zone['sw_lat']:.4f},{zone['sw_long']:.4f}) → "
            f"({zone['ne_lat']:.4f},{zone['ne_long']:.4f})...",
            end=" ",
            flush=True,
        )

        try:
            search_results = search_zone_with_retry(zone)
            if not isinstance(search_results, list):
                # Certains cas : structure enveloppée
                search_results = nested(search_results, ["results"], default=[])
            print(f"✓ {len(search_results)} résultats", flush=True)

            for result in search_results:
                room_id = get_room_id_from_result(result)
                if room_id:
                    all_room_ids.append(room_id)

        except Exception as e:
            print(f"❌ Erreur: {e}", flush=True)

        if idx < len(zones):
            time.sleep(DELAY_BETWEEN_ZONES)

    unique_ids = list(set(all_room_ids))
    print(
        f"\n✅ Phase 1 terminée: {len(unique_ids)} room_ids uniques trouvés\n",
        flush=True,
    )
    return unique_ids


@retry_on_failure(max_retries=3, delay=2)
def get_listing_details(room_id):
    """Récupère les détails complets d'un listing"""
    return pyairbnb.get_details(
        room_id=int(room_id),
        currency=CURRENCY,
        proxy_url=PROXY_URL,
        adults=2,
        language=LANGUAGE,
    )


def extract_listing_data(room_id, details):
    """
    Extrait toutes les données nécessaires depuis les détails.
    Retourne None si le listing est hors de Dubai (filtre lat/lon).
    """

    # ---------------------
    # Localisation → filtre Dubai
    # ---------------------
    lat = nested(
        details,
        [
            "pdp_listing_detail.lat",
            "pdp_listing_detail.latitude",
            "listing.lat",
            "listing.latitude",
        ],
        default="",
    )
    lng = nested(
        details,
        [
            "pdp_listing_detail.lng",
            "pdp_listing_detail.longitude",
            "listing.lng",
            "listing.longitude",
        ],
        default="",
    )

    try:
        if lat and lng:
            lat_f = float(lat)
            lng_f = float(lng)
            # Même bounding box que build_dubai_subzones, un peu de marge
            if not (24.80 <= lat_f <= 25.40 and 54.90 <= lng_f <= 55.50):
                # Hors Dubai → on ignore ce listing
                print(
                    f"   ↳ Ignoré (hors Dubai) lat={lat_f:.4f}, lng={lng_f:.4f}",
                    flush=True,
                )
                return None
    except Exception:
        pass  # si lat/lng illisibles, on ne filtre pas

    # ---------------------
    # Listing info
    # ---------------------
    listing_title = nested(
        details,
        [
            "pdp_listing_detail.name",
            "listing.name",
            "listing.title",
            "name",
            "title",
        ],
        default="",
    )

    description = nested(
        details,
        [
            "pdp_listing_detail.description",
            "listing.description",
            "description",
        ],
        default="",
    )

    license_code = extract_license_code(description)

    # ---------------------
    # Host info
    # ---------------------
    host_id = nested(
        details,
        [
            "pdp_listing_detail.primary_host.id",
            "primary_host.id",
            "listing.primary_host.id",
            "listing.user.id",
            "user.id",
        ],
        default="",
    )

    host_name = nested(
        details,
        [
            "pdp_listing_detail.primary_host.full_name",
            "pdp_listing_detail.primary_host.first_name",
            "primary_host.full_name",
            "primary_host.first_name",
            "listing.primary_host.full_name",
            "listing.primary_host.first_name",
            "user.first_name",
            "user.name",
        ],
        default="",
    )

    host_rating = nested(
        details,
        [
            "pdp_listing_detail.primary_host.overall_rating",
            "primary_host.overall_rating",
            "pdp_listing_detail.primary_host.overall_rating_localized",
            "primary_host.overall_rating_localized",
        ],
        default="",
    )

    host_reviews_count = nested(
        details,
        [
            "pdp_listing_detail.primary_host.review_count",
            "primary_host.review_count",
            "pdp_listing_detail.primary_host.reviews_count",
            "primary_host.reviews_count",
        ],
        default="",
    )

    member_since = nested(
        details,
        [
            "pdp_listing_detail.primary_host.member_since",
            "primary_host.member_since",
        ],
        default="",
    )

    host_joined_year = ""
    host_years_active = ""

    if isinstance(member_since, str) and len(member_since) >= 4:
        try:
            joined_year = int(member_since[:4])
            host_joined_year = joined_year
            host_years_active = datetime.now().year - joined_year
        except Exception:
            pass

    return {
        "room_id": room_id,
        "listing_url": f"https://www.airbnb.com/rooms/{room_id}",
        "listing_title": listing_title,
        "license_code": license_code,
        "host_id": str(host_id) if host_id else "",
        "host_name": host_name,
        "host_profile_url": (
            f"https://www.airbnb.com/users/show/{host_id}" if host_id else ""
        ),
        "host_rating": host_rating,
        "host_reviews_count": host_reviews_count,
        "host_joined_year": host_joined_year,
        "host_years_active": host_years_active,
    }


def write_csv(records):
    """Écrit tous les records dans le CSV"""
    fieldnames = [
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

    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)


def scrape_dubai_incremental():
    """
    Scraping incrémental avec sauvegarde Git progressive
    """
    start_time = time.time()

    print("=" * 80)
    print(f"🚀 SCRAPING DUBAI - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print(f"📊 Configuration: {LISTINGS_PER_RUN} listings ce run")
    print("=" * 80 + "\n")

    processed_ids = load_processed_ids()
    existing_records = load_existing_csv()

    # Phase 1
    all_room_ids = collect_all_room_ids()

    remaining_ids = [rid for rid in all_room_ids if rid not in processed_ids]

    print("📊 Statut:")
    print(f"   • Total Dubai (IDs trouvés): {len(all_room_ids)}")
    print(f"   • Déjà traités: {len(processed_ids)}")
    print(f"   • Restants: {len(remaining_ids)}")
    print(f"   • Ce run: {min(LISTINGS_PER_RUN, len(remaining_ids))}\n")

    if not remaining_ids:
        print("✅ TOUS LES LISTINGS SONT DÉJÀ TRAITÉS!")
        print(f"📊 Total final: {len(processed_ids)} listings dans {CSV_FILE}\n")
        return

    to_process = remaining_ids[:LISTINGS_PER_RUN]

    print(
        f"🔍 Phase 2: Extraction des détails ({len(to_process)} listings)\n",
        flush=True,
    )

    new_records = []
    commit_counter = 0

    for idx, room_id in enumerate(to_process, start=1):
        print(f"[{idx}/{len(to_process)}] 🏠 Listing {room_id}...", flush=True)

        try:
            details = get_listing_details(room_id)
            record = extract_listing_data(room_id, details)

            # Si hors Dubai, record = None
            if record is None:
                save_processed_id(room_id)
                continue

            new_records.append(record)
            save_processed_id(room_id)

            print(
                f"   ✓ host: {record['host_name'] or 'N/A'} | "
                f"rating: {record['host_rating'] or 'N/A'} | "
                f"license: {record['license_code'] or 'N/A'}",
                flush=True,
            )

            commit_counter += 1
            if commit_counter >= COMMIT_EVERY:
                all_records = existing_records + new_records

                # Calcul des totaux par host
                host_count = {}
                for rec in all_records:
                    hid = rec.get("host_id")
                    if hid:
                        host_count[hid] = host_count.get(hid, 0) + 1
                for rec in all_records:
                    hid = rec.get("host_id")
                    rec["host_total_listings_in_dubai"] = (
                        host_count.get(hid, 0) if hid else 0
                    )

                write_csv(all_records)
                git_commit_and_push(
                    f"Progress: +{commit_counter} listings "
                    f"(total: {len(all_records)})"
                )
                commit_counter = 0

        except Exception as e:
            print(f"❌ Erreur sur {room_id}: {e}", flush=True)

        time.sleep(DELAY_BETWEEN_DETAILS)

    # Fin de run : recompute host_total_listings_in_dubai globalement
    print(f"\n📊 Calcul des totaux par host...", flush=True)
    all_records = existing_records + new_records

    host_count = {}
    for rec in all_records:
        hid = rec.get("host_id")
        if hid:
            host_count[hid] = host_count.get(hid, 0) + 1

    for rec in all_records:
        hid = rec.get("host_id")
        rec["host_total_listings_in_dubai"] = host_count.get(hid, 0) if hid else 0

    write_csv(all_records)

    if commit_counter > 0 or new_records:
        git_commit_and_push(
            f"Completed run: +{len(new_records)} listings "
            f"(total: {len(all_records)})"
        )

    elapsed = time.time() - start_time
    print("\n" + "=" * 80)
    print(f"🎉 RUN TERMINÉ en {elapsed/60:.1f} minutes")
    print("=" * 80)
    print(f"📊 Ce run: +{len(new_records)} listings")
    print(f"📊 Total dans CSV: {len(all_records)} listings")
    print(f"📊 Restants: {len(remaining_ids) - len(to_process)}")

    if len(remaining_ids) - len(to_process) > 0:
        print("\n💡 Pour continuer: relance le workflow")
        print("   (ou augmente LISTINGS_PER_RUN si tu veux aller plus vite)")
    else:
        print("\n✅ SCRAPING COMPLET DE DUBAI!")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    scrape_dubai_incremental()
