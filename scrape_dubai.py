import csv
import os
import re
import subprocess
import time
from datetime import datetime, timedelta
from functools import wraps
import pyairbnb


# ==========================
# ⚙️ CONTRÔLE DU RUN - CHANGE CE NOMBRE SELON TON BESOIN
# ==========================
LISTINGS_PER_RUN = 200  # ← MODIFIE CE NOMBRE: 200, 1000, 5000, ou 999999 pour tout


# ==========================
# CONFIG GLOBALE
# ==========================
future_date = datetime.now() + timedelta(days=14)
CHECK_IN = future_date.strftime("%Y-%m-%d")
CHECK_OUT = (future_date + timedelta(days=5)).strftime("%Y-%m-%d")

CURRENCY = "AED"
LANGUAGE = "en"
PROXY_URL = ""
ZOOM_VALUE = 3

DELAY_BETWEEN_DETAILS = 0.5  # Délai entre appels get_details
DELAY_BETWEEN_ZONES = 2.0
COMMIT_EVERY = 50  # Commit Git tous les 50 listings

# Fichiers de sauvegarde
CSV_FILE = "dubai_listings.csv"
PROCESSED_IDS_FILE = "processed_ids.txt"


# ==========================
# UTILITAIRES
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
                    print(f"⚠️ Tentative {attempt + 1}/{max_retries} échouée: {e}. Retry dans {wait_time}s", flush=True)
                    time.sleep(wait_time)
            return None
        return wrapper
    return decorator


def build_dubai_subzones(rows=3, cols=4):
    """Divise Dubai en sous-zones"""
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
            
            zones.append({
                "name": f"zone_{r+1}_{c+1}",
                "ne_lat": z_ne_lat,
                "ne_long": z_ne_lng,
                "sw_lat": z_sw_lat,
                "sw_long": z_sw_lng,
            })
    return zones


def extract_license_code(text):
    """
    Extrait le license code de la description
    Format: BUS-MAG-42KDF (3 lettres - 3 lettres - code alphanumérique)
    """
    if not text:
        return ""
    
    # Pattern: 3 LETTRES - 3 LETTRES - 5-6 CARACTÈRES ALPHANUMÉRIQUES
    pattern = r'\b[A-Z]{3}-[A-Z]{3}-[A-Z0-9]{5,6}\b'
    matches = re.findall(pattern, str(text))
    
    # Retourner le premier match trouvé
    return matches[0] if matches else ""


def git_commit_and_push(message):
    """Commit et push vers GitHub"""
    try:
        subprocess.run(["git", "config", "user.name", "GitHub Actions"], check=True, capture_output=True)
        subprocess.run(["git", "config", "user.email", "actions@github.com"], check=True, capture_output=True)
        subprocess.run(["git", "add", CSV_FILE, PROCESSED_IDS_FILE], check=True, capture_output=True)
        subprocess.run(["git", "commit", "-m", message], check=True, capture_output=True)
        subprocess.run(["git", "push"], check=True, capture_output=True)
        print(f"✅ Git commit: {message}", flush=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Git commit échoué: {e}", flush=True)
        return False


def load_processed_ids():
    """Charge les IDs déjà traités"""
    if os.path.exists(PROCESSED_IDS_FILE):
        with open(PROCESSED_IDS_FILE, 'r') as f:
            ids = set(line.strip() for line in f if line.strip())
        print(f"📂 {len(ids)} listings déjà traités (chargés depuis {PROCESSED_IDS_FILE})", flush=True)
        return ids
    return set()


def save_processed_id(room_id):
    """Sauvegarde un ID comme traité"""
    with open(PROCESSED_IDS_FILE, 'a') as f:
        f.write(f"{room_id}\n")


def load_existing_csv():
    """Charge le CSV existant pour append"""
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            existing = list(reader)
        print(f"📂 {len(existing)} lignes déjà dans {CSV_FILE}", flush=True)
        return existing
    return []


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
        currency=CURRENCY,
        language=LANGUAGE,
        proxy_url=PROXY_URL,
    )


def collect_all_room_ids():
    """Phase 1: Récupère tous les room_ids de Dubai"""
    zones = build_dubai_subzones(rows=3, cols=4)
    all_room_ids = []
    
    print(f"\n🔍 Phase 1: Recherche des room_ids dans {len(zones)} zones", flush=True)
    print(f"📅 Dates: {CHECK_IN} → {CHECK_OUT}\n", flush=True)

    for idx, zone in enumerate(zones, start=1):
        print(f"[{idx}/{len(zones)}] 📍 Zone {zone['name']}...", end=" ", flush=True)

        try:
            search_results = search_zone_with_retry(zone)
            print(f"✓ {len(search_results)} résultats", flush=True)
            
            for result in search_results:
                room_id = result.get("room_id")
                if room_id:
                    all_room_ids.append(str(room_id))

        except Exception as e:
            print(f"❌ Erreur: {e}", flush=True)
        
        if idx < len(zones):
            time.sleep(DELAY_BETWEEN_ZONES)
    
    # Déduplication
    unique_ids = list(set(all_room_ids))
    print(f"\n✅ Phase 1 terminée: {len(unique_ids)} room_ids uniques trouvés\n", flush=True)
    return unique_ids


@retry_on_failure(max_retries=3, delay=2)
def get_listing_details(room_id):
    """Récupère les détails complets d'un listing"""
    return pyairbnb.get_details(
        room_id=room_id,
        currency=CURRENCY,
        proxy_url=PROXY_URL,
        adults=2,
        language=LANGUAGE,
    )


def extract_listing_data(room_id, details):
    """Extrait toutes les données nécessaires depuis les détails"""
    
    # Listing info
    listing_title = ""
    description = ""
    
    # Chemins possibles pour le titre
    for path in ["pdp_listing_detail.name", "listing.name", "name", "title"]:
        try:
            parts = path.split(".")
            value = details
            for part in parts:
                value = value.get(part) if isinstance(value, dict) else None
                if value is None:
                    break
            if value:
                listing_title = value
                break
        except:
            pass
    
    # Chemins possibles pour la description (pour extraire license_code)
    for path in ["pdp_listing_detail.description", "listing.description", "description"]:
        try:
            parts = path.split(".")
            value = details
            for part in parts:
                value = value.get(part) if isinstance(value, dict) else None
                if value is None:
                    break
            if value:
                description = value
                break
        except:
            pass
    
    # Extraire license code
    license_code = extract_license_code(description)
    
    # Host info
    host_id = ""
    host_name = ""
    host_rating = ""
    host_reviews_count = ""
    host_joined_year = ""
    host_years_active = ""
    
    # Chemins pour host
    host_paths = [
        "pdp_listing_detail.primary_host",
        "primary_host",
        "listing.primary_host",
        "listing.user",
        "user"
    ]
    
    host_data = None
    for path in host_paths:
        try:
            parts = path.split(".")
            value = details
            for part in parts:
                value = value.get(part) if isinstance(value, dict) else None
                if value is None:
                    break
            if value and isinstance(value, dict):
                host_data = value
                break
        except:
            pass
    
    if host_data:
        host_id = str(host_data.get("id", ""))
        host_name = host_data.get("first_name") or host_data.get("name") or ""
        host_rating = host_data.get("overall_rating") or host_data.get("rating") or ""
        host_reviews_count = host_data.get("review_count") or host_data.get("reviews_count") or ""
        
        # Année d'inscription
        member_since = host_data.get("member_since", "")
        if isinstance(member_since, str) and len(member_since) >= 4:
            try:
                joined_year = int(member_since[:4])
                host_joined_year = joined_year
                host_years_active = datetime.now().year - joined_year
            except:
                pass
    
    return {
        "room_id": room_id,
        "listing_url": f"https://www.airbnb.com/rooms/{room_id}",
        "listing_title": listing_title,
        "license_code": license_code,
        "host_id": host_id,
        "host_name": host_name,
        "host_profile_url": f"https://www.airbnb.com/users/show/{host_id}" if host_id else "",
        "host_rating": host_rating,
        "host_reviews_count": host_reviews_count,
        "host_joined_year": host_joined_year,
        "host_years_active": host_years_active,
    }


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
    
    # Charger l'historique
    processed_ids = load_processed_ids()
    existing_records = load_existing_csv()
    
    # Phase 1: Récupérer tous les room_ids
    all_room_ids = collect_all_room_ids()
    
    # Filtrer les IDs déjà traités
    remaining_ids = [rid for rid in all_room_ids if rid not in processed_ids]
    
    print(f"📊 Statut:")
    print(f"   • Total Dubai: {len(all_room_ids)} listings")
    print(f"   • Déjà traités: {len(processed_ids)}")
    print(f"   • Restants: {len(remaining_ids)}")
    print(f"   • Ce run: {min(LISTINGS_PER_RUN, len(remaining_ids))}\n")
    
    if len(remaining_ids) == 0:
        print("✅ TOUS LES LISTINGS SONT DÉJÀ TRAITÉS!")
        print(f"📊 Total final: {len(processed_ids)} listings dans {CSV_FILE}\n")
        return
    
    # Limiter au nombre demandé
    to_process = remaining_ids[:LISTINGS_PER_RUN]
    
    print(f"🔍 Phase 2: Extraction des détails ({len(to_process)} listings)\n", flush=True)
    
    new_records = []
    commit_counter = 0
    
    for idx, room_id in enumerate(to_process, start=1):
        print(f"[{idx}/{len(to_process)}] 🏠 Listing {room_id}...", end=" ", flush=True)
        
        try:
            details = get_listing_details(room_id)
            record = extract_listing_data(room_id, details)
            new_records.append(record)
            save_processed_id(room_id)
            
            print(f"✓ (license: {record['license_code'] or 'N/A'})", flush=True)
            
            # Commit Git tous les COMMIT_EVERY listings
            commit_counter += 1
            if commit_counter >= COMMIT_EVERY:
                # Sauvegarder le CSV mis à jour
                all_records = existing_records + new_records
                write_csv(all_records)
                
                git_commit_and_push(f"Progress: +{commit_counter} listings (total: {len(all_records)})")
                commit_counter = 0
            
        except Exception as e:
            print(f"❌ Erreur: {e}", flush=True)
        
        time.sleep(DELAY_BETWEEN_DETAILS)
    
    # Calcul host_total_listings_in_dubai
    print(f"\n📊 Calcul des totaux par host...", flush=True)
    all_records = existing_records + new_records
    
    host_count = {}
    for record in all_records:
        host_id = record.get("host_id")
        if host_id:
            host_count[host_id] = host_count.get(host_id, 0) + 1
    
    for record in all_records:
        host_id = record.get("host_id")
        record["host_total_listings_in_dubai"] = host_count.get(host_id, 0) if host_id else 0
    
    # Écriture CSV finale
    write_csv(all_records)
    
    # Commit final
    if commit_counter > 0 or len(new_records) > 0:
        git_commit_and_push(f"Completed run: +{len(new_records)} listings (total: {len(all_records)})")
    
    # Stats finales
    elapsed = time.time() - start_time
    print("\n" + "=" * 80)
    print(f"🎉 RUN TERMINÉ en {elapsed/60:.1f} minutes")
    print("=" * 80)
    print(f"📊 Ce run: +{len(new_records)} listings")
    print(f"📊 Total dans CSV: {len(all_records)} listings")
    print(f"📊 Restants: {len(remaining_ids) - len(to_process)}")
    
    if len(remaining_ids) - len(to_process) > 0:
        print(f"\n💡 Pour continuer: relance le workflow")
        print(f"   (ou change LISTINGS_PER_RUN pour aller plus vite)")
    else:
        print(f"\n✅ SCRAPING COMPLET DE DUBAI!")
    
    print("=" * 80 + "\n")


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


if __name__ == "__main__":
    scrape_dubai_incremental()
