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
ZOOM_VALUE = 4  # Plus précis pour cibler Dubai uniquement

DELAY_BETWEEN_DETAILS = 1.0  # Délai entre appels get_details
DELAY_BETWEEN_HOSTS = 1.5    # Délai entre appels get_host_details
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
                        print(f"❌ Échec final après {max_retries} tentatives: {e}", flush=True)
                        raise
                    wait_time = delay * (2 ** attempt)
                    print(f"⚠️ Tentative {attempt + 1}/{max_retries} échouée: {e}. Retry dans {wait_time}s", flush=True)
                    time.sleep(wait_time)
            return None
        return wrapper
    return decorator


def build_dubai_city_subzones(rows=4, cols=5):
    """
    Divise DUBAI VILLE en sous-zones précises
    Coordonnées resserrées pour éviter Abu Dhabi et autres émirats
    """
    # Coordonnées PRÉCISES de Dubai ville uniquement
    north = 25.3463   # Dubai Marina / Palm Jumeirah (limite nord)
    south = 24.7743   # Dubai Creek / Al Nahda (limite sud)
    east = 55.5224    # International City / Dragon Mart (limite est)
    west = 54.9493    # JBR / The Walk (limite ouest)

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
                "name": f"dubai_{r+1}_{c+1}",
                "ne_lat": z_ne_lat,
                "ne_long": z_ne_lng,
                "sw_lat": z_sw_lat,
                "sw_long": z_sw_lng,
            })
    
    print(f"📍 Zones créées pour DUBAI VILLE uniquement (4x5 = 20 zones)", flush=True)
    print(f"📍 Limites: {south:.4f}°N à {north:.4f}°N, {west:.4f}°E à {east:.4f}°E\n", flush=True)
    
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
        print(f"⚠️ Git commit échoué (normal si rien à commiter): {e}", flush=True)
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
    """Phase 1: Récupère tous les room_ids de Dubai ville uniquement"""
    zones = build_dubai_city_subzones(rows=4, cols=5)
    all_room_ids = []
    
    print(f"🔍 Phase 1: Recherche des room_ids dans {len(zones)} zones de DUBAI VILLE", flush=True)
    print(f"📅 Dates: {CHECK_IN} → {CHECK_OUT}\n", flush=True)

    for idx, zone in enumerate(zones, start=1):
        print(f"[{idx}/{len(zones)}] 📍 Zone {zone['name']}...", end=" ", flush=True)

        try:
            search_results = search_zone_with_retry(zone)
            
            if not search_results or len(search_results) == 0:
                print(f"⚠️ 0 résultats", flush=True)
                continue
            
            print(f"✓ {len(search_results)} résultats", flush=True)
            
            # Extraire room_id avec chemins multiples
            for result in search_results:
                room_id = None
                
                # Chemins possibles pour room_id
                if isinstance(result, dict):
                    room_id = (
                        result.get("room_id") or 
                        result.get("id") or 
                        result.get("listing", {}).get("id") or
                        result.get("listing", {}).get("room_id")
                    )
                
                if room_id:
                    all_room_ids.append(str(room_id))

        except Exception as e:
            print(f"❌ Erreur: {e}", flush=True)
        
        if idx < len(zones):
            time.sleep(DELAY_BETWEEN_ZONES)
    
    # Déduplication
    unique_ids = list(set(all_room_ids))
    print(f"\n✅ Phase 1 terminée: {len(unique_ids)} room_ids uniques trouvés à DUBAI\n", flush=True)
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


@retry_on_failure(max_retries=3, delay=2)
def get_host_full_details(host_id):
    """
    Récupère les détails COMPLETS du host via get_host_details()
    CETTE fonction est la clé pour avoir toutes les infos du host !
    """
    return pyairbnb.get_host_details(
        host_id=host_id,
        proxy_url=PROXY_URL,
    )


@retry_on_failure(max_retries=3, delay=1)
def get_host_listings_count(host_id):
    """
    Récupère TOUS les listings d'un host pour compter combien il en a
    """
    try:
        listings = pyairbnb.get_listings_from_user(
            host_id=host_id,
            proxy_url=PROXY_URL,
        )
        return len(listings) if listings else 0
    except:
        return 0


def extract_listing_data(room_id, details, host_cache):
    """
    Extrait toutes les données nécessaires depuis les détails
    Utilise get_host_details() pour les infos complètes du host
    """
    
    # ==================
    # LISTING INFO
    # ==================
    listing_title = ""
    description = ""
    host_id = ""
    
    # Titre du listing - chemins multiples
    title_paths = [
        ["pdp_listing_detail", "name"],
        ["listing", "name"],
        ["name"],
        ["title"],
    ]
    
    for path in title_paths:
        try:
            value = details
            for key in path:
                if isinstance(value, dict):
                    value = value.get(key)
                else:
                    value = None
                    break
            if value and isinstance(value, str):
                listing_title = value
                break
        except:
            continue
    
    # Description - pour extraire license_code
    desc_paths = [
        ["pdp_listing_detail", "description"],
        ["listing", "description"],
        ["description"],
    ]
    
    for path in desc_paths:
        try:
            value = details
            for key in path:
                if isinstance(value, dict):
                    value = value.get(key)
                else:
                    value = None
                    break
            if value and isinstance(value, str):
                description = value
                break
        except:
            continue
    
    license_code = extract_license_code(description)
    
    # Host ID - CRITIQUE pour get_host_details()
    host_id_paths = [
        ["pdp_listing_detail", "primary_host", "id"],
        ["primary_host", "id"],
        ["listing", "primary_host", "id"],
        ["listing", "user", "id"],
        ["user", "id"],
        ["host_id"],
    ]
    
    for path in host_id_paths:
        try:
            value = details
            for key in path:
                if isinstance(value, dict):
                    value = value.get(key)
                else:
                    value = None
                    break
            if value:
                host_id = str(value)
                break
        except:
            continue
    
    # ==================
    # HOST INFO via get_host_details()
    # ==================
    host_name = ""
    host_rating = ""
    host_reviews_count = ""
    host_joined_year = ""
    host_years_active = ""
    host_total_listings = 0
    
    if host_id:
        # Utiliser le cache pour éviter les appels répétés
        if host_id not in host_cache:
            print(f"    → Récupération infos host {host_id}...", end=" ", flush=True)
            try:
                host_details = get_host_full_details(host_id)
                
                if host_details and isinstance(host_details, dict):
                    # Nom du host
                    host_name = (
                        host_details.get("first_name") or
                        host_details.get("name") or
                        ""
                    )
                    
                    # Rating du host
                    host_rating = (
                        host_details.get("overall_rating") or
                        host_details.get("rating") or
                        host_details.get("guest_rating") or
                        ""
                    )
                    
                    # Nombre de reviews
                    host_reviews_count = (
                        host_details.get("review_count") or
                        host_details.get("reviews_count") or
                        host_details.get("number_of_reviews") or
                        ""
                    )
                    
                    # Année d'inscription
                    member_since = (
                        host_details.get("member_since") or
                        host_details.get("created_at") or
                        ""
                    )
                    
                    if isinstance(member_since, str) and len(member_since) >= 4:
                        try:
                            joined_year = int(member_since[:4])
                            host_joined_year = joined_year
                            host_years_active = datetime.now().year - joined_year
                        except:
                            pass
                    
                    # Compter les listings du host
                    host_total_listings = get_host_listings_count(host_id)
                    
                    # Cacher les infos du host
                    host_cache[host_id] = {
                        "name": host_name,
                        "rating": host_rating,
                        "reviews_count": host_reviews_count,
                        "joined_year": host_joined_year,
                        "years_active": host_years_active,
                        "total_listings": host_total_listings,
                    }
                    
                    print(f"✓ {host_name} ({host_total_listings} listings)", flush=True)
                    time.sleep(DELAY_BETWEEN_HOSTS)
                
            except Exception as e:
                print(f"❌ {e}", flush=True)
                host_cache[host_id] = {}
        else:
            # Utiliser le cache
            cached = host_cache[host_id]
            host_name = cached.get("name", "")
            host_rating = cached.get("rating", "")
            host_reviews_count = cached.get("reviews_count", "")
            host_joined_year = cached.get("joined_year", "")
            host_years_active = cached.get("years_active", "")
            host_total_listings = cached.get("total_listings", 0)
            print(f"    → Cache: {host_name}", flush=True)
    
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
        "host_total_listings_in_dubai": host_total_listings,
    }


def scrape_dubai_incremental():
    """
    Scraping incrémental avec sauvegarde Git progressive
    Utilise get_host_details() pour infos complètes des hosts
    """
    start_time = time.time()
    
    print("=" * 80)
    print(f"🚀 SCRAPING DUBAI VILLE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print(f"📊 Configuration: {LISTINGS_PER_RUN} listings ce run")
    print("=" * 80 + "\n")
    
    # Charger l'historique
    processed_ids = load_processed_ids()
    existing_records = load_existing_csv()
    
    # Phase 1: Récupérer tous les room_ids de DUBAI
    all_room_ids = collect_all_room_ids()
    
    if len(all_room_ids) == 0:
        print("❌ AUCUN LISTING TROUVÉ ! Vérifier les coordonnées géographiques.\n")
        return
    
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
    host_cache = {}  # Cache pour éviter d'appeler get_host_details() plusieurs fois
    
    for idx, room_id in enumerate(to_process, start=1):
        print(f"[{idx}/{len(to_process)}] 🏠 Listing {room_id}...", end=" ", flush=True)
        
        try:
            details = get_listing_details(room_id)
            
            if not details:
                print(f"❌ Pas de détails", flush=True)
                continue
            
            record = extract_listing_data(room_id, details, host_cache)
            new_records.append(record)
            save_processed_id(room_id)
            
            print(f"✓ {record['listing_title'][:40]}... (license: {record['license_code'] or 'N/A'})", flush=True)
            
            # Commit Git tous les COMMIT_EVERY listings
            commit_counter += 1
            if commit_counter >= COMMIT_EVERY:
                all_records = existing_records + new_records
                write_csv(all_records)
                git_commit_and_push(f"Progress: +{commit_counter} listings (total: {len(all_records)})")
                commit_counter = 0
            
        except Exception as e:
            print(f"❌ Erreur: {e}", flush=True)
        
        time.sleep(DELAY_BETWEEN_DETAILS)
    
    # Écriture CSV finale
    all_records = existing_records + new_records
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
    print(f"📊 Hosts uniques dans cache: {len(host_cache)}")
    
    if len(remaining_ids) - len(to_process) > 0:
        print(f"\n💡 Pour continuer: relance le workflow")
        print(f"   (ou change LISTINGS_PER_RUN pour aller plus vite)")
    else:
        print(f"\n✅ SCRAPING COMPLET DE DUBAI VILLE!")
    
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
