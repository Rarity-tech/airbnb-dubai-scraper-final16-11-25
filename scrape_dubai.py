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
LISTINGS_PER_RUN = 3000  # ← MODIFIE CE NOMBRE: 200, 1000, 5000, ou 999999


# ==========================
# CONFIG GLOBALE
# ==========================
future_date = datetime.now() + timedelta(days=2)
CHECK_IN = future_date.strftime("%Y-%m-%d")
CHECK_OUT = (future_date + timedelta(days=3)).strftime("%Y-%m-%d")

CURRENCY = "AED"
LANGUAGE = "en"
PROXY_URL = ""
ZOOM_VALUE = 9

DELAY_BETWEEN_DETAILS = 1.0
DELAY_BETWEEN_ZONES = 2.0
COMMIT_EVERY = 50

CSV_FILE = "dubai_listings.csv"
PROCESSED_IDS_FILE = "processed_ids.txt"

# Cache global pour API key et cookies
API_KEY = None
COOKIES = {}


# ==========================
# UTILITAIRES
# ==========================

def get_api_credentials():
    """Récupère l'API key et les cookies une seule fois"""
    global API_KEY, COOKIES
    
    if API_KEY is None:
        try:
            API_KEY = pyairbnb.get_api_key(PROXY_URL)  # ← CORRECTION ICI
            print(f"✅ API Key récupérée", flush=True)
        except Exception as e:
            print(f"⚠️ Impossible de récupérer l'API key: {e}", flush=True)
            API_KEY = ""
    
    return API_KEY, COOKIES


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
                    print(f"⚠️ Tentative {attempt + 1}/{max_retries} échouée. Retry dans {wait_time}s", flush=True)
                    time.sleep(wait_time)
            return None
        return wrapper
    return decorator


def build_dubai_city_subzones(rows=4, cols=5):
    """Zones précises de Dubai ville"""
    north = 25.3463
    south = 24.7743
    east = 55.5224
    west = 54.9493

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
    
    return zones


def extract_license_code(text):
    """Extrait le license code depuis la description - capture TOUT après 'Registration Details' jusqu'à virgule"""
    if not text:
        return ""
    
    # Convertir en string et nettoyer les balises HTML
    text_str = str(text)
    text_clean = re.sub(r'<[^>]+>', ' ', text_str)
    
    # Chercher après les mots-clés de registration
    keywords = [
        r'Registration\s+Details?',
        r'Registration\s+(?:Number|No\.?|Code)',
        r'License\s+(?:Number|No\.?|Code)',
        r'Permit\s+(?:Number|No\.?)',
    ]
    
    pattern = r'(?:' + '|'.join(keywords) + r')[:\s]*([^,\n]+)'
    
    match = re.search(pattern, text_clean, re.IGNORECASE)
    
    if match:
        code = match.group(1).strip()
        # Nettoyer les espaces multiples
        code = ' '.join(code.split())
        return code
    
    return ""


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
    except subprocess.CalledProcessError:
        return False


def load_processed_ids():
    """Charge les IDs déjà traités"""
    if os.path.exists(PROCESSED_IDS_FILE):
        with open(PROCESSED_IDS_FILE, 'r') as f:
            ids = set(line.strip() for line in f if line.strip())
        print(f"📂 {len(ids)} listings déjà traités", flush=True)
        return ids
    return set()


def save_processed_id(room_id):
    """Sauvegarde un ID comme traité"""
    with open(PROCESSED_IDS_FILE, 'a') as f:
        f.write(f"{room_id}\n")


def load_existing_csv():
    """Charge le CSV existant"""
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
    """Recherche dans une zone"""
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
    )


def collect_all_room_ids():
    """Phase 1: Récupère tous les room_ids"""
    zones = build_dubai_city_subzones(rows=4, cols=5)
    all_room_ids = []
    
    print(f"🔍 Phase 1: Recherche des room_ids dans {len(zones)} zones", flush=True)
    print(f"📅 Dates: {CHECK_IN} → {CHECK_OUT}\n", flush=True)

    for idx, zone in enumerate(zones, start=1):
        print(f"[{idx}/{len(zones)}] 📍 Zone {zone['name']}...", end=" ", flush=True)

        try:
            search_results = search_zone_with_retry(zone)
            
            if not search_results:
                print(f"⚠️ 0 résultats", flush=True)
                continue
            
            print(f"✓ {len(search_results)} résultats", flush=True)
            
            for result in search_results:
                room_id = None
                if isinstance(result, dict):
                    room_id = (
                        result.get("room_id") or 
                        result.get("id") or 
                        result.get("listing", {}).get("id")
                    )
                
                if room_id:
                    all_room_ids.append(str(room_id))

        except Exception as e:
            print(f"❌ Erreur: {e}", flush=True)
        
        if idx < len(zones):
            time.sleep(DELAY_BETWEEN_ZONES)
    
    unique_ids = list(set(all_room_ids))
    print(f"\n✅ Phase 1 terminée: {len(unique_ids)} room_ids uniques\n", flush=True)
    return unique_ids


@retry_on_failure(max_retries=3, delay=2)
def get_listing_details(room_id):
    """Récupère les détails d'un listing"""
    return pyairbnb.get_details(
        room_id=room_id,
        currency=CURRENCY,
        proxy_url=PROXY_URL,
        language=LANGUAGE,
    )


def extract_listing_data(room_id, details, host_cache):
    """Extrait toutes les données depuis get_details()"""
    
    # Titre
    listing_title = details.get("title", "")
    
    # Description (pour license_code)
    description = details.get("description", "")
    license_code = extract_license_code(description)
    
    # HOST_ID depuis details["host"]["id"]
    host_id = ""
    host_data = details.get("host", {})
    if isinstance(host_data, dict):
        host_id = str(host_data.get("id", ""))
    
    # Données du host (valeurs par défaut)
    host_name = ""
    host_rating = ""
    host_reviews_count = ""
    host_joined_year = ""
    host_years_active = ""
    host_total_listings = 0
    
    if host_id and host_id not in host_cache:
        # Récupérer les credentials API
        api_key, cookies = get_api_credentials()
        
        try:
            # Appeler get_host_details
            host_details_response = pyairbnb.get_host_details(
                api_key=api_key,
                cookies=cookies,
                host_id=host_id,
                language=LANGUAGE,
                proxy_url=PROXY_URL,
            )
            
            if host_details_response and isinstance(host_details_response, dict):
                # Vérifier si erreur API (profil invalide, permission denied, etc.)
                if "errors" in host_details_response:
                    print(f"⚠️ Host {host_id}: profil non accessible", flush=True)
                    host_cache[host_id] = {}
                else:
                    # Structure JSON exacte découverte dans les tests
                    data = host_details_response.get("data", {})
                    
                    # Chemin 1: data → node → hostRatingStats → ratingAverage
                    node = data.get("node", {})
                    host_rating_stats = node.get("hostRatingStats", {})
                    host_rating = host_rating_stats.get("ratingAverage", "")
                    
                    # Chemin 2: data → presentation → userProfileContainer → userProfile
                    presentation = data.get("presentation", {})
                    user_profile_container = presentation.get("userProfileContainer", {})
                    user_profile = user_profile_container.get("userProfile")
                    
                    if user_profile:
                        # Nom: smartName (comme "Caroline")
                        host_name = user_profile.get("smartName", "")
                        if not host_name:
                            host_name = user_profile.get("displayFirstName", "")
                        
                        # Reviews count
                        reviews_data = user_profile.get("reviewsReceivedFromGuests", {})
                        host_reviews_count = reviews_data.get("count", "")
                        
                        # Date de création et calcul des années
                        created_at = user_profile.get("createdAt", "")
                        if created_at:
                            try:
                                # Format ISO: "2018-02-22T04:47:06.000Z"
                                created_date = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                                host_joined_year = created_date.year
                                host_years_active = datetime.now().year - host_joined_year
                            except Exception as e:
                                print(f"⚠️ Date parsing error host {host_id}", flush=True)
                        
                        # Compter les listings du host
                        try:
                            host_listings = pyairbnb.get_listings_from_user(
                                host_id,
                                api_key,
                                PROXY_URL,
                            )
                            host_total_listings = len(host_listings) if host_listings else 0
                        except Exception as e:
                            print(f"⚠️ Erreur listings host {host_id}", flush=True)
                            host_total_listings = 0
                        
                        # Sauvegarder dans le cache
                        host_cache[host_id] = {
                            "name": host_name,
                            "rating": host_rating,
                            "reviews_count": host_reviews_count,
                            "joined_year": host_joined_year,
                            "years_active": host_years_active,
                            "total_listings": host_total_listings,
                        }
                    else:
                        # Pas de userProfile
                        print(f"⚠️ Host {host_id}: pas de userProfile", flush=True)
                        host_cache[host_id] = {}
                        
        except Exception as e:
            print(f"⚠️ Erreur host {host_id}: {e}", flush=True)
            host_cache[host_id] = {}
    
    elif host_id in host_cache:
        # Utiliser le cache
        cached = host_cache[host_id]
        host_name = cached.get("name", "")
        host_rating = cached.get("rating", "")
        host_reviews_count = cached.get("reviews_count", "")
        host_joined_year = cached.get("joined_year", "")
        host_years_active = cached.get("years_active", "")
        host_total_listings = cached.get("total_listings", 0)
    
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
    """Scraping incrémental avec checkpoint Git"""
    start_time = time.time()
    
    print("=" * 80)
    print(f"🚀 SCRAPING DUBAI - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print(f"📊 Configuration: {LISTINGS_PER_RUN} listings ce run")
    print("=" * 80 + "\n")
    
    processed_ids = load_processed_ids()
    existing_records = load_existing_csv()
    
    all_room_ids = collect_all_room_ids()
    
    if len(all_room_ids) == 0:
        print("❌ AUCUN LISTING TROUVÉ !\n")
        return
    
    remaining_ids = [rid for rid in all_room_ids if rid not in processed_ids]
    
    print(f"📊 Statut:")
    print(f"   • Total Dubai: {len(all_room_ids)} listings")
    print(f"   • Déjà traités: {len(processed_ids)}")
    print(f"   • Restants: {len(remaining_ids)}")
    print(f"   • Ce run: {min(LISTINGS_PER_RUN, len(remaining_ids))}\n")
    
    if len(remaining_ids) == 0:
        print("✅ TOUS LES LISTINGS SONT DÉJÀ TRAITÉS!\n")
        return
    
    to_process = remaining_ids[:LISTINGS_PER_RUN]
    
    print(f"🔍 Phase 2: Extraction des détails ({len(to_process)} listings)\n", flush=True)
    
    new_records = []
    commit_counter = 0
    host_cache = {}
    
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
            
            print(f"✓ {record['listing_title'][:30]}... | Host: {record['host_name'] or 'N/A'}", flush=True)
            
            commit_counter += 1
            if commit_counter >= COMMIT_EVERY:
                all_records = existing_records + new_records
                write_csv(all_records)
                git_commit_and_push(f"Progress: +{commit_counter} listings (total: {len(all_records)})")
                commit_counter = 0
            
        except Exception as e:
            print(f"❌ Erreur: {e}", flush=True)
        
        time.sleep(DELAY_BETWEEN_DETAILS)
    
    all_records = existing_records + new_records
    write_csv(all_records)
    
    if commit_counter > 0 or len(new_records) > 0:
        git_commit_and_push(f"Completed run: +{len(new_records)} listings (total: {len(all_records)})")
    
    elapsed = time.time() - start_time
    print("\n" + "=" * 80)
    print(f"🎉 RUN TERMINÉ en {elapsed/60:.1f} minutes")
    print("=" * 80)
    print(f"📊 Ce run: +{len(new_records)} listings")
    print(f"📊 Total dans CSV: {len(all_records)} listings")
    print(f"📊 Restants: {len(remaining_ids) - len(to_process)}")
    print(f"📊 Hosts uniques: {len(host_cache)}")
    
    if len(remaining_ids) - len(to_process) > 0:
        print(f"\n💡 Pour continuer: relance le workflow")
    else:
        print(f"\n✅ SCRAPING COMPLET DE DUBAI!")
    
    print("=" * 80 + "\n")


def write_csv(records):
    """Écrit le CSV"""
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
