import csv
import json
import os
import time
from datetime import datetime, timedelta
from functools import wraps

import pyairbnb


# ==========================
# CONFIG GLOBALE
# ==========================

# Dates dynamiques (2 semaines dans le futur pour max résultats)
future_date = datetime.now() + timedelta(days=14)
CHECK_IN = future_date.strftime("%Y-%m-%d")
CHECK_OUT = (future_date + timedelta(days=5)).strftime("%Y-%m-%d")

CURRENCY = "AED"
LANGUAGE = "en"
PROXY_URL = ""

# Zoom recommandé pour grande ville (cf. doc pyairbnb)
ZOOM_VALUE = 3

# Délais anti-rate-limit
DELAY_BETWEEN_DETAIL_CALLS = 0.5  # Augmenté à 0.5s pour sécurité
DELAY_BETWEEN_ZONES = 2.0  # Pause entre zones

# Checkpoints
CHECKPOINT_FILE = "checkpoint_progress.json"
BATCH_SIZE = 50  # Sauvegarder tous les 50 listings


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
                        print(f"❌ Échec définitif après {max_retries} tentatives: {e}", flush=True)
                        raise
                    wait_time = delay * (2 ** attempt)
                    print(f"⚠️ Tentative {attempt + 1}/{max_retries} échouée: {e}. Retry dans {wait_time}s", flush=True)
                    time.sleep(wait_time)
            return None
        return wrapper
    return decorator


def build_dubai_subzones(rows=3, cols=4, overlap_percent=0.1):
    """
    Divise Dubai en sous-zones avec overlap pour ne rien manquer.
    
    Coordonnées Dubai:
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
    
    # Overlap pour sécurité
    lat_overlap = lat_step * overlap_percent
    lng_overlap = lng_step * overlap_percent

    zones = []
    for r in range(rows):
        for c in range(cols):
            z_sw_lat = max(south, south + r * lat_step - lat_overlap)
            z_sw_lng = max(west, west + c * lng_step - lng_overlap)
            z_ne_lat = min(north, z_sw_lat + lat_step + lat_overlap)
            z_ne_lng = min(east, z_sw_lng + lng_step + lng_overlap)
            
            zones.append({
                "name": f"zone_{r+1}_{c+1}",
                "ne_lat": z_ne_lat,
                "ne_long": z_ne_lng,
                "sw_lat": z_sw_lat,
                "sw_long": z_sw_lng,
            })
    return zones


def try_paths(obj, paths, default=""):
    """Essaie plusieurs chemins possibles dans un JSON"""
    for p in paths:
        try:
            val = pyairbnb.get_nested_value(obj, p, None)
        except Exception:
            val = None
        if val not in (None, "", []):
            return val
    return default


def safe_int(value, default=None):
    """Conversion sécurisée en int"""
    try:
        return int(value)
    except Exception:
        return default


# ==========================
# SCRAPING OPTIMISÉ
# ==========================

@retry_on_failure(max_retries=3, delay=2)
def search_zone_with_retry(zone):
    """Recherche dans une zone avec retry automatique"""
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


def collect_listings_with_basic_info():
    """
    Phase 1: Récupère tous les listings avec infos basiques host
    depuis search_all (SANS appeler get_details)
    """
    zones = build_dubai_subzones(rows=3, cols=4, overlap_percent=0.1)
    listings_data = {}  # Clé = listing_id
    
    print(f"\n🔍 Phase 1: Recherche dans {len(zones)} sous-zones de Dubai", flush=True)
    print(f"📅 Dates: {CHECK_IN} → {CHECK_OUT}\n", flush=True)

    for idx, zone in enumerate(zones, start=1):
        print(f"[{idx}/{len(zones)}] 📍 Zone {zone['name']}...", end=" ", flush=True)

        try:
            search_results = search_zone_with_retry(zone)
        except Exception as e:
            print(f"❌ Échec: {e}", flush=True)
            continue

        # Normaliser en liste
        if not isinstance(search_results, list):
            possible_list = pyairbnb.get_nested_value(search_results, "results", [])
            search_results = possible_list if isinstance(possible_list, list) else []

        print(f"✓ {len(search_results)} résultats", flush=True)

        for item in search_results:
            listing_id = try_paths(item, ["listing.id", "id"])
            if not listing_id:
                continue
            
            listing_id = str(listing_id)
            
            # Déduplication
            if listing_id in listings_data:
                continue
            
            # Extraire infos basiques (déjà disponibles dans search_all!)
            listing_title = try_paths(item, ["listing.name", "listing.title"], "")
            
            # HOST INFO - Déjà présent dans search_all
            host_id = try_paths(item, ["listing.user.id", "user.id"], "")
            host_name = try_paths(item, ["listing.user.first_name", "user.first_name"], "")
            is_superhost = try_paths(item, ["listing.user.is_superhost", "user.is_superhost"], False)
            
            listings_data[listing_id] = {
                "listing_id": listing_id,
                "listing_title": listing_title,
                "host_id": str(host_id) if host_id else "",
                "host_name": host_name,
                "is_superhost": is_superhost,
                # Ces champs seront remplis en Phase 2 si nécessaire
                "license_code": "",
                "host_rating": "",
                "host_reviews_count": "",
                "host_joined_year": "",
                "host_years_active": "",
            }

        # Pause entre zones pour éviter rate limit
        if idx < len(zones):
            time.sleep(DELAY_BETWEEN_ZONES)
    
    print(f"\n✅ Phase 1 terminée: {len(listings_data)} listings uniques trouvés\n", flush=True)
    return listings_data


@retry_on_failure(max_retries=3, delay=2)
def get_listing_details_with_retry(listing_id):
    """Appel get_details avec retry automatique"""
    rid_int = safe_int(listing_id, listing_id)
    
    return pyairbnb.get_details(
        room_id=rid_int,
        currency=CURRENCY,
        proxy_url=PROXY_URL,
        adults=2,
        language=LANGUAGE,
    )


def enrich_with_detailed_info(listings_data, max_details_calls=500):
    """
    Phase 2 (OPTIONNELLE): Enrichit avec get_details pour license + host details
    
    Pour Dubai avec 20,000+ listings, appeler get_details sur tous prendrait 3+ heures.
    On limite à max_details_calls (par défaut 500) pour un échantillon représentatif.
    
    Priorisation:
    1. Hosts avec plusieurs listings (probablement pros)
    2. Superhosts
    3. Random sample du reste
    """
    print(f"\n🔍 Phase 2: Enrichissement avec détails (max {max_details_calls} appels)\n", flush=True)
    
    # Charger checkpoint si existe
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            checkpoint = json.load(f)
        processed_ids = set(checkpoint.get("processed_ids", []))
        print(f"📂 Checkpoint trouvé: {len(processed_ids)} déjà traités", flush=True)
    else:
        processed_ids = set()
    
    # Compter listings par host
    host_listing_count = {}
    for listing in listings_data.values():
        host_id = listing["host_id"]
        if host_id:
            host_listing_count[host_id] = host_listing_count.get(host_id, 0) + 1
    
    # Prioriser les listings à enrichir
    priority_queue = []
    
    # Priorité 1: Hosts avec 2+ listings (probablement pros)
    for listing in listings_data.values():
        host_id = listing["host_id"]
        if host_id and host_listing_count.get(host_id, 0) >= 2:
            priority_queue.append((3, listing["listing_id"]))  # Score 3
    
    # Priorité 2: Superhosts
    for listing in listings_data.values():
        if listing["is_superhost"]:
            priority_queue.append((2, listing["listing_id"]))  # Score 2
    
    # Priorité 3: Reste (random)
    import random
    remaining = [lid for lid in listings_data.keys() 
                 if lid not in [x[1] for x in priority_queue]]
    random.shuffle(remaining)
    for lid in remaining[:max_details_calls]:
        priority_queue.append((1, lid))
    
    # Trier par priorité (score décroissant)
    priority_queue.sort(reverse=True, key=lambda x: x[0])
    
    # Limiter au max
    priority_queue = priority_queue[:max_details_calls]
    to_process = [lid for _, lid in priority_queue if lid not in processed_ids]
    
    print(f"📊 À enrichir: {len(to_process)} listings (dont {len([x for x in priority_queue if x[0] == 3])} multi-listing hosts)", flush=True)
    
    for idx, listing_id in enumerate(to_process, start=1):
        print(f"[{idx}/{len(to_process)}] 🔄 Détails pour {listing_id}...", end=" ", flush=True)
        
        try:
            data = get_listing_details_with_retry(listing_id)
            
            # License
            license_code = try_paths(data, [
                "pdp_listing_detail.license_number",
                "pdp_listing_detail.license",
                "listing.license_number",
            ], "")
            
            # Host details
            host_rating = try_paths(data, [
                "pdp_listing_detail.primary_host.overall_rating",
                "primary_host.overall_rating",
            ], "")
            
            host_reviews_count = try_paths(data, [
                "pdp_listing_detail.primary_host.review_count",
                "primary_host.review_count",
            ], "")
            
            member_since = try_paths(data, [
                "pdp_listing_detail.primary_host.member_since",
                "primary_host.member_since",
            ], "")
            
            joined_year = ""
            years_active = ""
            if isinstance(member_since, str) and len(member_since) >= 4:
                try:
                    joined_year_int = int(member_since[:4])
                    joined_year = joined_year_int
                    years_active = datetime.now().year - joined_year_int
                except:
                    pass
            
            # Mise à jour
            listings_data[listing_id].update({
                "license_code": license_code,
                "host_rating": host_rating,
                "host_reviews_count": host_reviews_count,
                "host_joined_year": joined_year,
                "host_years_active": years_active,
            })
            
            processed_ids.add(listing_id)
            print("✓", flush=True)
            
            # Checkpoint tous les BATCH_SIZE
            if idx % BATCH_SIZE == 0:
                with open(CHECKPOINT_FILE, 'w') as f:
                    json.dump({"processed_ids": list(processed_ids)}, f)
                print(f"  💾 Checkpoint: {len(processed_ids)} traités", flush=True)
            
        except Exception as e:
            print(f"❌ {e}", flush=True)
        
        time.sleep(DELAY_BETWEEN_DETAIL_CALLS)
    
    # Cleanup checkpoint
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
    
    print(f"\n✅ Phase 2 terminée: {len(processed_ids)} listings enrichis\n", flush=True)
    
    # Ajouter host_total_listings_in_dubai pour TOUS
    for listing in listings_data.values():
        host_id = listing["host_id"]
        listing["host_total_listings_in_dubai"] = host_listing_count.get(host_id, 0) if host_id else 0


def scrape_dubai_to_csv(output_csv_path: str, enable_detailed_enrichment=False, max_details=500):
    """
    Scraping complet avec 2 phases:
    - Phase 1: Récupère tous les listings avec infos basiques (RAPIDE)
    - Phase 2: Enrichit un échantillon avec get_details (OPTIONNEL, LENT)
    
    Args:
        output_csv_path: Chemin du CSV de sortie
        enable_detailed_enrichment: Si True, exécute Phase 2 (lent!)
        max_details: Nombre max d'appels get_details en Phase 2
    """
    start_time = time.time()
    
    # Phase 1: Collecte rapide
    listings_data = collect_listings_with_basic_info()
    
    # Phase 2: Enrichissement optionnel
    if enable_detailed_enrichment:
        enrich_with_detailed_info(listings_data, max_details_calls=max_details)
    else:
        print("⏩ Phase 2 désactivée (enable_detailed_enrichment=False)")
        print("   Pas d'appels get_details = BEAUCOUP plus rapide!")
        print("   Tu auras: listing_id, title, host_id, host_name, is_superhost\n")
        
        # Compter quand même les listings par host
        host_listing_count = {}
        for listing in listings_data.values():
            host_id = listing["host_id"]
            if host_id:
                host_listing_count[host_id] = host_listing_count.get(host_id, 0) + 1
        
        for listing in listings_data.values():
            host_id = listing["host_id"]
            listing["host_total_listings_in_dubai"] = host_listing_count.get(host_id, 0) if host_id else 0
    
    # Écriture CSV
    print(f"💾 Écriture du CSV: {output_csv_path}...", end=" ", flush=True)
    
    fieldnames = [
        "listing_id",
        "listing_url",
        "listing_title",
        "license_code",
        "dtcm_link",
        "host_id",
        "host_name",
        "host_profile_url",
        "is_superhost",
        "host_rating",
        "host_reviews_count",
        "host_joined_year",
        "host_years_active",
        "host_total_listings_in_dubai",
    ]
    
    records = []
    for listing in listings_data.values():
        listing_id = listing["listing_id"]
        
        dtcm_link = ""
        if listing["license_code"]:
            dtcm_link = f"https://hhpermits.det.gov.ae/holidayhomes/Customization/DTCM/CustomPages/HHQRCode.aspx?r={listing['license_code']}"
        
        host_profile_url = ""
        if listing["host_id"]:
            host_profile_url = f"https://www.airbnb.com/users/show/{listing['host_id']}"
        
        records.append({
            "listing_id": listing_id,
            "listing_url": f"https://www.airbnb.com/rooms/{listing_id}",
            "listing_title": listing["listing_title"],
            "license_code": listing["license_code"],
            "dtcm_link": dtcm_link,
            "host_id": listing["host_id"],
            "host_name": listing["host_name"],
            "host_profile_url": host_profile_url,
            "is_superhost": listing["is_superhost"],
            "host_rating": listing["host_rating"],
            "host_reviews_count": listing["host_reviews_count"],
            "host_joined_year": listing["host_joined_year"],
            "host_years_active": listing["host_years_active"],
            "host_total_listings_in_dubai": listing["host_total_listings_in_dubai"],
        })
    
    with open(output_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    
    print("✓", flush=True)
    
    elapsed = time.time() - start_time
    print(f"\n🎉 Scraping terminé en {elapsed/60:.1f} minutes")
    print(f"📊 {len(records)} listings sauvegardés dans {output_csv_path}")
    
    # Stats
    hosts_with_multiple = sum(1 for r in records if r["host_total_listings_in_dubai"] >= 2)
    superhosts = sum(1 for r in records if r["is_superhost"])
    with_license = sum(1 for r in records if r["license_code"])
    
    print(f"\n📈 Statistiques:")
    print(f"   • Hosts avec 2+ listings: {hosts_with_multiple} ({hosts_with_multiple/len(records)*100:.1f}%)")
    print(f"   • Superhosts: {superhosts} ({superhosts/len(records)*100:.1f}%)")
    if enable_detailed_enrichment:
        print(f"   • Avec license DTCM: {with_license} ({with_license/max_details*100:.1f}% de l'échantillon enrichi)")


if __name__ == "__main__":
    # MODE RAPIDE (recommandé pour GitHub Actions): Seulement Phase 1
    # Temps estimé: 5-15 minutes pour tout Dubai
    scrape_dubai_to_csv(
        "dubai_listings.csv",
        enable_detailed_enrichment=False  # Désactivé = RAPIDE
    )
    
    # MODE COMPLET (pour exécution locale avec plus de temps):
    # Décommenter ci-dessous pour enrichir 500 listings avec licenses
    # Temps estimé: 10-20 minutes Phase 1 + 5-10 minutes Phase 2
    # scrape_dubai_to_csv(
    #     "dubai_listings_detailed.csv",
    #     enable_detailed_enrichment=True,
    #     max_details=500  # Limite à 500 pour rester sous 6h GitHub Actions
    # )
