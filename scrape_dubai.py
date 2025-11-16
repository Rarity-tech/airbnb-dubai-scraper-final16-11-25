import csv
import os
import re
import subprocess
import time
import json
from datetime import datetime, timedelta
from functools import wraps
import pyairbnb


# ==========================
# ⚙️ CONTRÔLE DU RUN
# ==========================
LISTINGS_PER_RUN = 5  # SEULEMENT 5 POUR DEBUG !


# ==========================
# CONFIG GLOBALE
# ==========================
future_date = datetime.now() + timedelta(days=14)
CHECK_IN = future_date.strftime("%Y-%m-%d")
CHECK_OUT = (future_date + timedelta(days=5)).strftime("%Y-%m-%d")

CURRENCY = "AED"
LANGUAGE = "en"
PROXY_URL = ""
ZOOM_VALUE = 4

DELAY_BETWEEN_DETAILS = 1.0
DELAY_BETWEEN_HOSTS = 1.5
DELAY_BETWEEN_ZONES = 2.0

CSV_FILE = "dubai_listings.csv"
PROCESSED_IDS_FILE = "processed_ids.txt"


def build_dubai_city_subzones(rows=4, cols=5):
    """Zones précises Dubai"""
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


def search_zone(zone):
    """Recherche dans une zone"""
    try:
        return pyairbnb.search_all(
            check_in=CHECK_IN,
            check_out=CHECK_OUT,
            ne_lat=zone["ne_lat"],
            ne_long=zone["ne_long"],
            sw_lat=zone["sw_lat"],
            sw_long=zone["sw_long"],
            zoom_value=ZOOM_VALUE,
            currency=CURRENCY,
            language=LANGUAGE,
            proxy_url=PROXY_URL,
        )
    except:
        return []


def collect_room_ids():
    """Phase 1: Récupère quelques room_ids"""
    zones = build_dubai_city_subzones(rows=4, cols=5)
    all_room_ids = []
    
    print(f"🔍 Phase 1: Recherche de {LISTINGS_PER_RUN} room_ids pour DEBUG\n")

    for zone in zones:
        if len(all_room_ids) >= LISTINGS_PER_RUN:
            break
            
        try:
            results = search_zone(zone)
            
            for result in results:
                if len(all_room_ids) >= LISTINGS_PER_RUN:
                    break
                    
                room_id = None
                if isinstance(result, dict):
                    room_id = (
                        result.get("room_id") or 
                        result.get("id") or 
                        result.get("listing", {}).get("id")
                    )
                
                if room_id:
                    all_room_ids.append(str(room_id))
                    
        except:
            continue
        
        time.sleep(0.5)
    
    unique_ids = list(set(all_room_ids))
    print(f"✅ {len(unique_ids)} room_ids trouvés\n")
    return unique_ids


def debug_get_details(room_id):
    """
    VERSION DEBUG QUI AFFICHE TOUTE LA STRUCTURE
    """
    print(f"\n{'='*80}")
    print(f"🔍 DEBUG - LISTING {room_id}")
    print(f"{'='*80}\n")
    
    try:
        details = pyairbnb.get_details(
            room_id=room_id,
            currency=CURRENCY,
            proxy_url=PROXY_URL,
            language=LANGUAGE,
        )
        
        if not details:
            print("❌ get_details() a retourné None ou vide\n")
            return None
        
        print(f"✅ get_details() a retourné des données\n")
        print(f"📊 TYPE: {type(details)}\n")
        
        # Afficher les CLÉS PRINCIPALES
        if isinstance(details, dict):
            print(f"🔑 CLÉS PRINCIPALES (niveau 1):")
            for key in list(details.keys())[:20]:  # Premières 20 clés
                print(f"   - {key}")
            print()
            
            # CHERCHER LE HOST_ID PARTOUT
            print(f"🔍 RECHERCHE DU HOST_ID DANS LA STRUCTURE:\n")
            
            paths_to_check = [
                ["pdp_listing_detail", "primary_host", "id"],
                ["pdp_listing_detail", "host", "id"],
                ["primary_host", "id"],
                ["host", "id"],
                ["listing", "primary_host", "id"],
                ["listing", "host", "id"],
                ["listing", "user", "id"],
                ["user", "id"],
                ["host_id"],
                ["hostId"],
            ]
            
            found_host_id = None
            
            for path in paths_to_check:
                try:
                    value = details
                    path_str = " → ".join(path)
                    
                    for key in path:
                        if isinstance(value, dict) and key in value:
                            value = value[key]
                        else:
                            value = None
                            break
                    
                    if value is not None:
                        print(f"   ✅ TROUVÉ à [{path_str}] = {value}")
                        if not found_host_id:
                            found_host_id = str(value)
                    else:
                        print(f"   ❌ Pas trouvé à [{path_str}]")
                        
                except Exception as e:
                    print(f"   ⚠️ Erreur [{' → '.join(path)}]: {e}")
            
            print()
            
            if found_host_id:
                print(f"🎯 HOST_ID IDENTIFIÉ: {found_host_id}\n")
                
                # TESTER get_host_details()
                print(f"🧪 TEST get_host_details({found_host_id})...\n")
                
                try:
                    host_details = pyairbnb.get_host_details(
                        host_id=found_host_id,
                        proxy_url=PROXY_URL,
                    )
                    
                    if host_details and isinstance(host_details, dict):
                        print(f"✅ get_host_details() FONCTIONNE !\n")
                        print(f"📊 DONNÉES HOST RÉCUPÉRÉES:")
                        print(f"   - Nom: {host_details.get('first_name') or host_details.get('name')}")
                        print(f"   - Rating: {host_details.get('overall_rating') or host_details.get('rating')}")
                        print(f"   - Reviews: {host_details.get('review_count') or host_details.get('reviews_count')}")
                        print(f"   - Member since: {host_details.get('member_since')}")
                        print()
                        
                        # TESTER get_listings_from_user()
                        print(f"🧪 TEST get_listings_from_user({found_host_id})...\n")
                        
                        try:
                            host_listings = pyairbnb.get_listings_from_user(
                                host_id=found_host_id,
                                proxy_url=PROXY_URL,
                            )
                            
                            if host_listings:
                                print(f"✅ get_listings_from_user() FONCTIONNE !")
                                print(f"   - Ce host a {len(host_listings)} listings\n")
                            else:
                                print(f"⚠️ get_listings_from_user() a retourné vide\n")
                                
                        except Exception as e:
                            print(f"❌ get_listings_from_user() ERREUR: {e}\n")
                        
                    else:
                        print(f"❌ get_host_details() a retourné None ou vide\n")
                        
                except Exception as e:
                    print(f"❌ get_host_details() ERREUR: {e}\n")
                    
            else:
                print(f"❌ HOST_ID NON TROUVÉ DANS LA STRUCTURE !\n")
                print(f"📄 STRUCTURE COMPLÈTE (premiers 500 caractères):")
                print(json.dumps(details, indent=2, ensure_ascii=False)[:500])
                print("...\n")
            
        else:
            print(f"⚠️ details n'est pas un dict, c'est: {type(details)}\n")
            
        return details
        
    except Exception as e:
        print(f"❌ ERREUR GÉNÉRALE: {e}\n")
        import traceback
        traceback.print_exc()
        return None


def main():
    """Version DEBUG"""
    print("=" * 80)
    print("🐛 MODE DEBUG - ANALYSE DE get_details()")
    print("=" * 80)
    print(f"📊 Va tester {LISTINGS_PER_RUN} listings")
    print("=" * 80 + "\n")
    
    # Récupérer quelques room_ids
    room_ids = collect_room_ids()
    
    if len(room_ids) == 0:
        print("❌ Aucun room_id trouvé !")
        return
    
    # Tester chaque listing
    print(f"{'='*80}")
    print(f"🧪 PHASE DEBUG - TEST DE get_details() ET get_host_details()")
    print(f"{'='*80}\n")
    
    for idx, room_id in enumerate(room_ids, start=1):
        print(f"\n{'#'*80}")
        print(f"# LISTING {idx}/{len(room_ids)}")
        print(f"{'#'*80}")
        
        debug_get_details(room_id)
        
        print(f"\n{'='*80}\n")
        time.sleep(2)
    
    print("=" * 80)
    print("🎉 DEBUG TERMINÉ")
    print("=" * 80)
    print()
    print("📋 INSTRUCTIONS:")
    print("1. Regarde les logs ci-dessus")
    print("2. Cherche où le HOST_ID est trouvé")
    print("3. Partage-moi les résultats")
    print()


if __name__ == "__main__":
    main()
