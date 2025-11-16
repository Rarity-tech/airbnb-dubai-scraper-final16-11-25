import os
import csv
import json
import subprocess
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import pyairbnb

# =============================================================================
# Configuration
# =============================================================================

LISTINGS_PER_RUN = int(os.getenv("LISTINGS_PER_RUN", "200"))
OUTPUT_CSV = "dubai_listings.csv"

# Dates: environ 2 semaines dans le futur, séjour de 5 nuits
TODAY = datetime.utcnow().date()
CHECKIN_OFFSET_DAYS = 14
STAY_NIGHTS = 5
CHECKIN_DATE = TODAY + timedelta(days=CHECKIN_OFFSET_DAYS)
CHECKOUT_DATE = CHECKIN_DATE + timedelta(days=STAY_NIGHTS)

CURRENCY = "AED"
LANGUAGE = "en"


# =============================================================================
# Grille de zones sur Dubaï (3 x 4) – mêmes coordonnées que les logs
# =============================================================================


def build_zones() -> List[Dict[str, Any]]:
    zones: List[Dict[str, Any]] = []

    lat_start = 24.85
    lat_step = 0.1667
    lon_start = 54.95
    lon_step = 0.125

    for i in range(3):  # lignes
        sw_lat = lat_start + i * lat_step
        ne_lat = sw_lat + lat_step
        for j in range(4):  # colonnes
            sw_lon = lon_start + j * lon_step
            ne_lon = sw_lon + lon_step
            zones.append(
                {
                    "name": f"zone_{i+1}_{j+1}",
                    "sw_lat": round(sw_lat, 4),
                    "sw_lon": round(sw_lon, 4),
                    "ne_lat": round(ne_lat, 4),
                    "ne_lon": round(ne_lon, 4),
                }
            )
    return zones


ZONES = build_zones()


# =============================================================================
# Utilitaires
# =============================================================================


def log(msg: str) -> None:
    print(msg, flush=True)


def safe_get(d: Dict[str, Any], key: str, default: Any = None) -> Any:
    return d.get(key, default) if isinstance(d, dict) else default


def recursive_find(
    obj: Any,
    key_predicate,
    value_predicate=lambda v: True,
) -> Optional[Any]:
    """Recherche récursive dans un dict/list.

    key_predicate: fonction(k) -> bool
    value_predicate: fonction(v) -> bool
    Retourne la première valeur qui matche, sinon None.
    """

    if isinstance(obj, dict):
        for k, v in obj.items():
            try:
                if key_predicate(k) and value_predicate(v):
                    return v
            except Exception:
                pass
            found = recursive_find(v, key_predicate, value_predicate)
            if found is not None:
                return found

    elif isinstance(obj, list):
        for item in obj:
            found = recursive_find(item, key_predicate, value_predicate)
            if found is not None:
                return found

    return None


def extract_host_and_rating(details: Dict[str, Any]) -> Tuple[Optional[str], Optional[float]]:
    """Extraction robuste de host et rating à partir du JSON Airbnb.

    On ne dépend pas d'un chemin précis: on scanne récursivement pour
    trouver des clés pertinentes (flexible aux changements de structure).
    """

    # 1) Host name
    def host_key_pred(k: str) -> bool:
        k_lower = k.lower()
        # cherche hostName, hostname, primaryHost.name, etc.
        return ("host" in k_lower and "name" in k_lower) or k_lower in {"hostname", "host_name"}

    host_val = recursive_find(
        details,
        key_predicate=host_key_pred,
        value_predicate=lambda v: isinstance(v, str) and v.strip() != "",
    )

    host: Optional[str] = None
    if isinstance(host_val, str):
        host = host_val.strip()

    # 2) Rating
    def rating_key_pred(k: str) -> bool:
        k_lower = k.lower()
        return "rating" in k_lower or "reviewscore" in k_lower or "overall" in k_lower

    def rating_val_pred(v: Any) -> bool:
        if not isinstance(v, (int, float)):
            return False
        # Airbnb ratings typiquement entre 0 et 5 (ou 0–100 dans certains cas)
        return 0 <= float(v) <= 100

    rating_val = recursive_find(details, rating_key_pred, rating_val_pred)

    rating: Optional[float] = None
    if isinstance(rating_val, (int, float)):
        rating = float(rating_val)
        # normaliser 0–100 -> 0–5 si nécessaire
        if rating > 5:
            rating = round(rating / 20.0, 2)

    return host, rating


def extract_license(details: Dict[str, Any]) -> Optional[str]:
    """Extraction robuste de la licence.

    On essaie d'abord via pyairbnb.get_nested_value si disponible, sinon
    on retombe sur une recherche récursive par clé contenant "license".
    """

    license_num: Optional[str] = None

    # Essai via utilitaire pyairbnb (si présent dans cette version)
    get_nested_value = getattr(pyairbnb, "get_nested_value", None)
    if callable(get_nested_value):
        try:
            # Chemin utilisé classiquement par pyairbnb pour les licences.
            license_candidate = get_nested_value(
                details,
                "data.presentation.stayProductDetailPage.sections.sectionIdToSectionMap.LICENSE_DEFAULT.section.additionalSections.0.license",
                None,
            )
            if isinstance(license_candidate, str) and license_candidate.strip():
                license_num = license_candidate.strip()
        except Exception:
            pass

    if license_num:
        return license_num

    # Fallback: scan récursif des clés contenant "license" ou "licence"
    def license_key_pred(k: str) -> bool:
        k_lower = k.lower()
        return "license" in k_lower or "licence" in k_lower

    license_val = recursive_find(
        details,
        key_predicate=license_key_pred,
        value_predicate=lambda v: isinstance(v, str) and v.strip() != "",
    )

    if isinstance(license_val, str) and license_val.strip():
        return license_val.strip()

    return None


# =============================================================================
# Phase 1 – Recherche des room_ids
# =============================================================================


def search_room_ids_for_zone(zone: Dict[str, Any]) -> List[int]:
    """Retourne une liste de room_ids pour une zone donnée."""

    try:
        data = pyairbnb.search_all(
            checkin=str(CHECKIN_DATE),
            checkout=str(CHECKOUT_DATE),
            ne_lat=zone["ne_lat"],
            ne_long=zone["ne_lon"],
            sw_lat=zone["sw_lat"],
            sw_long=zone["sw_lon"],
            zoom_value=11,
            price_min=0,
            price_max=0,
            place_type="",
            amenities=[],
            free_cancellation=False,
            currency=CURRENCY,
            language=LANGUAGE,
            proxy_url="",
        )

        # Utilitaire standard pyairbnb pour récupérer les listings à partir du JSON
        get_nested_value = getattr(pyairbnb, "get_nested_value", None)
        if callable(get_nested_value):
            listings = get_nested_value(
                data,
                "data.dora.exploreV3.sections.0.items",
                [],
            )
        else:
            listings = data or []

        room_ids: List[int] = []
        for item in listings:
            if not isinstance(item, dict):
                continue
            listing = item.get("listing") or item.get("listingCard") or item
            room_id = listing.get("id") or listing.get("listingId") or listing.get("roomId")
            if isinstance(room_id, (int, str)):
                try:
                    room_ids.append(int(room_id))
                except ValueError:
                    continue

        return room_ids

    except Exception as e:
        log(f"   ⚠️ Erreur pendant la recherche de la zone {zone['name']}: {e}")
        return []


def phase1_collect_all_room_ids() -> Set[int]:
    log("🔍 Phase 1: Recherche des room_ids")
    log(f"   Zones : {len(ZONES)}")
    log(f"   Dates : {CHECKIN_DATE} → {CHECKOUT_DATE}")
    log("")

    all_ids: Set[int] = set()

    for idx, zone in enumerate(ZONES, start=1):
        log(
            f"[{idx}/{len(ZONES)}] 📍 {zone['name']} ({zone['sw_lat']:.4f},{zone['sw_lon']:.4f}) → ({zone['ne_lat']:.4f},{zone['ne_lon']:.4f})...",
        )
        room_ids = search_room_ids_for_zone(zone)
        all_ids.update(room_ids)
        log(f"   ✓ {len(room_ids)} résultats")

    log("")
    log(f"✅ Phase 1 terminée: {len(all_ids)} room_ids uniques trouvés")
    log("")
    return all_ids


# =============================================================================
# Phase 2 – Détails des listings
# =============================================================================


def get_existing_ids_from_csv() -> Set[int]:
    if not os.path.exists(OUTPUT_CSV):
        return set()

    ids: Set[int] = set()
    with open(OUTPUT_CSV, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            listing_id = row.get("listing_id") or row.get("id")
            if listing_id is None:
                continue
            try:
                ids.add(int(listing_id))
            except ValueError:
                continue
    return ids


def get_listing_details(room_id: int) -> Dict[str, Any]:
    """Appel de l'API de détails de listing via pyairbnb.

    On garde la signature la plus compatible possible avec pyairbnb 2.1.1.
    """

    return pyairbnb.get_details(
        room_id=room_id,
        checkin=str(CHECKIN_DATE),
        checkout=str(CHECKOUT_DATE),
        currency=CURRENCY,
        language=LANGUAGE,
        proxy_url="",
    )


def compute_host_totals_in_csv() -> None:
    """Calcule host_total_listings_in_dubai dans le CSV existant.

    On relit le CSV, on compte le nombre de listings par host, puis on
    réécrit le fichier avec une colonne host_total_listings_in_dubai.
    """

    if not os.path.exists(OUTPUT_CSV):
        log("ℹ️ Aucun CSV pour calculer les totaux par host.")
        return

    with open(OUTPUT_CSV, "r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    # Comptage par host (on utilise la colonne 'host' si disponible)
    host_counts: Dict[str, int] = {}
    for row in rows:
        host = row.get("host") or row.get("host_name") or "N/A"
        if not host or host == "N/A":
            continue
        host_counts[host] = host_counts.get(host, 0) + 1

    if not rows:
        log("ℹ️ Aucun enregistrement dans le CSV.")
        return

    fieldnames = list(rows[0].keys())
    if "host_total_listings_in_dubai" not in fieldnames:
        fieldnames.append("host_total_listings_in_dubai")

    for row in rows:
        host = row.get("host") or row.get("host_name") or "N/A"
        total = host_counts.get(host, 0) if host != "N/A" else 0
        row["host_total_listings_in_dubai"] = str(total)

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def git_commit_and_push(message: str) -> None:
    """Commit + push avec gestion propre des erreurs (token read-only, etc.)."""

    try:
        subprocess.run(["git", "config", "user.email", "actions@github.com"], check=True)
        subprocess.run(["git", "config", "user.name", "github-actions"], check=True)

        # Ajouter uniquement le CSV
        subprocess.run(["git", "add", OUTPUT_CSV], check=True)

        # Vérifier s'il y a quelque chose à committer
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            check=True,
            capture_output=True,
            text=True,
        )
        if not status.stdout.strip():
            log("ℹ️ Aucun changement à committer.")
            return

        subprocess.run(["git", "commit", "-m", message], check=True)

        try:
            subprocess.run(["git", "push"], check=True)
        except subprocess.CalledProcessError as e:
            log(
                f"⚠️ Git push échoué (probablement permissions read-only): {e}",
            )
    except Exception as e:
        log(f"⚠️ Erreur Git (commit/push): {e}")


def ensure_csv_header(fieldnames: List[str]) -> None:
    if not os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()


def phase2_extract_details(room_ids: Iterable[int]) -> None:
    room_ids_list = list(room_ids)
    total = len(room_ids_list)

    if total == 0:
        log("ℹ️ Aucun listing à traiter en Phase 2.")
        return

    log(f"🔍 Phase 2: Extraction des détails ({total} listings)")
    log("")

    # Champ minimal, mais on garde 'host', 'rating', 'license' pour rester
    # compatible avec les usages existants.
    fieldnames = [
        "scraped_at",
        "listing_id",
        "zone",
        "checkin",
        "checkout",
        "host",
        "rating",
        "license",
        "host_total_listings_in_dubai",  # rempli après coup
    ]
    ensure_csv_header(fieldnames)

    # On lit pour savoir quelles zones associer si on veut (optionnel)
    # Pour simplifier, on ne stocke pas la zone ici – ce n'est pas vital
    # pour la correction host/rating demandée.

    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        for idx, room_id in enumerate(room_ids_list, start=1):
            log(f"[{idx}/{total}] 🏠 Listing {room_id}...")

            attempts = 0
            max_attempts = 3
            details: Optional[Dict[str, Any]] = None

            while attempts < max_attempts:
                try:
                    details = get_listing_details(room_id)
                    break
                except Exception as e:
                    attempts += 1
                    if attempts < max_attempts:
                        log(
                            f"   ⚠️ Tentative {attempts}/{max_attempts} échouée: {e}. Retry dans 2s",
                        )
                        import time

                        time.sleep(2)
                    else:
                        log(
                            f"   ❌ Echec définitif après {max_attempts} tentatives: {e}",
                        )

            if details is None:
                continue

            # Extraction robuste host / rating / license
            host, rating = extract_host_and_rating(details)
            license_num = extract_license(details)

            # Log utilisateur
            host_display = host if host else "N/A"
            rating_display = f"{rating:.2f}" if isinstance(rating, (int, float)) else "N/A"
            license_display = license_num if license_num else "N/A"
            log(
                f"   ✓ host: {host_display} | rating: {rating_display} | license: {license_display}",
            )

            row = {
                "scraped_at": datetime.utcnow().isoformat(timespec="seconds"),
                "listing_id": str(room_id),
                "zone": "N/A",  # si besoin on pourra enrichir plus tard
                "checkin": str(CHECKIN_DATE),
                "checkout": str(CHECKOUT_DATE),
                "host": host_display,
                "rating": rating_display,
                "license": license_display,
                "host_total_listings_in_dubai": "0",  # mis à jour après
            }

            writer.writerow(row)

            # Commit / push intermédiaire toutes les 50 lignes
            if idx % 50 == 0:
                git_commit_and_push(
                    f"Ajout de {idx} listings Dubai (run auto)",
                )

    # Après avoir rempli le CSV, on calcule les totaux par host
    log("")
    log("📊 Calcul des totaux par host...")
    compute_host_totals_in_csv()

    # Commit final
    git_commit_and_push("Mise à jour des totaux par host Dubai")


# =============================================================================
# Main
# =============================================================================


def main() -> None:
    log("""===============================================================================""")
    log(f"🚀 SCRAPING DUBAI - {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
    log("""===============================================================================""")
    log(f"📊 Configuration: {LISTINGS_PER_RUN} listings ce run")
    log("""===============================================================================""")
    log("")

    # Phase 1 – collecter les room_ids
    all_ids = phase1_collect_all_room_ids()

    existing_ids = get_existing_ids_from_csv()
    already_processed = len(all_ids & existing_ids)
    remaining_ids = list(all_ids - existing_ids)

    to_process = remaining_ids[:LISTINGS_PER_RUN]

    log("📊 Statut:")
    log(f"   • Total Dubai (IDs trouvés): {len(all_ids)}")
    log(f"   • Déjà traités: {already_processed}")
    log(f"   • Restants: {len(remaining_ids)}")
    log(f"   • Ce run: {len(to_process)}")
    log("")

    # Phase 2 – détails
    phase2_extract_details(to_process)

    # Stats finales
    total_in_csv = len(get_existing_ids_from_csv())
    log("""===============================================================================""")
    log("🎉 RUN TERMINÉ")
    log("""===============================================================================""")
    log(f"📊 Ce run: +{len(to_process)} listings")
    log(f"📊 Total dans CSV: {total_in_csv} listings")
    log(f"📊 Restants: {max(len(all_ids) - total_in_csv, 0)}")
    log("")
    log("💡 Pour continuer: relance le workflow")
    log("   (ou augmente LISTINGS_PER_RUN si tu veux aller plus vite)")
    log("""===============================================================================""")


if __name__ == "__main__":
    main()
