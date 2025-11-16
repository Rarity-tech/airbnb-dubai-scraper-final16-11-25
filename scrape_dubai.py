import csv
from datetime import date, timedelta

# On importe UNIQUEMENT ce que le module pyairbnb expose dans __all__
from pyairbnb import search_all, get_details


# =========================
# CONFIGURATION GÉNÉRALE
# =========================

# Décalage dans le futur pour les dates (pour que la recherche soit valide)
CHECK_IN_OFFSET_DAYS = 30
STAY_NIGHTS = 3

# Taille de l'échantillon pour les tests (tu pourras augmenter plus tard)
SAMPLE_SIZE = 40

# Devise et langue utilisées par la lib pyairbnb
CURRENCY = "USD"
LANGUAGE = "en"

# Bounding box approximative pour Dubaï (NE = nord-est, SW = sud-ouest)
NE_LAT = 25.35
NE_LON = 55.50
SW_LAT = 25.00
SW_LON = 55.15


def main():
    # =========================
    # 1. Préparer les dates
    # =========================
    today = date.today()
    check_in = (today + timedelta(days=CHECK_IN_OFFSET_DAYS)).strftime("%Y-%m-%d")
    check_out = (today + timedelta(days=CHECK_IN_OFFSET_DAYS + STAY_NIGHTS)).strftime("%Y-%m-%d")

    print(f"Recherche pour Dubaï du {check_in} au {check_out}")

    # =========================
    # 2. Appel à search_all
    # =========================
    # Paramètres basés sur l’exemple officiel du README pyairbnb
    results = search_all(
        check_in=check_in,
        check_out=check_out,
        ne_lat=NE_LAT,
        ne_long=NE_LON,
        sw_lat=SW_LAT,
        sw_long=SW_LON,
        zoom_value=12,
        price_min=10,
        price_max=1000,
        place_type="Any",   # on ne filtre pas (conforme à l’exemple)
        amenities=[],       # pas de filtre d’aménités pour l’instant
        free_cancellation=False,
        currency=CURRENCY,
        language=LANGUAGE,
        proxy_url="",       # pas de proxy
    )

    # On s’attend à ce que results soit une liste de logements
    total_found = len(results) if isinstance(results, list) else 0
    print(f"Nombre total de logements trouvés par search_all : {total_found}")

    if not isinstance(results, list) or total_found == 0:
        print("Aucun résultat ou format inattendu retourné par search_all. Arrêt.")
        return

    # On réduit à un petit échantillon pour le test
    listings = results[:SAMPLE_SIZE]
    print(f"On va traiter un échantillon de {len(listings)} logements.")

    # =========================
    # 3. Boucle sur l’échantillon + get_details
    # =========================
    rows = []

    for idx, item in enumerate(listings, start=1):
        # D’après la logique habituelle de l’API Airbnb,
        # chaque item contient un sous-objet "listing" avec un "id".
        listing_data = item.get("listing", {}) if isinstance(item, dict) else {}
        room_id = listing_data.get("id") or item.get("id") if isinstance(item, dict) else None

        if room_id is None:
            print(f"[AVERTISSEMENT] Impossible de trouver l'id du logement pour l'élément {idx}, on saute.")
            continue

        room_id_str = str(room_id)
        print(f"[{idx}] Traitement du logement room_id={room_id_str} ...")

        # Appel à get_details selon l’exemple officiel pyairbnb
        details = get_details(
            room_id=room_id_str,
            currency=CURRENCY,
            proxy_url="",
            adults=2,
            language=LANGUAGE,
        )

        if not isinstance(details, dict):
            print(f"[{idx}] Format inattendu pour get_details(room_id={room_id_str}), on saute.")
            continue

        # La structure exacte dépend de la réponse Airbnb ; la lib vise généralement
        # un nœud principal "pdp_listing_detail". On tombe sinon sur le dict brut.
        pdp = details.get("pdp_listing_detail") or details

        if not isinstance(pdp, dict):
            print(f"[{idx}] 'pdp_listing_detail' manquant ou non dict, on saute.")
            continue

        # =========================
        # 3.1. Informations principales du listing
        # =========================
        license_code = pdp.get("license")
        name = pdp.get("name")
        city = pdp.get("city")
        country = pdp.get("country")
        neighbourhood = pdp.get("public_address")

        # URL de l’annonce
        listing_url = pdp.get("listing_url")
        if not listing_url:
            listing_url = f"https://www.airbnb.com/rooms/{room_id_str}"

        # =========================
        # 3.2. Informations sur l’hôte (host)
        # =========================
        # La lib pyairbnb renvoie typiquement un bloc "primary_host" ou "host"
        host = pdp.get("primary_host") or pdp.get("host") or {}
        if not isinstance(host, dict):
            host = {}

        host_id = host.get("id")
        host_name = host.get("first_name") or host.get("name")
        is_superhost = host.get("is_superhost")

        host_profile_url = f"https://www.airbnb.com/users/show/{host_id}" if host_id else None

        # =========================
        # 3.3. Notes et avis
        # =========================
        avg_rating = pdp.get("avg_rating") or pdp.get("overall_rating")
        reviews_count = pdp.get("reviews_count") or pdp.get("review_count")

        # =========================
        # 3.4. Capacité et caractéristiques
        # =========================
        max_guests = pdp.get("person_capacity") or pdp.get("guest_count")
        bedrooms = pdp.get("bedrooms")
        bathrooms = pdp.get("bathrooms") or pdp.get("bathroom_label")

        # =========================
        # 3.5. Prix de base
        # =========================
        price_detail = pdp.get("price") or pdp.get("pricing_quote") or {}
        if not isinstance(price_detail, dict):
            price_detail = {}

        nightly_price = price_detail.get("rate") or price_detail.get("amount")
        currency = price_detail.get("currency") or CURRENCY

        # =========================
        # 3.6. Ligne pour le CSV
        # =========================
        row = {
            "room_id": room_id_str,
            "name": name,
            "city": city,
            "country": country,
            "neighbourhood": neighbourhood,
            "license": license_code,
            "listing_url": listing_url,
            "host_id": host_id,
            "host_name": host_name,
            "host_profile_url": host_profile_url,
            "is_superhost": is_superhost,
            "avg_rating": avg_rating,
            "reviews_count": reviews_count,
            "max_guests": max_guests,
            "bedrooms": bedrooms,
            "bathrooms": bathrooms,
            "nightly_price": nightly_price,
            "price_currency": currency,
        }

        rows.append(row)

    # =========================
    # 4. Écriture du CSV
    # =========================
    output_file = "dubai_listings.csv"
    fieldnames = [
        "room_id",
        "name",
        "city",
        "country",
        "neighbourhood",
        "license",
        "listing_url",
        "host_id",
        "host_name",
        "host_profile_url",
        "is_superhost",
        "avg_rating",
        "reviews_count",
        "max_guests",
        "bedrooms",
        "bathrooms",
        "nightly_price",
        "price_currency",
    ]

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Fichier CSV généré : {output_file}")
    print(f"Nombre de lignes écrites : {len(rows)}")


if __name__ == "__main__":
    main()
