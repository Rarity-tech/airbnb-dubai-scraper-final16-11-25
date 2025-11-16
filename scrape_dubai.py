# scrape_dubai.py

from pyairbnb.client import PyAirbnb
import csv
import time

def search_dubai_listings():
    client = PyAirbnb()
    all_results = []
    max_pages = 3
    current_page = 0

    while current_page < max_pages:
        response = client.search_all(
            location="Dubai, United Arab Emirates",
            items_per_grid=50,
            min_bathrooms=1,
            min_bedrooms=1,
            min_price=300,
            max_price=3000,
            allow_flexible_dates=False,
            items_per_page=50,
            source="structured_search_input_header"
        )

        if not response or "results" not in response:
            print("❌ Aucun résultat reçu ou structure inattendue.")
            break

        all_results.extend(response["results"])
        print(f"✔️ Page {current_page + 1} : {len(response['results'])} annonces récupérées.")

        if not response.get("has_next_page", False):
            break

        current_page += 1
        time.sleep(2)

    return all_results

def extract_listing_info(listings):
    extracted = []

    for listing in listings:
        data = listing.get("listing", {})
        host = data.get("primary_host", {})
        pricing = listing.get("pricing_quote", {}).get("rate", {})

        extracted.append({
            "id": data.get("id"),
            "name": data.get("name"),
            "property_type": data.get("property_type"),
            "room_type": data.get("room_type_category"),
            "bedrooms": data.get("bedrooms"),
            "bathrooms": data.get("bathrooms"),
            "price_per_night": pricing.get("amount"),
            "monthly_price_factor": listing.get("pricing_quote", {}).get("monthly_price_factor"),
            "weekly_price_factor": listing.get("pricing_quote", {}).get("weekly_price_factor"),
            "is_superhost": host.get("is_superhost"),
            "license_code": data.get("license"),
            "latitude": data.get("lat"),
            "longitude": data.get("lng"),
            "reviews_count": data.get("reviews_count"),
            "star_rating": data.get("star_rating"),
            "city": data.get("city"),
            "neighborhood": data.get("localized_neighborhood"),
        })

    return extracted

def save_to_csv(data, filename="dubai_airbnb_listings.csv"):
    if not data:
        print("❌ Aucune donnée à enregistrer.")
        return

    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

    print(f"✅ Données enregistrées dans : {filename}")

def main():
    print("--- Phase 1 : Recherche des annonces ---")
    results = search_dubai_listings()
    print(f"Total d'annonces trouvées : {len(results)}")

    print("--- Phase 2 : Extraction des données ---")
    listings_data = extract_listing_info(results)

    print("--- Phase 3 : Export CSV ---")
    save_to_csv(listings_data)

if __name__ == "__main__":
    main()
