import pyairbnb
import json

print("="*80)
print("🧪 TEST AVEC UN ROOM_ID RÉEL QUI A MARCHÉ")
print("="*80)
print()

# Room ID qui a fonctionné dans tes logs
test_room_ids = [
    "1163628413062135453",  # New 2BR + Maid's Room
    "1269373644663014153",  # Pool view apartment
    "1423637965499671234",  # The Grand Family Haven
]

for room_id in test_room_ids:
    print("="*80)
    print(f"🏠 TEST LISTING: {room_id}")
    print("="*80)
    print()
    
    try:
        print("🔄 Appel de get_details()...")
        details = pyairbnb.get_details(
            room_id=room_id,
            currency="AED",
            proxy_url="",
            language="en",
        )
        
        if not details:
            print("❌ get_details() a retourné None")
            continue
        
        print(f"✅ get_details() OK - Type: {type(details)}")
        print()
        
        if isinstance(details, dict):
            print("🔑 CLÉS PRINCIPALES (20 premières):")
            for i, key in enumerate(list(details.keys())[:20], 1):
                print(f"   {i}. {key}")
            print()
            
            # CHERCHER LE HOST_ID PARTOUT
            print("🔍 RECHERCHE INTENSIVE DU HOST_ID:")
            print()
            
            host_id_paths = [
                ["pdp_listing_detail", "primary_host", "id"],
                ["pdp_listing_detail", "host", "id"],
                ["pdp_listing_detail", "hostDetails", "id"],
                ["primary_host", "id"],
                ["host", "id"],
                ["hostDetails", "id"],
                ["listing", "primary_host", "id"],
                ["listing", "host", "id"],
                ["listing", "user", "id"],
                ["user", "id"],
                ["host_id"],
                ["hostId"],
                ["userId"],
                ["user_id"],
            ]
            
            found_host_id = None
            found_path = None
            
            for path in host_id_paths:
                try:
                    value = details
                    for key in path:
                        if isinstance(value, dict) and key in value:
                            value = value[key]
                        else:
                            value = None
                            break
                    
                    if value is not None:
                        print(f"   ✅ TROUVÉ: {' → '.join(path)} = {value}")
                        if not found_host_id:
                            found_host_id = str(value)
                            found_path = path
                    else:
                        print(f"   ❌ Absent: {' → '.join(path)}")
                        
                except Exception as e:
                    print(f"   ⚠️ Erreur: {' → '.join(path)}: {e}")
            
            print()
            
            if found_host_id:
                print(f"🎯 HOST_ID TROUVÉ: {found_host_id}")
                print(f"📍 Chemin: {' → '.join(found_path)}")
                print()
                
                # TESTER get_host_details
                print(f"🧪 TEST get_host_details({found_host_id})...")
                print()
                
                try:
                    host_details = pyairbnb.get_host_details(
                        host_id=found_host_id,
                        proxy_url="",
                    )
                    
                    if host_details and isinstance(host_details, dict):
                        print("✅ get_host_details() FONCTIONNE !")
                        print()
                        print("📊 DONNÉES HOST:")
                        
                        # Nom
                        host_name = (
                            host_details.get("first_name") or
                            host_details.get("name") or
                            "N/A"
                        )
                        print(f"   Nom: {host_name}")
                        
                        # Rating
                        host_rating = (
                            host_details.get("overall_rating") or
                            host_details.get("rating") or
                            host_details.get("guest_rating") or
                            "N/A"
                        )
                        print(f"   Rating: {host_rating}")
                        
                        # Reviews
                        reviews = (
                            host_details.get("review_count") or
                            host_details.get("reviews_count") or
                            host_details.get("number_of_reviews") or
                            "N/A"
                        )
                        print(f"   Reviews: {reviews}")
                        
                        # Member since
                        member_since = (
                            host_details.get("member_since") or
                            host_details.get("created_at") or
                            "N/A"
                        )
                        print(f"   Member since: {member_since}")
                        
                        print()
                        print("🔑 TOUTES LES CLÉS dans host_details:")
                        for key in host_details.keys():
                            print(f"   - {key}")
                        
                        print()
                        
                        # TESTER get_listings_from_user
                        print(f"🧪 TEST get_listings_from_user({found_host_id})...")
                        print()
                        
                        try:
                            host_listings = pyairbnb.get_listings_from_user(
                                host_id=found_host_id,
                                proxy_url="",
                            )
                            
                            if host_listings:
                                print(f"✅ get_listings_from_user() FONCTIONNE !")
                                print(f"   → Ce host a {len(host_listings)} listings")
                            else:
                                print("⚠️ get_listings_from_user() a retourné vide")
                                
                        except Exception as e:
                            print(f"❌ get_listings_from_user() ERREUR: {e}")
                        
                    else:
                        print("❌ get_host_details() a retourné None ou pas un dict")
                        print(f"   Type retourné: {type(host_details)}")
                        
                except Exception as e:
                    print(f"❌ get_host_details() ERREUR: {e}")
                    import traceback
                    traceback.print_exc()
                    
            else:
                print("❌❌❌ HOST_ID INTROUVABLE DANS LA STRUCTURE !")
                print()
                print("📄 AFFICHAGE DES PREMIÈRES CLÉS ET LEUR CONTENU:")
                print()
                
                for key in list(details.keys())[:5]:
                    print(f"🔑 {key}:")
                    content = details[key]
                    if isinstance(content, dict):
                        print(f"   Type: dict avec {len(content)} clés")
                        print(f"   Sous-clés: {list(content.keys())[:10]}")
                    elif isinstance(content, list):
                        print(f"   Type: list avec {len(content)} éléments")
                    else:
                        print(f"   Type: {type(content)}")
                        print(f"   Valeur: {str(content)[:100]}")
                    print()
            
        else:
            print(f"⚠️ details n'est pas un dict: {type(details)}")
            
    except Exception as e:
        print(f"❌ ERREUR GÉNÉRALE: {e}")
        import traceback
        traceback.print_exc()
    
    print()
    print("="*80)
    print()

print("🎉 TESTS TERMINÉS")
print()
print("📋 SI HOST_ID TROUVÉ:")
print("   → Partage-moi le CHEMIN exact")
print("   → Je vais corriger le code")
print()
print("📋 SI HOST_ID NON TROUVÉ:")
print("   → Partage-moi la structure des clés")
print("   → On va le trouver ensemble")
