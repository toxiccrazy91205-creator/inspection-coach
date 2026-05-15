# etl/ahd_osm_etl.py
import httpx
import hashlib
import os
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

OUTPUT_FILE = "./data/demo_seed.json"
OVERPASS_URL = "https://overpass.kumi.systems/api/interpreter"

def get_ahmedabad_restaurants():
    query = """
    [out:json][timeout:90];
    area[name="Ahmedabad"]->.searchArea;
    (node["amenity"~"restaurant|fast_food"](area.searchArea);
     way["amenity"~"restaurant|fast_food"](area.searchArea););
    out center;
    """
    logger.info(f"Querying {OVERPASS_URL}...")
    try:
        with httpx.Client(timeout=100.0) as client:
            response = client.get(OVERPASS_URL, params={"data": query})
            response.raise_for_status()
            data = response.json()
            elements = data.get("elements", [])
            restaurants = []
            for el in elements:
                tags = el.get("tags", {})
                name = tags.get("name")
                if not name: continue
                lat = el.get("lat") or el.get("center", {}).get("lat")
                lon = el.get("lon") or el.get("center", {}).get("lon")
                if lat and lon:
                    restaurants.append({
                        "name": name, "lat": float(lat), "lon": float(lon),
                        "cuisine": tags.get("cuisine", "Indian"),
                        "addr": tags.get("addr:street") or tags.get("addr:full") or "Ahmedabad"
                    })
            return restaurants
    except Exception as e:
        logger.error(f"Failed to fetch real data: {e}")
        return []

def transform_to_app_format(raw_list):
    processed = []
    logger.info(f"Processing {len(raw_list)} real restaurants...")
    for item in raw_list:
        h = hashlib.sha256(f"{item['name']}{item['lat']}".encode()).hexdigest()
        
        # Base probability (randomized but deterministic)
        base_prob = (int(h[11:13], 16) % 35 + 15) / 100.0
        
        # Mock violation probabilities
        vios = [
            {"code": "Sch4.2.1", "probability": round(base_prob * 1.2, 2), "label": "Equipment Maintenance & Cleanliness"},
            {"code": "Sch4.1.2", "probability": round(base_prob * 0.8, 2), "label": "Food Storage Conditions"}
        ]

        processed.append({
            "fssai_id": "107" + str(int(h[:11], 16))[:11],
            "name": item['name'],
            "area": item['addr'],
            "latitude": item['lat'],
            "longitude": item['lon'],
            "prob_fssai_fail": base_prob,
            "cuisine": item['cuisine'],
            "last_inspection_date": "2024-04-10",
            "top_reasons": ["Historical compliance pattern", "Area density index"],
            "top_violation_probs": vios,
            "predicted_points": round(base_prob * 100, 1),
            "data_version": "real-osm-v1.0"
        })
    return processed

if __name__ == "__main__":
    raw = get_ahmedabad_restaurants()
    if raw:
        final = transform_to_app_format(raw)
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(final, f, indent=2, ensure_ascii=False)
        print(f"SUCCESS: {len(final)} real restaurants saved to {OUTPUT_FILE}")
    else:
        print("Failed to get data.")
