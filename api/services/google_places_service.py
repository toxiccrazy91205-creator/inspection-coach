# api/services/google_places_service.py
"""
Live geospatial risk assessment via Google Places Nearby Search API (New, v1).

Searches within a 200-meter radius for environmental risk factors:
  - marketplaces (open-air food markets attract pests)
  - bus/transit stations (high foot traffic, waste accumulation)
  - general contractors (construction dust, debris, disruption)

Returns an environmental_index (0.0–1.0) and a list of nearby risk factors.
Falls back to a deterministic mock if MAPS_API_KEY is not set.
"""
import hashlib
import math
import os
from typing import Any, Dict, List

import httpx

MAPS_API_KEY = os.getenv("MAPS_API_KEY", "")

PLACES_URL = "https://places.googleapis.com/v1/places:searchNearby"

# Google Places Table A types that indicate environmental risk
RISK_TYPES = ["marketplace", "bus_station", "transit_station", "general_contractor"]

# Weight multiplier per type (some are riskier than others)
TYPE_WEIGHTS = {
    "marketplace": 1.5,         # open-air food → pest attraction
    "bus_station": 1.0,         # high foot traffic, waste
    "transit_station": 1.0,     # high foot traffic, waste
    "general_contractor": 0.8,  # construction debris / dust
}

SEARCH_RADIUS = 200.0  # meters
MAX_RESULTS = 20


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Approximate distance in metres between two lat/lon points."""
    R = 6_371_000  # Earth radius in metres
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _mock_response(lat: float, lon: float) -> Dict[str, Any]:
    """
    Deterministic mock based on lat/lon hash.
    Ensures the app never crashes when MAPS_API_KEY is missing.
    """
    h = int(hashlib.md5(f"{lat:.6f},{lon:.6f}".encode()).hexdigest(), 16)
    count = (h % 5)  # 0–4 mock places
    mock_factors: List[Dict[str, Any]] = []
    mock_names = [
        ("Local Vegetable Market", "marketplace"),
        ("AMTS Bus Stop", "bus_station"),
        ("Metro Station Entrance", "transit_station"),
        ("Construction Site", "general_contractor"),
        ("Weekly Bazaar", "marketplace"),
    ]
    for i in range(count):
        name, ptype = mock_names[i % len(mock_names)]
        dist = 50 + ((h >> (i * 8)) % 150)  # 50–199 m
        mock_factors.append({"name": name, "type": ptype, "distance_m": float(dist)})

    # Weighted score
    weighted = sum(TYPE_WEIGHTS.get(f["type"], 1.0) for f in mock_factors)
    env_index = round(min(1.0, weighted / 5.0), 4)

    return {
        "environmental_index": env_index,
        "nearby_risk_factors": mock_factors,
        "_source": "mock",
    }


async def get_environmental_risk(lat: float, lon: float) -> Dict[str, Any]:
    """
    Query Google Places Nearby Search for environmental risk factors
    within 200m of the given coordinates.

    Returns:
        {
            "environmental_index": float (0.0–1.0),
            "nearby_risk_factors": [
                {"name": str, "type": str, "distance_m": float},
                ...
            ]
        }
    """
    if not MAPS_API_KEY:
        return _mock_response(lat, lon)

    headers = {
        "X-Goog-Api-Key": MAPS_API_KEY,
        "X-Goog-FieldMask": "places.displayName,places.types,places.location",
        "Content-Type": "application/json",
    }

    body = {
        "includedTypes": RISK_TYPES,
        "locationRestriction": {
            "circle": {
                "center": {"latitude": lat, "longitude": lon},
                "radius": SEARCH_RADIUS,
            }
        },
        "maxResultCount": MAX_RESULTS,
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(PLACES_URL, json=body, headers=headers)
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        print(f"[google_places] API call failed ({e}), using mock fallback", flush=True)
        return _mock_response(lat, lon)

    places = data.get("places", [])
    risk_factors: List[Dict[str, Any]] = []

    for place in places:
        display_name = place.get("displayName", {}).get("text", "Unknown")
        types = place.get("types", [])
        loc = place.get("location", {})
        p_lat = loc.get("latitude", lat)
        p_lon = loc.get("longitude", lon)
        dist = round(_haversine_m(lat, lon, p_lat, p_lon), 1)

        # Identify the matching risk type
        matched_type = "unknown"
        for t in RISK_TYPES:
            if t in types:
                matched_type = t
                break

        risk_factors.append({
            "name": display_name,
            "type": matched_type,
            "distance_m": dist,
        })

    # Weighted environmental index
    weighted = sum(TYPE_WEIGHTS.get(f["type"], 1.0) for f in risk_factors)
    env_index = round(min(1.0, weighted / 5.0), 4)

    return {
        "environmental_index": env_index,
        "nearby_risk_factors": risk_factors,
        "_source": "google_places_api",
    }
