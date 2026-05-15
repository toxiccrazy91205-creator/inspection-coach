#!/usr/bin/env python3
"""
Generate Ahmedabad restaurant seed data for Health Inspection Coach.

Produces data/demo_seed.json with 15 mock restaurants across Ahmedabad,
each with realistic coordinates, FSSAI license IDs, and base risk scores.
"""
import json
import os
import random
from datetime import date, timedelta

SEED_FILE = os.getenv("DEMO_SEED_FILE", "./data/demo_seed.json")

# 15 Ahmedabad restaurants with realistic lat/lon
RESTAURANTS = [
    {"fssai_id": "10020064000001", "name": "Manek Chowk Khau Gali",   "area": "Manek Chowk",   "cuisine": "Street Food",     "lat": 23.0258, "lon": 72.5873, "base_risk": 18},
    {"fssai_id": "10020064000002", "name": "Lucky Restaurant",         "area": "Lal Darwaja",   "cuisine": "Mughlai",         "lat": 23.0236, "lon": 72.5822, "base_risk": 14},
    {"fssai_id": "10020064000003", "name": "ZK Restaurant",            "area": "SG Highway",    "cuisine": "Multi-Cuisine",   "lat": 23.0355, "lon": 72.5110, "base_risk": 8},
    {"fssai_id": "10020064000004", "name": "Honest Restaurant",        "area": "Navrangpura",   "cuisine": "Gujarati",        "lat": 23.0360, "lon": 72.5612, "base_risk": 6},
    {"fssai_id": "10020064000005", "name": "Keshav Dining",            "area": "Vastrapur",     "cuisine": "Gujarati Thali",  "lat": 23.0350, "lon": 72.5265, "base_risk": 10},
    {"fssai_id": "10020064000006", "name": "Gordhan Thal",             "area": "Satellite",     "cuisine": "Gujarati Thali",  "lat": 23.0220, "lon": 72.5110, "base_risk": 5},
    {"fssai_id": "10020064000007", "name": "Gopi Dining Hall",         "area": "CG Road",       "cuisine": "Gujarati",        "lat": 23.0305, "lon": 72.5600, "base_risk": 12},
    {"fssai_id": "10020064000008", "name": "Rajwadu",                  "area": "SG Highway",    "cuisine": "Traditional",     "lat": 23.0600, "lon": 72.5150, "base_risk": 7},
    {"fssai_id": "10020064000009", "name": "Patang Hotel",             "area": "Kankaria",      "cuisine": "Revolving Dine",  "lat": 23.0070, "lon": 72.6000, "base_risk": 9},
    {"fssai_id": "10020064000010", "name": "Agashiye",                 "area": "Bhadra",        "cuisine": "Heritage Gujarati","lat": 23.0250, "lon": 72.5815, "base_risk": 4},
    {"fssai_id": "10020064000011", "name": "Vishalla",                 "area": "Vasna",         "cuisine": "Village Gujarati","lat": 22.9950, "lon": 72.5550, "base_risk": 11},
    {"fssai_id": "10020064000012", "name": "Tomatoes Restaurant",      "area": "Prahlad Nagar", "cuisine": "Italian",         "lat": 23.0140, "lon": 72.5080, "base_risk": 6},
    {"fssai_id": "10020064000013", "name": "The Green House",          "area": "Bodakdev",      "cuisine": "Continental",     "lat": 23.0400, "lon": 72.5030, "base_risk": 5},
    {"fssai_id": "10020064000014", "name": "Sankalp Restaurant",       "area": "Paldi",         "cuisine": "South Indian",    "lat": 23.0160, "lon": 72.5610, "base_risk": 8},
    {"fssai_id": "10020064000015", "name": "Barbeque Nation",          "area": "Drive-In",      "cuisine": "BBQ Buffet",      "lat": 23.0430, "lon": 72.5460, "base_risk": 7},
]

FSSAI_VIOLATIONS = [
    {"code": "Sch4.2.1", "label": "Temperature control of food — hot/cold holding requirements"},
    {"code": "Sch4.3.1", "label": "Cleanliness of premises and equipment"},
    {"code": "Sch4.5.1", "label": "Pest control measures — evidence of pests on premises"},
    {"code": "Sch4.7.1", "label": "Personal hygiene of food handlers"},
]


def generate_seed():
    random.seed(42)  # Deterministic output
    today = date.today()
    seed = {}

    for r in RESTAURANTS:
        fssai_id = r["fssai_id"]
        base_risk = r["base_risk"]
        prob_fail = round(min(0.95, base_risk / 28.0), 3)

        # Generate plausible violation probabilities scaled by base risk
        vio_probs = []
        for i, v in enumerate(FSSAI_VIOLATIONS):
            p = round(min(0.95, 0.3 + prob_fail * 0.5 - i * 0.08), 3)
            vio_probs.append({
                "code": v["code"],
                "probability": max(0.05, p),
                "label": v["label"],
            })

        # Random last inspection date within last 6 months
        days_back = random.randint(30, 180)
        last_date = (today - timedelta(days=days_back)).isoformat()

        # Build reasons
        reasons = [f"Base risk score: {base_risk}/28"]
        if base_risk >= 14:
            reasons.append("Recent hygiene concerns noted")
        elif base_risk >= 8:
            reasons.append("Moderate risk from past observations")
        else:
            reasons.append("Generally compliant history")

        seed[fssai_id] = {
            "fssai_id": fssai_id,
            "name": r["name"],
            "area": r["area"],
            "cuisine": r["cuisine"],
            "latitude": r["lat"],
            "longitude": r["lon"],
            "base_risk_score": base_risk,
            "prob_fssai_fail": prob_fail,
            "predicted_points": float(base_risk),
            "top_reasons": reasons,
            "top_violation_probs": vio_probs,
            "last_inspection_date": last_date,
            "model_version": "ahmedabad-v1.0",
            "data_version": "demo-seed",
        }

    os.makedirs(os.path.dirname(SEED_FILE) or ".", exist_ok=True)
    with open(SEED_FILE, "w") as f:
        json.dump(seed, f, indent=2)
    print(f"[OK] Wrote {len(seed)} Ahmedabad restaurants to {SEED_FILE}")


if __name__ == "__main__":
    generate_seed()
