# api/services/model_service.py
"""
ModelService for Health Inspection Coach — Ahmedabad.

Loads demo seed data and scores restaurants by combining base risk
with live environmental risk from the Google Places service.
"""
import json
import os
from typing import Any, Dict

from api.services.google_places_service import get_environmental_risk


class ModelService:
    """
    Loads Ahmedabad demo seed (FSSAI restaurants) and scores them
    by blending base risk with real-time environmental index.
    """

    def __init__(self, demo_seed: str):
        self.demo_seed_path = demo_seed
        self._demo = self._load_demo_seed()

    def _load_demo_seed(self) -> Dict[str, Any]:
        """Load demo_seed.json and ensure it is keyed by fssai_id."""
        try:
            if self.demo_seed_path and os.path.exists(self.demo_seed_path):
                with open(self.demo_seed_path, "r") as f:
                    data = json.load(f)
                    # If it's a list, convert to dict keyed by fssai_id
                    if isinstance(data, list):
                        return {str(item["fssai_id"]): item for item in data if "fssai_id" in item}
                    return data
        except Exception as e:
            print(f"[model_service] WARNING: failed to load demo seed: {e}", flush=True)
        return {}

    def list_restaurants(self) -> list:
        """Return a summary list of all seeded restaurants."""
        return [
            {
                "fssai_id": v["fssai_id"],
                "name": v["name"],
                "area": v.get("area", ""),
            }
            for v in self._demo.values()
        ]

    async def score_restaurant(self, fssai_id: str) -> Dict[str, Any]:
        """
        Score a restaurant by FSSAI ID.

        1. Look up seed data
        2. Call Google Places service for environmental risk
        3. Apply environmental penalty to base probability
        4. Return complete payload matching ScoreResponse
        """
        fssai_id = str(fssai_id)
        if fssai_id not in self._demo:
            raise KeyError(f"FSSAI ID '{fssai_id}' not found in seed data")

        entry = dict(self._demo[fssai_id])  # copy
        lat = entry.get("latitude", 0.0)
        lon = entry.get("longitude", 0.0)

        # --- Live environmental risk ---
        env_result = await get_environmental_risk(lat, lon)
        env_index = env_result.get("environmental_index", 0.0)
        risk_factors = env_result.get("nearby_risk_factors", [])

        # --- Scoring ---
        base_prob = entry.get("prob_fssai_fail", 0.2)

        # Apply environmental penalty: up to +15% bump
        prob_fssai_fail = round(min(0.99, base_prob + 0.15 * env_index), 4)

        # Adjust violation probabilities based on environmental index
        vio_probs = entry.get("top_violation_probs", [])
        adjusted_vios = []
        for v in vio_probs:
            adj_prob = round(min(0.99, v["probability"] + 0.10 * env_index), 4)
            adjusted_vios.append({
                "code": v["code"],
                "probability": adj_prob,
                "label": v["label"],
            })

        # Build reasons
        reasons = list(entry.get("top_reasons", []))
        if env_index >= 0.5:
            reasons.append(f"High environmental risk (index: {env_index:.2f}) — nearby markets/transit")
        elif env_index >= 0.2:
            reasons.append(f"Moderate environmental risk (index: {env_index:.2f})")

        return {
            "fssai_id": fssai_id,
            "name": entry.get("name", ""),
            "area": entry.get("area", ""),
            "prob_fssai_fail": prob_fssai_fail,
            "predicted_points": entry.get("predicted_points"),
            "top_reasons": reasons,
            "top_violation_probs": adjusted_vios,
            "environmental_index": env_index,
            "nearby_risk_factors": risk_factors,
            "model_version": entry.get("model_version", "ahmedabad-v1.0"),
            "data_version": entry.get("data_version", "demo-seed"),
            "last_inspection_date": entry.get("last_inspection_date"),
            "latitude": lat,
            "longitude": lon,
        }
