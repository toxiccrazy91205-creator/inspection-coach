# api/routers/score.py
"""POST /score — risk prediction for an Ahmedabad restaurant by FSSAI ID."""
import os

from fastapi import APIRouter, HTTPException

from api.models import ScoreRequest, ScoreResponse
from api.services.model_service import ModelService

router = APIRouter()

DEMO_SEED_FILE = os.getenv("DEMO_SEED_FILE", "./data/demo_seed.json")
model_service = ModelService(demo_seed=DEMO_SEED_FILE)


@router.get("/restaurants", summary="List all seeded restaurants")
async def list_restaurants():
    """Return a summary list of all seeded Ahmedabad restaurants."""
    return model_service.list_restaurants()


@router.post("/score", response_model=ScoreResponse, summary="Get FSSAI risk score for a restaurant")
async def score(req: ScoreRequest):
    """
    Returns a risk prediction for the given Ahmedabad restaurant (identified by FSSAI license ID).

    **Scoring approach:** heuristic-based. The `prob_fssai_fail` field is the estimated
    probability of failing the next FSSAI inspection, derived from:
    - Base risk score from historical observations
    - Real-time environmental index (nearby markets, transit hubs, construction within 200m)
    - FSSAI Schedule 4 violation category probabilities

    For the 15 pre-seeded demo restaurants the response is returned from seed data
    augmented with a live Google Places environmental scan. If no API key is configured,
    a deterministic mock environmental response is used.
    """
    try:
        payload = await model_service.score_restaurant(req.fssai_id)
        return ScoreResponse(**payload)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f"FSSAI ID '{req.fssai_id}' not found. Use one of the 15 seeded Ahmedabad restaurants.",
        )
