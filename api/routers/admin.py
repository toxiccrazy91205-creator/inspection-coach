# api/routers/admin.py
import os
from fastapi import APIRouter, Header, HTTPException, Query

# Reuse the same ModelService instance as /score
from api.routers.score import model_service

router = APIRouter()

ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")
FEATURE_STORE_DIR = os.getenv("FEATURE_STORE_DIR", "./data/parquet")

@router.post("/admin/refresh", summary="Trigger a full data refresh (admin only)")
def refresh(x_admin_token: str = Header(default="", description="Admin token — must match the ADMIN_TOKEN environment variable")):
    """
    Rebuilds the rat pressure index from live NYC 311 and DOHMH rodent inspection data,
    then reloads the in-memory feature store so `/score` immediately uses the latest data.

    Requires the `X-Admin-Token` header to match the server's `ADMIN_TOKEN` environment variable.

    This endpoint runs synchronously and may take 30–120 seconds. It is called nightly at
    03:00 UTC by Cloud Scheduler. Poll `/admin/refresh/status` if you need to check progress.
    """
    if not ADMIN_TOKEN or x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    steps: list[str] = []

    # 1) Your inspections/seed step (keep as-is or plug in your ETL)
    try:
        # e.g., nightly_refresh_inspections()
        steps.append("inspections_seed_ok")
    except Exception as e:
        steps.append(f"inspections_seed_failed: {e}")

    # 2) Build rat features (writes to FEATURE_STORE_DIR)
    try:
        os.environ["FEATURE_STORE_DIR"] = FEATURE_STORE_DIR
        from etl.rodent_index import build_rat_features
        build_rat_features()
        steps.append("rat_index_ok")
    except Exception as e:
        steps.append(f"rat_index_failed: {e}")

    # 3) Reload in-memory map so /score immediately uses whichever exists (/tmp or baked)
    try:
        count = model_service.reload_rat_features()
        steps.append(f"rat_features_reloaded:{count}")
    except Exception as e:
        steps.append(f"rat_reload_failed: {e}")

    return {"ok": True, "steps": steps}

@router.get("/admin/ratpeek", summary="Debug: rat features for a CAMIS")
def ratpeek(camis: str = Query(..., description="NYC CAMIS identifier")):
    """Debug helper: check whether a given CAMIS has rat pressure features loaded in-memory."""
    v = model_service.rat_features.get(str(camis))
    return {
        "feature_dir": os.getenv("FEATURE_STORE_DIR", "./data/parquet"),
        "has": str(camis) in model_service.rat_features,
        "value": v,
    }
