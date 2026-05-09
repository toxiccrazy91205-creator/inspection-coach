# api/routers/admin.py
import os
import threading
from fastapi import APIRouter, Header, HTTPException

from api.routers.score import model_service

router = APIRouter()

ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")
FEATURE_STORE_DIR = os.getenv("FEATURE_STORE_DIR", "./data/parquet")

_refresh_status: dict = {"running": False, "last": None}


def _run_refresh():
    steps: list[str] = []
    try:
        from etl.nyc_inspections_etl import fetch, write_parquet
        df = fetch()
        if df.empty:
            steps.append("inspections_empty_response")
        else:
            write_parquet(df, "inspections_raw")
            steps.append(f"inspections_ok:{len(df)}_rows")
            from api.routers.score import _latest_visit_summary
            _latest_visit_summary.cache_clear()
    except Exception as e:
        steps.append(f"inspections_failed:{e}")

    try:
        os.environ["FEATURE_STORE_DIR"] = FEATURE_STORE_DIR
        from etl.rodent_index import build_rat_features
        build_rat_features()
        steps.append("rat_index_ok")
    except Exception as e:
        steps.append(f"rat_index_failed:{e}")

    try:
        count = model_service.reload_rat_features()
        steps.append(f"rat_features_reloaded:{count}")
    except Exception as e:
        steps.append(f"rat_reload_failed:{e}")

    _refresh_status["running"] = False
    _refresh_status["last"] = steps


@router.post("/admin/refresh")
def refresh(x_admin_token: str = Header(default="")):
    if not ADMIN_TOKEN or x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if _refresh_status["running"]:
        return {"ok": False, "message": "Refresh already in progress"}

    _refresh_status["running"] = True
    threading.Thread(target=_run_refresh, daemon=True).start()
    return {"ok": True, "message": "Refresh started in background — call /admin/refresh/status to check progress"}


@router.get("/admin/refresh/status")
def refresh_status(x_admin_token: str = Header(default="")):
    if not ADMIN_TOKEN or x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return {
        "running": _refresh_status["running"],
        "last_result": _refresh_status["last"],
    }


@router.get("/admin/ratpeek")
def ratpeek(camis: str):
    v = model_service.rat_features.get(str(camis))
    return {
        "feature_dir": os.getenv("FEATURE_STORE_DIR", "./data/parquet"),
        "has": str(camis) in model_service.rat_features,
        "value": v,
    }
