import os
from datetime import date
import pandas as pd
from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

RUNTIME_PARQUET_DIR = os.getenv("FEATURE_STORE_DIR", "/tmp/data/parquet")
BAKED_PARQUET_DIR = os.getenv("BAKED_FEATURE_DIR", "./data/parquet")
RAW_FILE_RUNTIME = os.path.join(RUNTIME_PARQUET_DIR, "inspections_raw.parquet")
RAW_FILE_BAKED = os.path.join(BAKED_PARQUET_DIR, "inspections_raw.parquet")

COLS = ["camis", "dba", "boro", "building", "street", "zipcode",
        "cuisine_description", "inspection_date", "score", "grade"]


def _parquet_path():
    p = RAW_FILE_RUNTIME if os.path.exists(RAW_FILE_RUNTIME) else RAW_FILE_BAKED
    if not os.path.exists(p):
        raise HTTPException(status_code=500, detail="No data parquet found.")
    return p


@router.get("/neighborhood")
def neighborhood(zip: str = Query(..., min_length=5, max_length=5), limit: int = 30):
    p = _parquet_path()
    df = pd.read_parquet(p, columns=COLS)
    df = df[df["zipcode"].astype(str).str.strip() == zip.strip()].copy()
    if df.empty:
        return []

    df["inspection_date"] = pd.to_datetime(df["inspection_date"], errors="coerce")
    df["score"] = pd.to_numeric(df["score"], errors="coerce")

    # One row per restaurant: most recent inspection
    latest = (
        df.sort_values("inspection_date")
        .groupby("camis", as_index=False)
        .last()
    )

    # Infer grade from score when missing
    def infer_grade(row):
        g = str(row.get("grade", "") or "").strip().upper()
        if g in ("A", "B", "C"):
            return g
        s = row.get("score")
        if s is None or pd.isna(s):
            return None
        if s <= 13: return "A"
        if s <= 27: return "B"
        return "C"

    latest["grade_display"] = latest.apply(infer_grade, axis=1)

    # Sort: most risky first (highest score), unscored at end
    latest = latest.sort_values("score", ascending=False, na_position="last").head(limit)

    today = date.today()
    results = []
    for r in latest.itertuples(index=False):
        last_date = str(r.inspection_date)[:10] if pd.notna(r.inspection_date) else None
        days_since = None
        if last_date:
            try:
                days_since = (today - date.fromisoformat(last_date)).days
            except Exception:
                pass
        score_val = int(r.score) if pd.notna(r.score) else None
        results.append({
            "camis": str(r.camis),
            "name": str(r.dba),
            "address": " ".join(filter(None, [str(r.building or "").strip(), str(r.street or "").strip(), zip])),
            "boro": str(r.boro or ""),
            "cuisine": str(r.cuisine_description or ""),
            "last_grade": r.grade_display,
            "last_score": score_val,
            "last_date": last_date,
            "days_since": days_since,
        })
    return results
