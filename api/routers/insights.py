import os
from datetime import date
from functools import lru_cache
import pandas as pd
from fastapi import APIRouter, HTTPException

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


@lru_cache(maxsize=1)
def _compute_insights():
    df = pd.read_parquet(_parquet_path(), columns=COLS)
    df["inspection_date"] = pd.to_datetime(df["inspection_date"], errors="coerce")
    df["score"] = pd.to_numeric(df["score"], errors="coerce")

    # One row per restaurant: most recent inspection
    latest = (
        df.sort_values("inspection_date")
        .groupby("camis", as_index=False)
        .last()
    )

    # Infer grade from score when not a valid letter grade
    def _grade(row):
        g = str(row.get("grade") or "").strip().upper()
        if g in ("A", "B", "C"):
            return g
        s = row.get("score")
        if s is None or pd.isna(s):
            return None
        if s <= 13: return "A"
        if s <= 27: return "B"
        return "C"

    latest["grade_display"] = latest.apply(_grade, axis=1)

    total = len(latest)
    counts = latest["grade_display"].value_counts().to_dict()
    grade_counts = {g: int(counts.get(g, 0)) for g in ("A", "B", "C")}
    grade_counts["ungraded"] = int(total - sum(grade_counts.values()))

    # Top 5 riskiest: highest score, must have a score
    top = (
        latest[latest["score"].notna()]
        .sort_values("score", ascending=False)
        .head(5)
    )

    today = date.today()
    top_risky = []
    for r in top.itertuples(index=False):
        last_date = str(r.inspection_date)[:10] if pd.notna(r.inspection_date) else None
        days_since = None
        if last_date:
            try:
                days_since = (today - date.fromisoformat(last_date)).days
            except Exception:
                pass
        top_risky.append({
            "camis": str(r.camis),
            "name": str(r.dba),
            "address": " ".join(filter(None, [
                str(r.building or "").strip(),
                str(r.street or "").strip(),
            ])),
            "boro": str(r.boro or ""),
            "cuisine": str(r.cuisine_description or ""),
            "last_score": int(r.score),
            "last_grade": r.grade_display,
            "last_date": last_date,
            "days_since": days_since,
        })

    return {
        "total_restaurants": total,
        "grade_counts": grade_counts,
        "top_risky": top_risky,
    }


@router.get("/insights", summary="NYC-wide restaurant inspection statistics")
def insights():
    """
    Returns precomputed citywide statistics derived from the latest inspection per restaurant:
    - Total number of restaurants tracked
    - Grade distribution (count of A / B / C / ungraded across all ~31k restaurants)
    - Top 5 highest-scoring (riskiest) restaurants in NYC by most recent inspection score

    Results are cached in memory after the first call and reset on data refresh.
    """
    return _compute_insights()
