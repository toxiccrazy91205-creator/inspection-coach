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


@router.get("/neighborhood", summary="List restaurants in a zip code by inspection risk")
def neighborhood(
    zip: str = Query(..., min_length=5, max_length=5, description="5-digit NYC zip code"),
    limit: int = Query(default=30, description="Maximum number of restaurants to return (default 30)"),
):
    """
    Returns restaurants in the given NYC zip code, ranked by most recent inspection score
    (highest / most risky first). Each result includes the restaurant's last grade, score,
    inspection date, and days since last inspection.

    Grades are inferred from the point score when not explicitly recorded (0–13 = A, 14–27 = B, 28+ = C).
    Only the most recent inspection per restaurant is returned.
    """
    p = _parquet_path()
    df = pd.read_parquet(p, columns=COLS)
    df = df[df["zipcode"].astype(str).str.strip() == zip.strip()].copy()
    if df.empty:
        return []

    df["inspection_date"] = pd.to_datetime(df["inspection_date"], errors="coerce")
    df["score"] = pd.to_numeric(df["score"], errors="coerce")

    # Latest inspection date per restaurant
    latest_dates = df.dropna(subset=["inspection_date"]).groupby("camis")["inspection_date"].max()
    df = df.join(latest_dates.rename("latest_date"), on="camis")
    latest_rows = df[df["inspection_date"] == df["latest_date"]].copy()

    # Aggregate across all rows of the latest inspection so we don't depend on row order:
    # - score: take first non-null (should be identical across rows)
    # - grade: take first valid A/B/C found; only infer from score if none present
    # - dba/boro/cuisine/building/street: take first non-null
    def _first_valid_grade(s: pd.Series) -> str | None:
        for v in s.dropna():
            g = str(v).strip().upper()
            if g in ("A", "B", "C"):
                return g
        return None

    def _infer_grade(grade, score):
        if grade in ("A", "B", "C"):
            return grade
        if score is None or pd.isna(score):
            return None
        if score <= 13: return "A"
        if score <= 27: return "B"
        return "C"

    agg = latest_rows.groupby("camis").agg(
        dba=("dba", "first"),
        boro=("boro", "first"),
        building=("building", "first"),
        street=("street", "first"),
        cuisine_description=("cuisine_description", "first"),
        inspection_date=("inspection_date", "first"),
        score=("score", "first"),
        grade=("grade", _first_valid_grade),
    ).reset_index()

    agg["grade_display"] = agg.apply(lambda r: _infer_grade(r["grade"], r["score"]), axis=1)

    # Sort: most risky first (highest score), unscored at end
    agg = agg.sort_values("score", ascending=False, na_position="last").head(limit)

    today = date.today()
    results = []
    for r in agg.itertuples(index=False):
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
