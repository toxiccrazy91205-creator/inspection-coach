import os
from functools import lru_cache
import pandas as pd
from fastapi import APIRouter, HTTPException

router = APIRouter()

RUNTIME_PARQUET_DIR = os.getenv("FEATURE_STORE_DIR", "/tmp/data/parquet")
BAKED_PARQUET_DIR = os.getenv("BAKED_FEATURE_DIR", "./data/parquet")
RAW_FILE_RUNTIME = os.path.join(RUNTIME_PARQUET_DIR, "inspections_raw.parquet")
RAW_FILE_BAKED = os.path.join(BAKED_PARQUET_DIR, "inspections_raw.parquet")

COLS = ["camis", "boro", "inspection_date", "score", "grade",
        "violation_code", "violation_description", "critical_flag"]


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

    # One row per restaurant: most recent inspection date
    latest_dates = (
        df.dropna(subset=["inspection_date"])
        .groupby("camis")["inspection_date"]
        .max()
        .rename("latest_date")
    )
    df = df.join(latest_dates, on="camis")
    latest_rows = df[df["inspection_date"] == df["latest_date"]].copy()

    # One row per restaurant (latest inspection)
    grade_per = (
        latest_rows.sort_values("inspection_date")
        .groupby("camis", as_index=False)
        .last()[["camis", "boro", "score", "grade"]]
    )

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

    grade_per["grade_display"] = grade_per.apply(_grade, axis=1)
    total = len(grade_per)
    counts = grade_per["grade_display"].value_counts().to_dict()
    grade_counts = {g: int(counts.get(g, 0)) for g in ("A", "B", "C")}
    grade_counts["ungraded"] = int(total - sum(grade_counts.values()))

    # Borough breakdown
    BOROS = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]
    borough_stats = []
    for boro in BOROS:
        b = grade_per[grade_per["boro"].str.upper() == boro.upper()]
        if len(b) == 0:
            continue
        bcounts = b["grade_display"].value_counts().to_dict()
        borough_stats.append({
            "boro": boro,
            "total": int(len(b)),
            "avg_score": round(float(b["score"].dropna().mean()), 1) if b["score"].notna().any() else None,
            "grade_counts": {g: int(bcounts.get(g, 0)) for g in ("A", "B", "C")},
        })

    # Top violations — count unique restaurants affected by each violation code
    # in their most recent inspection
    viols = latest_rows[
        latest_rows["violation_code"].notna() &
        latest_rows["violation_description"].notna()
    ][["camis", "violation_code", "violation_description", "critical_flag"]].drop_duplicates(
        subset=["camis", "violation_code"]
    )

    # Map each code to its description and critical flag (use most common)
    code_meta = (
        viols.groupby("violation_code")
        .agg(
            description=("violation_description", lambda x: x.mode().iloc[0] if len(x) else ""),
            critical=("critical_flag", lambda x: (x.str.upper() == "CRITICAL").any()),
            restaurant_count=("camis", "nunique"),
        )
        .reset_index()
        .sort_values("restaurant_count", ascending=False)
        .head(3)
    )

    top_violations = [
        {
            "code": str(r.violation_code),
            "description": str(r.description),
            "critical": bool(r.critical),
            "restaurant_count": int(r.restaurant_count),
        }
        for r in code_meta.itertuples(index=False)
    ]

    return {
        "total_restaurants": total,
        "grade_counts": grade_counts,
        "top_violations": top_violations,
        "borough_stats": borough_stats,
    }


@router.get("/insights", summary="NYC-wide restaurant inspection statistics")
def insights():
    """
    Returns precomputed citywide statistics derived from the latest inspection per restaurant:
    - Total number of restaurants tracked
    - Grade distribution (count of A / B / C / ungraded across all ~31k restaurants)
    - Top 3 most common violations by number of restaurants affected (latest inspection only)

    Results are cached in memory after the first call and reset on data refresh.
    """
    return _compute_insights()
