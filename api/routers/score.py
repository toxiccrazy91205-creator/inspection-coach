import os
import pandas as pd
from datetime import date
from functools import lru_cache
from fastapi import APIRouter, HTTPException
from typing import List

from api.models import ScoreRequest, ScoreResponse, ViolationProb
from api.services.model_service import ModelService


router = APIRouter()
svc = ModelService(model_path="", demo_seed="./data/demo_seed.json")

router = APIRouter()

DEMO_SEED_FILE = os.getenv("DEMO_SEED_FILE", "./data/demo_seed.json")
MODEL_PATH = os.getenv("MODEL_PATH", "./models/dummy.joblib")
model_service = ModelService(model_path=MODEL_PATH, demo_seed=DEMO_SEED_FILE)

RUNTIME_PARQUET_DIR = os.getenv("FEATURE_STORE_DIR", "/tmp/data/parquet")
BAKED_PARQUET_DIR = os.getenv("BAKED_FEATURE_DIR", "./data/parquet")
RAW_FILE_RUNTIME = os.path.join(RUNTIME_PARQUET_DIR, "inspections_raw.parquet")
RAW_FILE_BAKED = os.path.join(BAKED_PARQUET_DIR, "inspections_raw.parquet")

CODE_LABELS = {
    "04M": "Food not held at proper temp",
    "04L": "Evidence of mice",
    "10F": "Personal cleanliness",
}

def _parquet_path() -> str:
    p = RAW_FILE_RUNTIME if os.path.exists(RAW_FILE_RUNTIME) else RAW_FILE_BAKED
    if not os.path.exists(p):
        raise HTTPException(status_code=500, detail="No data parquet found. Run /admin/refresh or rebuild with data.")
    return p


@lru_cache(maxsize=1024)
def _latest_visit_summary(camis: str):
    def _read_filtered(path: str, camis: str) -> pd.DataFrame:
        try:
            return pd.read_parquet(path, filters=[("camis", "=", camis)])
        except Exception:
            df = pd.read_parquet(path)
            return df[df["camis"].astype(str) == str(camis)]

    def _extract_latlon(row: pd.Series) -> tuple:
        lat = lon = None
        for c in ["latitude", "Latitude", "lat", "LATITUDE"]:
            if c in row.index and pd.notna(row[c]):
                try: lat = float(row[c]); break
                except Exception: pass
        for c in ["longitude", "Longitude", "lon", "LONGITUDE"]:
            if c in row.index and pd.notna(row[c]):
                try: lon = float(row[c]); break
                except Exception: pass
        return lat, lon

    p = _parquet_path()
    df = _read_filtered(p, camis)
    if df.empty:
        return None

    if "inspection_date" in df.columns:
        df = df.sort_values("inspection_date")

    last = df.tail(1).iloc[0]

    last_date = (
        str(last["inspection_date"])[:10]
        if "inspection_date" in df.columns and pd.notna(last["inspection_date"])
        else None
    )

    try:
        last_score = int(last["score"]) if "score" in df.columns and pd.notna(last["score"]) else None
    except Exception:
        last_score = None

    last_grade = (
        str(last["grade"]).strip().upper()
        if "grade" in df.columns and pd.notna(last["grade"])
        else None
    )

    lat, lon = _extract_latlon(last)

    # fill coords from alternate parquet if missing
    alt = RAW_FILE_BAKED if p == RAW_FILE_RUNTIME else RAW_FILE_RUNTIME
    if (lat is None or lon is None) and os.path.exists(alt):
        df2 = _read_filtered(alt, camis)
        if not df2.empty:
            if "inspection_date" in df2.columns:
                df2 = df2.sort_values("inspection_date")
            last2 = df2.tail(1).iloc[0]
            lat2, lon2 = _extract_latlon(last2)
            lat = lat if lat is not None else lat2
            lon = lon if lon is not None else lon2

    same_visit = df[df["inspection_date"] == last["inspection_date"]] if "inspection_date" in df.columns else df.tail(1)

    labels_by_code: dict = {}
    if not same_visit.empty and "violation_code" in same_visit.columns and "violation_description" in same_visit.columns:
        tmp = same_visit.dropna(subset=["violation_code", "violation_description"]).copy()
        if not tmp.empty:
            labels_by_code = (
                tmp.groupby("violation_code")["violation_description"]
                .agg(lambda s: s.mode().iloc[0] if not s.mode().empty else s.iloc[0])
                .astype(str)
                .to_dict()
            )

    vio_counts: list = []
    if not same_visit.empty and "violation_code" in same_visit.columns:
        counts = same_visit["violation_code"].dropna().astype(str).value_counts().head(3)
        for code, cnt in counts.items():
            label = labels_by_code.get(code) or CODE_LABELS.get(code) or f"Violation {code}"
            vio_counts.append((code, int(cnt), label))

    # --- NEW: score history across distinct inspection dates ---
    score_history: list = []
    inspection_count = 1
    recurrence: dict = {}

    if "inspection_date" in df.columns and "score" in df.columns:
        # one row per inspection date (take the max score per date — most informative)
        by_date = (
            df.dropna(subset=["inspection_date"])
            .groupby("inspection_date")["score"]
            .apply(lambda s: _safe_int(s.dropna().iloc[0]) if not s.dropna().empty else None)
            .reset_index()
            .sort_values("inspection_date")
        )
        score_history = [
            (str(r["inspection_date"])[:10], r["score"])
            for _, r in by_date.iterrows()
            if r["score"] is not None
        ]
        inspection_count = len(by_date)

        # recurrence: count distinct inspection dates each violation code appeared in
        if "violation_code" in df.columns:
            recurrence = (
                df.dropna(subset=["violation_code", "inspection_date"])
                .groupby("violation_code")["inspection_date"]
                .nunique()
                .to_dict()
            )

    # days since last inspection
    days_since_last = None
    if last_date:
        try:
            days_since_last = (date.today() - date.fromisoformat(last_date)).days
        except Exception:
            pass

    return {
        "last_date": last_date,
        "last_score": last_score,
        "last_grade": last_grade,
        "vio_counts": vio_counts,
        "latitude": lat,
        "longitude": lon,
        "score_history": score_history,
        "inspection_count": inspection_count,
        "days_since_last": days_since_last,
        "recurrence": recurrence,
    }


def _safe_int(val):
    try:
        return int(val)
    except Exception:
        return None


def _heuristic_from_summary(s) -> tuple:
    last_score = s["last_score"]
    last_grade = s["last_grade"]
    score_history = s.get("score_history", [])
    days_since = s.get("days_since_last")
    inspection_count = s.get("inspection_count", 1)
    recurrence = s.get("recurrence", {})

    # --- base prob from last score ---
    if last_score is not None:
        if last_score >= 28:
            prob_bc = 0.85
        elif last_score >= 21:
            prob_bc = 0.70
        elif last_score >= 14:
            prob_bc = 0.50
        elif last_score >= 8:
            prob_bc = 0.30
        else:
            prob_bc = 0.12
        predicted_points = last_score
        reasons = [f"Last points: {last_score}"]
    elif last_grade in {"B", "C"}:
        prob_bc, predicted_points, reasons = 0.55, 18, [f"Last grade: {last_grade}"]
    else:
        prob_bc, predicted_points, reasons = 0.20, 10, ["Limited history"]

    if last_grade and f"Last grade: {last_grade}" not in reasons:
        reasons.append(f"Last grade: {last_grade}")

    # --- score trend: adjust based on last 3 inspections ---
    scores = [sc for _, sc in score_history if sc is not None]
    if len(scores) >= 2:
        recent, prior = scores[-1], scores[-2]
        delta = recent - prior  # positive = worsening, negative = improving
        if delta >= 10:
            prob_bc = min(0.95, prob_bc + 0.12)
            reasons.append(f"Score worsening ({prior}→{recent})")
        elif delta >= 5:
            prob_bc = min(0.95, prob_bc + 0.06)
            reasons.append(f"Score trending up ({prior}→{recent})")
        elif delta <= -10:
            prob_bc = max(0.05, prob_bc - 0.12)
            reasons.append(f"Score improving ({prior}→{recent})")
        elif delta <= -5:
            prob_bc = max(0.05, prob_bc - 0.06)
            reasons.append(f"Score trending down ({prior}→{recent})")

    # --- days since last inspection ---
    if days_since is not None:
        if days_since > 540:
            # Very overdue — pull toward the population mean (~35%)
            prob_bc = 0.4 * prob_bc + 0.6 * 0.35
            reasons.append(f"Not inspected in {days_since} days")
        elif days_since > 365:
            prob_bc = 0.7 * prob_bc + 0.3 * 0.35
            reasons.append(f"Not inspected in {days_since} days")

    # --- inspection count: flag low confidence ---
    if inspection_count == 1:
        reasons.append("Limited history (1 inspection on record)")
    elif inspection_count == 2:
        reasons.append("Early history (2 inspections on record)")

    # --- violation probs with recurrence boost ---
    top_vios: List[ViolationProb] = []
    total = sum(cnt for _, cnt, _ in s["vio_counts"]) or 1
    for code, cnt, label in s["vio_counts"]:
        base_prob = float(cnt) / float(total)
        recur_count = recurrence.get(code, 1)
        if recur_count >= 4:
            base_prob = min(0.99, base_prob * 1.6)
        elif recur_count >= 3:
            base_prob = min(0.99, base_prob * 1.4)
        elif recur_count >= 2:
            base_prob = min(0.99, base_prob * 1.2)
        top_vios.append(ViolationProb(code=code, probability=base_prob, label=label))

    return prob_bc, predicted_points, reasons, top_vios


@router.post("/score", response_model=ScoreResponse)
def score(req: ScoreRequest):
    camis = str(req.camis)

    def attach_rat(payload: dict) -> dict:
        rf = getattr(model_service, "rat_features", {}).get(camis)
        if rf:
            payload.update({
                "rat_index": rf.get("rat_index"),
                "rat311_cnt_180d_k1": rf.get("rat311_cnt_180d_k1"),
                "ratinsp_fail_365d_k1": rf.get("ratinsp_fail_365d_k1"),
            })
        return payload

    # Try seeded (demo) path
    try:
        payload = model_service.score_camis(camis)
        s = _latest_visit_summary(camis)
        if s:
            payload.update({
                "last_inspection_date": s["last_date"],
                "last_points": s["last_score"],
                "last_grade": s["last_grade"],
                "latitude": s.get("latitude"),
                "longitude": s.get("longitude"),
            })
        payload = attach_rat(payload)
        return ScoreResponse(**payload)

    except KeyError:
        # Heuristic fallback
        s = _latest_visit_summary(camis)
        if not s:
            raise HTTPException(status_code=404, detail="CAMIS not found")

        prob_bc, predicted_points, reasons, top_vios = _heuristic_from_summary(s)
        payload = {
            "camis": camis,
            "prob_bc": float(prob_bc),
            "predicted_points": float(predicted_points),
            "top_reasons": reasons,
            "top_violation_probs": top_vios,
            "model_version": "heuristic-v2",
            "data_version": "runtime",
            "last_inspection_date": s["last_date"],
            "last_points": s["last_score"],
            "last_grade": s["last_grade"],
            "latitude": s.get("latitude"),
            "longitude": s.get("longitude"),
        }
        payload = attach_rat(payload)
        return ScoreResponse(**payload)
