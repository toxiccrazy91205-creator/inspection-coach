# DineSafe NYC — Health Inspection Compliance Coach

## What it does

Given a restaurant's CAMIS ID (NYC's unique restaurant identifier), the API returns:
- Probability of getting a B or C grade on the next inspection (`prob_bc`)
- Predicted violation score (points)
- Top likely violation categories with probabilities
- A "rat pressure index" — geospatial measure of nearby rodent complaints and failed city rat inspections

Live API: https://health-inspection-compliance-coach-production.up.railway.app  
Swagger docs: https://health-inspection-compliance-coach-production.up.railway.app/docs

---

## Tech stack

- **FastAPI** backend, deployed on **Google Cloud Run** (serverless, scale-to-zero)
- Data stored as **Parquet files** (Pandas + PyArrow), baked into the Docker image as a cold-start fallback
- **H3 hexagonal grid** for geospatial rat pressure aggregation
- **Nightly ETL** via Cloud Scheduler pulling fresh data from NYC Open Data (Socrata API)
- Python 3.11, Uvicorn, Pydantic v2

---

## Data pipeline

1. `etl/nyc_inspections_etl.py` — fetches NYC restaurant inspection records → `data/parquet/inspections_raw.parquet`
2. `etl/rodent_index.py` — fetches 311 rodent complaints + DOHMH rat inspections → builds `data/parquet/rat_index.parquet` using H3 hex cells (res 9 ≈ 150–200m)
3. `etl/feature_engineering.py` — pre-seeds 25 example restaurants → `data/demo_seed.json` for fast demo responses

---

## API structure

```
api/
├── main.py                  # FastAPI app, CORS, routers
├── models.py                # Pydantic schemas
├── routers/
│   ├── score.py             # POST /score
│   ├── search.py            # GET /search
│   └── admin.py             # POST /admin/refresh, GET /admin/ratpeek, /admin/rawpeek
└── services/
    └── model_service.py     # ModelService: loads parquet, demo seed, LRU cache
```

### Key endpoints

| Endpoint | Purpose |
|---|---|
| `GET /health` | Health check |
| `GET /metadata` | App version and data window info |
| `GET /search?name=` | Find restaurants by name (min 2 chars, up to 25 results) |
| `POST /score` | Get risk prediction for a CAMIS |
| `POST /admin/refresh` | Trigger full data refresh (requires `X-Admin-Token`) |
| `GET /admin/ratpeek?camis=` | Debug rat features for a CAMIS |
| `GET /admin/rawpeek?camis=` | Debug raw parquet for a CAMIS |

---

## Scoring approach

Currently **heuristic-based, not ML-trained**:
- Derives `prob_bc` from a restaurant's last inspection score
- Blends in rat pressure bump (up to ~12%) if local rodent activity is high
- Boosts mice violation probability (code `04L`) when rat index is elevated
- Falls back to `demo_seed.json` for the 25 pre-seeded restaurants; falls back to heuristic from parquet for all others
- Structured to swap in a real ML model later (`models/dummy.joblib` placeholder)

---

## Deployment

- Docker image bakes parquet files for cold-start resilience
- Runtime parquet written to `/tmp` (Cloud Run writable); image parquet at `/app/data/parquet` as fallback
- Cloud Scheduler runs nightly at 03:00 UTC → calls `POST /admin/refresh`

### Key environment variables

| Var | Purpose |
|---|---|
| `FEATURE_STORE_DIR` | Runtime parquet directory (default `/tmp/data/parquet`) |
| `BAKED_FEATURE_DIR` | Image-baked parquet fallback (default `/app/data/parquet`) |
| `ADMIN_TOKEN` | Required for /admin endpoints |
| `NYC_APP_TOKEN` | Socrata API token (higher rate limits) |
| `RATS_DAYS_311` | Time window for 311 complaints (default 180 days) |
| `RATS_DAYS_INSP` | Time window for rat inspections (default 365 days) |
