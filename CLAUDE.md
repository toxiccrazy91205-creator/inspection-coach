# Health Inspection Coach — Ahmedabad

## What it does

Given a restaurant's FSSAI license ID (India's Food Safety and Standards Authority identifier), the API returns:
- Probability of failing the next FSSAI inspection (`prob_fssai_fail`)
- Predicted violation point total
- Top likely FSSAI Schedule 4 violation categories with probabilities
- An "environmental index" — real-time geospatial measure of nearby risk factors (markets, transit hubs, construction) via Google Places API

---

## Tech stack

- **FastAPI** backend
- **httpx** for async HTTP calls to Google Places Nearby Search API (v1)
- **Demo seed data** — 15 pre-seeded Ahmedabad restaurants in `data/demo_seed.json`
- Python 3.11, Uvicorn, Pydantic v2

---

## Data pipeline

1. `etl/generate_ahmedabad_seed.py` — generates `data/demo_seed.json` with 15 Ahmedabad restaurants (FSSAI IDs, coordinates, base risk scores)
2. `api/services/google_places_service.py` — real-time 200m radius search via Google Places for environmental risk factors
3. `api/services/model_service.py` — combines seed data + environmental index into a risk score

---

## API structure

```
api/
├── main.py                              # FastAPI app, CORS, routers
├── models.py                            # Pydantic schemas
├── routers/
│   └── score.py                         # POST /score
└── services/
    ├── model_service.py                 # ModelService: loads seed, async scoring
    └── google_places_service.py         # Google Places Nearby Search proxy
```

### Key endpoints

| Endpoint | Purpose |
|---|---|
| `GET /health` | Health check |
| `GET /metadata` | App version and city info |
| `POST /score` | Get FSSAI risk prediction for a restaurant |

---

## Scoring approach

Heuristic-based (not ML-trained):
- Derives `prob_fssai_fail` from a restaurant's base risk score (0–28 scale)
- Blends in environmental penalty (up to +15%) based on nearby risk factors
- Adjusts FSSAI Schedule 4 violation probabilities by environmental index
- Uses mock environmental data when `MAPS_API_KEY` is not set

---

## Deployment

- Docker image includes only API code and seed data (no Parquet files)
- Set `MAPS_API_KEY` environment variable for live Google Places integration
- Falls back to deterministic mock data without API key

### Key environment variables

| Var | Purpose |
|---|---|
| `MAPS_API_KEY` | Google Places API key for live environmental risk assessment |
| `DEMO_SEED_FILE` | Path to demo seed JSON (default `./data/demo_seed.json`) |
